"""Batch ingestion service: atomicity, idempotency and reference resolution."""
from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass

from . import config, dicom_io
from .config import MAX_FILES_PER_BATCH, ensure_dirs
from .db import immediate_transaction
from .uid import remap_uid


class IngestionError(Exception):
    """Rejection of a whole batch; nothing is persisted or left to download.

    ``http_status`` distinguishes malformed input (422) from conflicts with
    already-stored state such as byte mismatches (409).
    """

    def __init__(self, message: str, http_status: int = 409) -> None:
        super().__init__(message)
        self.http_status = http_status


@dataclass
class UploadedFile:
    filename: str
    content: bytes


def _upsert_mapping(
    conn: sqlite3.Connection,
    center_id: str,
    original_uid: str,
    kind: str,
    batch_id: str,
) -> str:
    new_uid = remap_uid(center_id, original_uid)
    # The mapping value is deterministic, so an existing row is always equal.
    conn.execute(
        """
        INSERT INTO uid_mappings
            (center_id, original_uid, new_uid, uid_kind, first_seen_batch)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (center_id, original_uid) DO NOTHING
        """,
        (center_id, original_uid, new_uid, kind, batch_id),
    )
    return new_uid


def _validate_center_id(center_id: str) -> str:
    center_id = (center_id or "").strip()
    if not center_id:
        raise IngestionError("center_id is required", http_status=422)
    if len(center_id) > 64 or any(ch not in
                                  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                                  for ch in center_id):
        raise IngestionError(
            "center_id must be 1-64 chars of letters, digits, '_' or '-'",
            http_status=422,
        )
    return center_id


def ingest_batch(center_id: str, files: list[UploadedFile]) -> dict:
    """Ingest a batch atomically.

    Returns a summary dict on success; raises :class:`IngestionError` when the
    batch is rejected (in which case no row and no file is persisted).
    """

    center_id = _validate_center_id(center_id)
    if not files:
        raise IngestionError("at least one DICOM file is required", http_status=422)
    if len(files) > MAX_FILES_PER_BATCH:
        raise IngestionError(
            f"a batch may contain at most {MAX_FILES_PER_BATCH} files, "
            f"got {len(files)}",
            http_status=422,
        )

    batch_id = uuid.uuid4().hex

    # ---- Phase 1: parse and validate every file outside the DB transaction.
    parsed_objects: list[dicom_io.ParsedObject] = []
    seen_sop_in_batch: set[str] = set()
    for uploaded in files:
        try:
            parsed = dicom_io.parse_and_validate(uploaded.content)
        except dicom_io.DicomValidationError as exc:
            raise IngestionError(
                f"file {uploaded.filename!r} rejected: {exc}", http_status=422
            ) from exc
        if parsed.original_sop_uid in seen_sop_in_batch:
            raise IngestionError(
                f"file {uploaded.filename!r} duplicates SOP UID "
                f"{parsed.original_sop_uid} within the same batch",
                http_status=422,
            )
        seen_sop_in_batch.add(parsed.original_sop_uid)
        parsed_objects.append(parsed)

    ensure_dirs()
    prepared: list[dict] = []

    # ---- Phase 2: one writer transaction covers the whole batch.
    try:
        with immediate_transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO centers (center_id) VALUES (?)",
                (center_id,),
            )

            # Idempotent retransmit / byte-conflict pre-check.
            for parsed in parsed_objects:
                row = conn.execute(
                    """
                    SELECT sha256, new_sop_uid, id
                    FROM instances
                    WHERE center_id = ? AND original_sop_uid = ?
                    """,
                    (center_id, parsed.original_sop_uid),
                ).fetchone()
                if row is not None and row["sha256"] != parsed.sha256:
                    raise IngestionError(
                        f"SOP UID {parsed.original_sop_uid} already exists for "
                        f"center {center_id} with different bytes; the whole "
                        f"batch is rejected"
                    )

            for parsed in parsed_objects:
                existing = conn.execute(
                    """
                    SELECT id, new_sop_uid, stored_path
                    FROM instances
                    WHERE center_id = ? AND original_sop_uid = ? AND sha256 = ?
                    """,
                    (
                        center_id,
                        parsed.original_sop_uid,
                        parsed.sha256,
                    ),
                ).fetchone()

                # Rewrite all UIDs through the deterministic center mapping.
                def map_fn(original_uid: str, kind: str) -> str:
                    return _upsert_mapping(
                        conn, center_id, original_uid, kind, batch_id
                    )

                dicom_io.rewrite_uids(parsed.ds, map_fn)
                encoded = dicom_io.encode_and_verify(parsed)
                new_sop_uid = remap_uid(center_id, parsed.original_sop_uid)
                new_study_uid = remap_uid(center_id, parsed.original_study_uid)
                new_series_uid = remap_uid(center_id, parsed.original_series_uid)

                if existing is not None:
                    # Same center + same original SOP + identical bytes:
                    # retransmit reuses the stored object verbatim.
                    instance_id = existing["id"]
                    stored_path = existing["stored_path"]
                    reused = True
                else:
                    instance_id = None
                    stored_path = ""
                    reused = False

                # Record reference rows even for reused objects: the same
                # bytes may be retransmitted alongside new references.
                for original_ref in dict.fromkeys(parsed.referenced_sop_uids):
                    new_ref = _upsert_mapping(
                        conn, center_id, original_ref, "sop", batch_id
                    )
                    if instance_id is not None:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO instance_references
                                (center_id, source_instance_id,
                                 original_referenced_uid, new_referenced_uid)
                            VALUES (?, ?, ?, ?)
                            """,
                            (center_id, instance_id, original_ref, new_ref),
                        )

                prepared.append(
                    {
                        "parsed": parsed,
                        "encoded": encoded,
                        "new_sop_uid": new_sop_uid,
                        "new_study_uid": new_study_uid,
                        "new_series_uid": new_series_uid,
                        "instance_id": instance_id,
                        "stored_path": stored_path,
                        "reused": reused,
                    }
                )

            # Materialize new objects only after all DB checks have passed.
            for item in prepared:
                if not item["reused"]:
                    parsed = item["parsed"]
                    relative = (
                        f"{center_id}/{item['new_sop_uid']}.{parsed.sha256}.dcm"
                    )
                    absolute = config.STORAGE_DIR / relative
                    absolute.parent.mkdir(parents=True, exist_ok=True)
                    tmp_path = absolute.with_suffix(".part")
                    tmp_path.write_bytes(item["encoded"])
                    tmp_path.replace(absolute)
                    item["stored_path"] = relative
                    cur = conn.execute(
                        """
                        INSERT INTO instances (
                            center_id, batch_id, original_sop_uid, new_sop_uid,
                            original_study_uid, new_study_uid,
                            original_series_uid, new_series_uid, modality,
                            transfer_syntax_uid, sha256, byte_size, stored_path
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            center_id,
                            batch_id,
                            parsed.original_sop_uid,
                            item["new_sop_uid"],
                            parsed.original_study_uid,
                            item["new_study_uid"],
                            parsed.original_series_uid,
                            item["new_series_uid"],
                            parsed.modality,
                            parsed.transfer_syntax_uid,
                            parsed.sha256,
                            len(item["encoded"]),
                            relative,
                        ),
                    )
                    item["instance_id"] = cur.lastrowid

                    for original_ref in dict.fromkeys(parsed.referenced_sop_uids):
                        new_ref = remap_uid(center_id, original_ref)
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO instance_references (
                                center_id, source_instance_id,
                                original_referenced_uid, new_referenced_uid
                            ) VALUES (?, ?, ?, ?)
                            """,
                            (
                                center_id,
                                cur.lastrowid,
                                original_ref,
                                new_ref,
                            ),
                        )

            # SOP mappings carried by ingested objects are now present.
            conn.execute(
                """
                UPDATE uid_mappings SET present = 1
                WHERE center_id = ? AND uid_kind = 'sop' AND original_uid IN (
                    SELECT original_sop_uid FROM instances WHERE center_id = ?
                )
                """,
                (center_id, center_id),
            )
            # Recompute resolution for every reference of this center in one
            # shot; this also resolves references created by earlier batches.
            conn.execute(
                """
                UPDATE instance_references SET resolved = 1
                WHERE center_id = ? AND new_referenced_uid IN (
                    SELECT new_uid FROM uid_mappings
                    WHERE center_id = ? AND present = 1
                )
                """,
                (center_id, center_id),
            )

            reused_count = sum(1 for item in prepared if item["reused"])
            conn.execute(
                """
                INSERT INTO batches
                    (batch_id, center_id, status, file_count, reason)
                VALUES (?, ?, 'committed', ?, NULL)
                """,
                (batch_id, center_id, len(prepared)),
            )
    except IngestionError:
        raise
    except sqlite3.IntegrityError as exc:
        # The DB transaction rolls back. Remove any object files that were
        # already materialized so no half batch is ever downloadable.
        for item in prepared:
            if not item["reused"] and item.get("stored_path"):
                orphan = config.STORAGE_DIR / item["stored_path"]
                orphan.unlink(missing_ok=True)
        raise IngestionError(f"batch conflicted with existing data: {exc}") from exc

    return {
        "batch_id": batch_id,
        "center_id": center_id,
        "status": "committed",
        "file_count": len(prepared),
        "reused_count": reused_count,
        "files": [
            {
                "original_sop_uid": item["parsed"].original_sop_uid,
                "new_sop_uid": item["new_sop_uid"],
                "new_study_uid": item["new_study_uid"],
                "new_series_uid": item["new_series_uid"],
                "modality": item["parsed"].modality,
                "reused": item["reused"],
            }
            for item in prepared
        ],
    }
