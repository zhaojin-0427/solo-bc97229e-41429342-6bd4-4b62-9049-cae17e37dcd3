"""Batch ingestion service: atomicity, idempotency and reference resolution.

The work is split in two reusable phases so that both the synchronous upload
API and the background backfill worker obey exactly the same rules:

* :func:`parse_files` parses and validates every object *outside* any SQLite
  write transaction (remote reads therefore never hold the writer lock).
* :func:`commit_parsed_batch` materializes a whole page in one
  ``BEGIN IMMEDIATE`` transaction; either every instance, reference, batch row
  and (for backfill) cursor update lands together, or none of them does.
"""
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


def validate_center_id(center_id: str) -> str:
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


def parse_files(
    center_id: str, files: list[UploadedFile]
) -> list[dicom_io.ParsedObject]:
    """Phase 1: parse/validate every object, holding no SQLite write lock."""
    center_id = validate_center_id(center_id)
    if not files:
        raise IngestionError("at least one DICOM file is required", http_status=422)
    if len(files) > MAX_FILES_PER_BATCH:
        raise IngestionError(
            f"a batch may contain at most {MAX_FILES_PER_BATCH} files, "
            f"got {len(files)}",
            http_status=422,
        )

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
    return parsed_objects


def commit_parsed_batch(
    center_id: str,
    parsed_objects: list[dicom_io.ParsedObject],
    *,
    batch_id: str | None = None,
    origin_kind: str = "upload",
    origin_job_id: str | None = None,
    page_no: int | None = None,
    cursor: str | None = None,
) -> dict:
    """Phase 2: commit an already-validated page in one writer transaction.

    ``origin_kind`` is ``"upload"`` for the synchronous API or ``"backfill"``
    for worker pages; the extra origin columns make backfill-created batches
    traceable back to their job and page.
    """
    center_id = validate_center_id(center_id)
    if not parsed_objects:
        raise IngestionError("at least one DICOM file is required", http_status=422)
    if len(parsed_objects) > MAX_FILES_PER_BATCH:
        raise IngestionError(
            f"a batch may contain at most {MAX_FILES_PER_BATCH} files, "
            f"got {len(parsed_objects)}",
            http_status=422,
        )

    batch_id = batch_id or uuid.uuid4().hex
    ensure_dirs()
    prepared: list[dict] = []

    # ---- One writer transaction covers the whole batch. ------------------ #
    try:
        with immediate_transaction() as conn:
            _commit_parsed_batch_on_conn(
                conn,
                center_id,
                parsed_objects,
                batch_id=batch_id,
                origin_kind=origin_kind,
                origin_job_id=origin_job_id,
                page_no=page_no,
                cursor=cursor,
                prepared=prepared,
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
        "reused_count": sum(1 for item in prepared if item["reused"]),
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


def ingest_batch(center_id: str, files: list[UploadedFile]) -> dict:
    """Ingest an uploaded batch atomically (parse phase then commit phase).

    Returns a summary dict on success; raises :class:`IngestionError` when the
    batch is rejected (in which case no row and no file is persisted).
    """
    parsed_objects = parse_files(center_id, files)
    return commit_parsed_batch(center_id, parsed_objects)


def _commit_parsed_batch_on_conn(
    conn: sqlite3.Connection,
    center_id: str,
    parsed_objects: list[dicom_io.ParsedObject],
    *,
    batch_id: str,
    origin_kind: str,
    origin_job_id: str | None,
    page_no: int | None,
    cursor: str | None,
    prepared: list[dict],
) -> None:
    """Run every batch write on an already-open writer connection.

    The caller owns the transaction; on any exception nothing is committed and
    the caller is responsible for rolling back and removing orphan files.
    """
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

        # Record reference rows even for reused objects: the same bytes may
        # be retransmitted alongside new references.
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
    # Recompute resolution for every reference of this center in one shot;
    # this also resolves references created by earlier batches/pages.
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

    conn.execute(
        """
        INSERT INTO batches
            (batch_id, center_id, status, file_count, reason,
             origin_kind, origin_job_id, page_no, cursor)
        VALUES (?, ?, 'committed', ?, NULL, ?, ?, ?, ?)
        """,
        (
            batch_id,
            center_id,
            len(prepared),
            origin_kind,
            origin_job_id,
            page_no,
            cursor,
        ),
    )


def commit_backfill_page(
    *,
    job_id: str,
    center_id: str,
    parsed_objects: list[dicom_io.ParsedObject],
    next_cursor: str,
    page_no: int,
) -> dict:
    """Commit one backfill page: instances, references, batch row *and* the
    job cursor advance in a single ``BEGIN IMMEDIATE`` transaction.

    Either the whole page becomes visible and the cursor moves, or nothing at
    all changes — a crash between any two statements leaves the job resumable
    at the previous cursor, so a page can never be ingested twice into two
    batches.
    """
    center_id = validate_center_id(center_id)
    if not parsed_objects:
        raise IngestionError("a backfill page must contain at least one object",
                             http_status=422)
    if len(parsed_objects) > MAX_FILES_PER_BATCH:
        raise IngestionError(
            f"a page may contain at most {MAX_FILES_PER_BATCH} objects, "
            f"got {len(parsed_objects)}",
            http_status=422,
        )

    batch_id = uuid.uuid4().hex
    ensure_dirs()
    prepared: list[dict] = []
    try:
        with immediate_transaction() as conn:
            # Claim ownership inside the writer transaction so two workers
            # (e.g. a concurrent resume request against a running job) cannot
            # commit the same page twice.
            job = conn.execute(
                "SELECT status, runner_token FROM backfill_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if job is None:
                raise IngestionError(f"backfill job {job_id} not found",
                                     http_status=404)
            if job["status"] != "running":
                raise IngestionError(
                    f"job {job_id} is {job['status']}, not running; refusing to "
                    f"commit a page",
                    http_status=409,
                )

            _commit_parsed_batch_on_conn(
                conn,
                center_id,
                parsed_objects,
                batch_id=batch_id,
                origin_kind="backfill",
                origin_job_id=job_id,
                page_no=page_no,
                cursor=next_cursor,
                prepared=prepared,
            )

            reused_count = sum(1 for item in prepared if item["reused"])
            # The cursor advances in the SAME transaction as the batch row.
            conn.execute(
                """
                UPDATE backfill_jobs
                SET cursor = ?,
                    pages_committed = pages_committed + 1,
                    instances_committed = instances_committed + ?,
                    instances_reused = instances_reused + ?,
                    last_batch_id = ?,
                    last_error_code = NULL,
                    last_error_message = NULL,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE job_id = ?
                """,
                (
                    next_cursor,
                    len(prepared),
                    reused_count,
                    batch_id,
                    job_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO backfill_job_attempts
                    (job_id, page_no, cursor, outcome, batch_id,
                     item_count, reused_count)
                VALUES (?, ?, ?, 'committed', ?, ?, ?)
                """,
                (
                    job_id,
                    page_no,
                    next_cursor,
                    batch_id,
                    len(prepared),
                    reused_count,
                ),
            )
    except IngestionError:
        raise
    except sqlite3.IntegrityError as exc:
        for item in prepared:
            if not item["reused"] and item.get("stored_path"):
                orphan = config.STORAGE_DIR / item["stored_path"]
                orphan.unlink(missing_ok=True)
        raise IngestionError(f"page conflicted with existing data: {exc}") from exc

    return {
        "batch_id": batch_id,
        "center_id": center_id,
        "status": "committed",
        "file_count": len(prepared),
        "reused_count": sum(1 for item in prepared if item["reused"]),
        "next_cursor": next_cursor,
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
