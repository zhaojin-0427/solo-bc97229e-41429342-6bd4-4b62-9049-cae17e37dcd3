"""Background backfill worker: cursor-driven, resumable page ingestion.

Lifecycle and crash safety
--------------------------
* Jobs live in ``backfill_jobs``. A row is ``queued`` until a worker claims it
  with an atomic ``UPDATE ... WHERE status IN ('queued','paused','failed')``
  guarded by a unique ``runner_token``; only one worker can ever own a page.
* All remote reads (manifest fetch, file downloads, DICOM parsing) happen
  *outside* SQLite writer transactions, so a slow center-imaging service never
  blocks uploads or queries on the local database.
* A page is committed through :func:`app.service.commit_backfill_page`:
  instances, references, the batch row and the cursor advance in one
  ``BEGIN IMMEDIATE`` transaction. A crash right after commit is indistinguishable
  from a normal completion: on restart the worker reads the new cursor and
  simply continues, and no second batch is ever created for that page.
* Recoverable trouble (unreachable source, bad file, byte conflict, UID
  mismatch) pauses the job and keeps the old cursor; ``POST .../resume``
  retries the identical page.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from . import config, remote, service
from .config import MAX_FILES_PER_BATCH
from .db import get_connection
from .remote import RemoteSourceError
from .service import IngestionError, UploadedFile

logger = logging.getLogger("backfill")

# Stable error codes surfaced through the API. Codes for transport/HTTP
# failures are produced by app.remote (e.g. download_failed, download_http_error,
# manifest_http_error) and passed through unchanged.
ERR_BAD_DICOM = "bad_dicom"
ERR_UID_MISMATCH = "uid_mismatch"
ERR_BYTE_CONFLICT = "byte_conflict"
ERR_PAGE_INVALID = "page_invalid"
ERR_INTERNAL = "internal_error"


class BackfillError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 409) -> None:
        super().__init__(message)
        self.code = code
        self.http_status = http_status


# --------------------------------------------------------------------------- #
# Job CRUD (synchronous, short-lived SQLite connections)
# --------------------------------------------------------------------------- #
def create_job(
    center_id: str,
    original_study_uid: str,
    source_base_url: str,
    page_size: int,
) -> dict[str, Any]:
    try:
        center_id = service.validate_center_id(center_id)
    except IngestionError as exc:
        raise BackfillError(
            ERR_PAGE_INVALID, str(exc), http_status=exc.http_status
        ) from exc
    original_study_uid = (original_study_uid or "").strip()
    from .uid import is_valid_dicom_uid

    if not original_study_uid or not is_valid_dicom_uid(original_study_uid):
        raise BackfillError(
            ERR_PAGE_INVALID,
            "original_study_uid must be a valid DICOM UID",
            http_status=422,
        )
    source_base_url = (source_base_url or "").strip()
    if not source_base_url.startswith(("http://", "https://")):
        raise BackfillError(
            ERR_PAGE_INVALID,
            "source_base_url must be an http(s) URL",
            http_status=422,
        )
    if not 1 <= page_size <= MAX_FILES_PER_BATCH:
        raise BackfillError(
            ERR_PAGE_INVALID,
            f"page_size must be between 1 and {MAX_FILES_PER_BATCH}",
            http_status=422,
        )

    job_id = uuid.uuid4().hex
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO centers (center_id) VALUES (?)",
            (center_id,),
        )
        try:
            conn.execute(
                """
                INSERT INTO backfill_jobs
                    (job_id, center_id, original_study_uid, source_base_url,
                     page_size, status, cursor)
                VALUES (?, ?, ?, ?, ?, 'queued', '')
                """,
                (job_id, center_id, original_study_uid, source_base_url,
                 page_size),
            )
        except Exception as exc:
            # UNIQUE(center_id, original_study_uid): one backfill per study.
            existing = conn.execute(
                "SELECT job_id FROM backfill_jobs "
                "WHERE center_id = ? AND original_study_uid = ?",
                (center_id, original_study_uid),
            ).fetchone()
            if existing is not None:
                raise BackfillError(
                    "duplicate_job",
                    f"a backfill job {existing['job_id']} already exists for "
                    f"center {center_id!r} and study {original_study_uid!r}",
                    http_status=409,
                ) from exc
            raise
        row = conn.execute(
            "SELECT * FROM backfill_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def get_job(job_id: str) -> dict[str, Any]:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM backfill_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise BackfillError("job_not_found", "backfill job not found",
                                http_status=404)
        return dict(row)
    finally:
        conn.close()


def list_jobs(center_id: str | None, limit: int, offset: int) -> list[dict]:
    conn = get_connection()
    try:
        if center_id:
            rows = conn.execute(
                "SELECT * FROM backfill_jobs WHERE center_id = ? "
                "ORDER BY created_at DESC, job_id DESC LIMIT ? OFFSET ?",
                (center_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM backfill_jobs ORDER BY created_at DESC, "
                "job_id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_attempts(job_id: str, limit: int) -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM backfill_job_attempts WHERE job_id = ? "
            "ORDER BY id DESC LIMIT ?",
            (job_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def request_resume(job_id: str) -> dict[str, Any]:
    """Move a paused/failed job back to the queue; the cursor is untouched."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT status FROM backfill_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise BackfillError("job_not_found", "backfill job not found",
                                http_status=404)
        if row["status"] in ("queued", "running"):
            raise BackfillError(
                "job_not_paused",
                f"job is already {row['status']}; nothing to resume",
                http_status=409,
            )
        if row["status"] == "completed":
            raise BackfillError(
                "job_completed",
                "job already reached the end of the source study",
                http_status=409,
            )
        conn.execute(
            """
            UPDATE backfill_jobs
            SET status = 'queued', runner_token = NULL, runner_started_at = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE job_id = ? AND status IN ('paused', 'failed')
            """,
            (job_id,),
        )
        return get_job(job_id)
    finally:
        conn.close()


def request_pause(job_id: str) -> dict[str, Any]:
    """Operator-requested pause; the current page still finishes atomically."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT status FROM backfill_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise BackfillError("job_not_found", "backfill job not found",
                                http_status=404)
        if row["status"] == "completed":
            raise BackfillError(
                "job_completed", "completed jobs cannot be paused",
                http_status=409,
            )
        # 'paused' is sticky: the worker only claims queued rows.
        conn.execute(
            """
            UPDATE backfill_jobs
            SET status = CASE WHEN status = 'running' THEN 'paused'
                              ELSE status END,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE job_id = ?
            """,
            (job_id,),
        )
        return get_job(job_id)
    finally:
        conn.close()


def recover_stale_runs() -> int:
    """At startup any 'running' row belongs to a dead process; requeue it."""
    conn = get_connection()
    try:
        cur = conn.execute(
            """
            UPDATE backfill_jobs
            SET status = 'queued', runner_token = NULL, runner_started_at = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE status = 'running'
            """
        )
        return cur.rowcount
    finally:
        conn.close()


def _claim_next_job() -> dict[str, Any] | None:
    """Atomically claim one queued job for this worker."""
    token = uuid.uuid4().hex
    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE backfill_jobs
            SET status = 'running', runner_token = ?,
                runner_started_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE job_id = (
                SELECT job_id FROM backfill_jobs
                WHERE status = 'queued'
                ORDER BY created_at
                LIMIT 1
            )
            """,
            (token,),
        )
        row = conn.execute(
            "SELECT * FROM backfill_jobs WHERE runner_token = ?", (token,)
        ).fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()


def _record_pause(
    job_id: str,
    page_no: int,
    cursor: str,
    code: str,
    message: str,
    *,
    fatal: bool = False,
) -> None:
    conn = get_connection()
    try:
        # Never overwrite a terminal/completed state, and only the owning
        # runner may park its own job.
        conn.execute(
            """
            UPDATE backfill_jobs
            SET status = ?, last_error_code = ?, last_error_message = ?,
                last_error_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                runner_token = NULL, runner_started_at = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE job_id = ? AND status = 'running'
            """,
            ("failed" if fatal else "paused", code, message, job_id),
        )
        conn.execute(
            """
            INSERT INTO backfill_job_attempts
                (job_id, page_no, cursor, outcome, error_code, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (job_id, page_no, cursor, "failed" if fatal else "paused",
             code, message),
        )
    finally:
        conn.close()


def _mark_completed(job_id: str) -> None:
    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE backfill_jobs
            SET status = 'completed', runner_token = NULL,
                runner_started_at = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE job_id = ? AND status = 'running'
            """,
            (job_id,),
        )
    finally:
        conn.close()


def _release_running(job_id: str) -> None:
    """Hand a finished/cancelled running job back without an error record."""
    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE backfill_jobs
            SET runner_token = NULL, runner_started_at = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            WHERE job_id = ? AND status = 'running'
            """,
            (job_id,),
        )
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Page processing
# --------------------------------------------------------------------------- #
async def _process_page(
    client: remote.httpx.AsyncClient, job: dict[str, Any]
) -> str:
    """Process one page. Returns the new cursor; '' means the study is done.

    Raises :class:`BackfillError` on any retryable/fatal condition; the job's
    persisted cursor never changes on failure.
    """
    job_id = job["job_id"]
    center_id = job["center_id"]
    study_uid = job["original_study_uid"]
    base_url = job["source_base_url"]
    cursor = job["cursor"]
    page_no = job["pages_committed"]
    page_size = int(job["page_size"])

    # ---- Remote read phase: no SQLite write transaction is open. -------- #
    try:
        page = await remote.fetch_manifest_page(
            client, base_url, study_uid, cursor, page_size
        )
    except RemoteSourceError as exc:
        raise BackfillError(exc.code, str(exc)) from exc

    if not page.items and page.next_cursor:
        raise BackfillError(
            "manifest_error",
            "remote returned an empty page with a non-empty next cursor",
        )

    if not page.items:
        # Empty page + empty next cursor: end of the immutable study.
        return ""

    downloaded: list[UploadedFile] = []
    expected_sops: list[str] = []
    for item in page.items:
        try:
            content = await remote.download_object(client, item.file_url)
        except RemoteSourceError as exc:
            raise BackfillError(exc.code, str(exc)) from exc
        downloaded.append(UploadedFile(filename=item.sop_uid, content=content))
        expected_sops.append(item.sop_uid)

    # Parse/validate outside the writer transaction.
    try:
        parsed_objects = service.parse_files(center_id, downloaded)
    except IngestionError as exc:
        code = ERR_BAD_DICOM if exc.http_status == 422 else ERR_PAGE_INVALID
        raise BackfillError(code, str(exc), http_status=exc.http_status) from exc

    # Cross-check the manifest SOP UID against the DICOM attribute.
    parsed_by_sop = {p.original_sop_uid: p for p in parsed_objects}
    if set(parsed_by_sop) != set(expected_sops):
        raise BackfillError(
            ERR_UID_MISMATCH,
            "SOP UIDs carried by the DICOM objects do not match the manifest: "
            f"manifest={sorted(expected_sops)} "
            f"objects={sorted(parsed_by_sop)}",
        )
    for parsed in parsed_objects:
        if parsed.original_study_uid != study_uid:
            raise BackfillError(
                ERR_UID_MISMATCH,
                f"object SOP {parsed.original_sop_uid} carries StudyInstanceUID "
                f"{parsed.original_study_uid!r}, expected {study_uid!r}",
            )

    # ---- Single atomic commit: instances + references + batch + cursor. - #
    try:
        result = await asyncio.to_thread(
            service.commit_backfill_page,
            job_id=job_id,
            center_id=center_id,
            parsed_objects=parsed_objects,
            next_cursor=page.next_cursor,
            page_no=page_no,
        )
    except IngestionError as exc:
        code = ERR_BAD_DICOM if exc.http_status == 422 else ERR_BYTE_CONFLICT
        raise BackfillError(code, str(exc), http_status=exc.http_status) from exc
    logger.info(
        "job %s committed page %s (%d objects, %d reused) batch=%s",
        job_id, page_no, result["file_count"], result["reused_count"],
        result["batch_id"],
    )
    return page.next_cursor


async def _run_claimed_job(
    client: remote.httpx.AsyncClient, job: dict[str, Any]
) -> None:
    job_id = job["job_id"]
    try:
        while True:
            # Re-read before every page so an operator pause/stop is visible.
            current = await asyncio.to_thread(get_job, job_id)
            if current["status"] != "running":
                # Paused externally between pages; nothing to clean up.
                return
            try:
                next_cursor = await _process_page(client, current)
            except BackfillError as exc:
                logger.warning("job %s paused: %s %s", job_id, exc.code, exc)
                await asyncio.to_thread(
                    _record_pause,
                    job_id,
                    current["pages_committed"],
                    current["cursor"],
                    exc.code,
                    str(exc),
                )
                return
            except Exception as exc:  # defensive: never kill the worker loop
                # An unmapped error while talking to the remote (a server-side
                # stream reset, an ASGI transport quirk, ... ) is always
                # retryable: park the job as paused at the unchanged cursor so
                # an operator can resume it instead of losing the job.
                logger.exception("job %s paused on an unmapped error", job_id)
                await asyncio.to_thread(
                    _record_pause,
                    job_id,
                    current["pages_committed"],
                    current["cursor"],
                    ERR_INTERNAL,
                    repr(exc),
                    fatal=False,
                )
                return

            if next_cursor == "":
                await asyncio.to_thread(_mark_completed, job_id)
                logger.info("job %s completed", job_id)
                return
            # Loop and fetch the next manifest page at the new cursor.
    finally:
        await asyncio.to_thread(_release_running, job_id)


# --------------------------------------------------------------------------- #
# Worker loop
# --------------------------------------------------------------------------- #
class BackfillWorker:
    """Single background task that pumps queued jobs inside this process.

    A fresh HTTP client is built per claimed job (through
    :func:`app.remote.new_http_client`, which tests can replace) and closed
    when the job parks or completes, so network resources never leak across
    jobs and transport injection does not require restarting the worker.
    """

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop_event: asyncio.Event | None = None

    async def start(self) -> None:
        stale = await asyncio.to_thread(recover_stale_runs)
        if stale:
            logger.info("requeued %d stale running job(s) from a previous run",
                        stale)
        # A fresh event per start: a previous stop() leaves the old one set.
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="backfill-worker")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

    async def _loop(self) -> None:
        stop_event = self._stop_event
        assert stop_event is not None
        while not stop_event.is_set():
            try:
                job = await asyncio.to_thread(_claim_next_job)
            except Exception:  # noqa: BLE001
                logger.exception("failed to claim a backfill job")
                job = None
            if job is None:
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=config.BACKFILL_POLL_INTERVAL_S,
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            http_client = remote.new_http_client()
            try:
                try:
                    await _run_claimed_job(http_client, job)
                except Exception:  # noqa: BLE001
                    # The loop must survive any unexpected per-job failure;
                    # park the job as paused (retryable) so it can be resumed.
                    logger.exception("worker loop caught error for job %s",
                                     job["job_id"])
                    await asyncio.to_thread(
                        _record_pause,
                        job["job_id"],
                        job["pages_committed"],
                        job["cursor"],
                        ERR_INTERNAL,
                        "worker caught an unexpected error",
                        fatal=False,
                    )
            finally:
                await http_client.aclose()


worker = BackfillWorker()
