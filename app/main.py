"""HTTP API for multi-center DICOM ingestion.

Endpoints
---------
POST /api/v1/batches                 upload a batch (multipart, <= 20 files)
GET  /api/v1/batches/{batch_id}      batch status and per-file UID mapping
GET  /api/v1/batches                 list batches (filter by center)
GET  /api/v1/instances               search instances / inspect UID mapping
GET  /api/v1/instances/{id}/download download the rewritten DICOM object
GET  /api/v1/references/pending      reference-first bookkeeping per center
GET  /healthz                        liveness probe
GET  /docs                           interactive Swagger UI
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
from pydantic import BaseModel

from . import config
from .config import MAX_FILE_BYTES, MAX_FILES_PER_BATCH
from .db import get_connection, init_db
from .service import IngestionError, UploadedFile, ingest_batch


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Create / migrate the schema before accepting traffic.
    init_db()
    yield


app = FastAPI(
    title="Multi-Center DICOM Ingest API",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Ingests Explicit VR Little Endian DICOM objects from multiple source "
        "centers, deterministically remaps every instance/study/series UID "
        "(including nested references) per center, and guarantees atomic, "
        "idempotent batches."
    ),
)


# --------------------------------------------------------------------------- #
# Response schemas
# --------------------------------------------------------------------------- #
class FileResult(BaseModel):
    original_sop_uid: str
    new_sop_uid: str
    new_study_uid: str
    new_series_uid: str
    modality: str | None
    reused: bool


class BatchResult(BaseModel):
    batch_id: str
    center_id: str
    status: str
    file_count: int
    reused_count: int
    files: list[FileResult]


class BatchSummary(BaseModel):
    batch_id: str
    center_id: str
    status: str
    file_count: int
    reason: str | None
    created_at: str


class InstanceRow(BaseModel):
    id: int
    center_id: str
    batch_id: str
    original_sop_uid: str
    new_sop_uid: str
    original_study_uid: str
    new_study_uid: str
    original_series_uid: str
    new_series_uid: str
    modality: str | None
    transfer_syntax_uid: str
    sha256: str
    byte_size: int
    created_at: str


class PendingReference(BaseModel):
    center_id: str
    original_referenced_uid: str
    new_referenced_uid: str
    source_instance_id: int
    resolved: bool


# --------------------------------------------------------------------------- #
# Ingestion
# --------------------------------------------------------------------------- #
@app.post(
    "/api/v1/batches",
    response_model=BatchResult,
    status_code=status.HTTP_201_CREATED,
    tags=["batches"],
    summary="Upload one atomic batch of DICOM files for a source center",
)
async def upload_batch(
    center_id: str = Form(
        ..., description="Source center identifier (letters, digits, _ or -)."
    ),
    files: list[UploadFile] = File(
        ...,
        description=(
            f"1-{MAX_FILES_PER_BATCH} DICOM files, Explicit VR Little Endian "
            "(transfer syntax 1.2.840.10008.1.2.1)."
        ),
    ),
) -> BatchResult:
    if len(files) > MAX_FILES_PER_BATCH:
        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            f"a batch may contain at most {MAX_FILES_PER_BATCH} files",
        )

    payload: list[UploadedFile] = []
    for upload in files:
        content = await upload.read()
        if len(content) > MAX_FILE_BYTES:
            raise HTTPException(
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                f"file {upload.filename!r} exceeds {MAX_FILE_BYTES} bytes",
            )
        payload.append(UploadedFile(filename=upload.filename or "unnamed",
                                    content=content))

    try:
        result = ingest_batch(center_id, payload)
    except IngestionError as exc:
        raise HTTPException(exc.http_status, str(exc)) from exc
    return BatchResult(**result)


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #
@app.get(
    "/api/v1/batches/{batch_id}",
    tags=["batches"],
    summary="Fetch a batch with per-file original/new UID mapping",
)
def get_batch(batch_id: str) -> dict:
    conn = get_connection()
    try:
        batch = conn.execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "batch not found")
        rows = conn.execute(
            "SELECT * FROM instances WHERE batch_id = ? ORDER BY id", (batch_id,)
        ).fetchall()
        return {
            "batch_id": batch["batch_id"],
            "center_id": batch["center_id"],
            "status": batch["status"],
            "file_count": batch["file_count"],
            "reason": batch["reason"],
            "created_at": batch["created_at"],
            "files": [dict(r) for r in rows],
        }
    finally:
        conn.close()


@app.get(
    "/api/v1/batches",
    response_model=list[BatchSummary],
    tags=["batches"],
    summary="List batches, optionally filtered by center",
)
def list_batches(
    center_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[BatchSummary]:
    conn = get_connection()
    try:
        cols = "batch_id, center_id, status, file_count, reason, created_at"
        if center_id:
            rows = conn.execute(
                f"SELECT {cols} FROM batches WHERE center_id = ? "
                "ORDER BY created_at DESC, batch_id DESC LIMIT ? OFFSET ?",
                (center_id, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {cols} FROM batches "
                "ORDER BY created_at DESC, batch_id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [BatchSummary(**dict(r)) for r in rows]
    finally:
        conn.close()


@app.get(
    "/api/v1/instances",
    response_model=list[InstanceRow],
    tags=["instances"],
    summary="Search instances and inspect their UID mappings",
)
def list_instances(
    center_id: str | None = Query(None),
    original_sop_uid: str | None = Query(
        None, description="Original SOP UID as supplied by the source center."
    ),
    new_sop_uid: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> list[InstanceRow]:
    clauses, params = [], []
    if center_id:
        clauses.append("center_id = ?")
        params.append(center_id)
    if original_sop_uid:
        clauses.append("original_sop_uid = ?")
        params.append(original_sop_uid)
    if new_sop_uid:
        clauses.append("new_sop_uid = ?")
        params.append(new_sop_uid)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.extend([limit])
    conn = get_connection()
    try:
        rows = conn.execute(
            f"SELECT * FROM instances {where} ORDER BY id DESC LIMIT ?", params
        ).fetchall()
        return [InstanceRow(**dict(r)) for r in rows]
    finally:
        conn.close()


@app.get(
    "/api/v1/instances/{instance_id}/download",
    tags=["instances"],
    summary="Download the rewritten DICOM object (Explicit VR Little Endian)",
)
def download_instance(instance_id: int) -> FileResponse:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT stored_path, new_sop_uid, transfer_syntax_uid "
            "FROM instances WHERE id = ?",
            (instance_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "instance not found")
    path = config.STORAGE_DIR / row["stored_path"]
    if not path.is_file():
        # Storage and DB must never diverge; treat it as a server fault rather
        # than handing back a partial file.
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "stored object missing; contact the platform operator",
        )
    return FileResponse(
        path,
        media_type="application/dicom",
        filename=f"{row['new_sop_uid']}.dcm",
        headers={
            "X-New-SOP-Instance-UID": row["new_sop_uid"],
            "X-Transfer-Syntax-UID": row["transfer_syntax_uid"],
        },
    )


@app.get(
    "/api/v1/references/pending",
    response_model=list[PendingReference],
    tags=["references"],
    summary="List references whose target image has not arrived yet",
)
def pending_references(
    center_id: str = Query(..., description="Center scope; references never cross centers."),
    resolved: bool | None = Query(None),
) -> list[PendingReference]:
    conn = get_connection()
    try:
        sql = (
            "SELECT center_id, original_referenced_uid, new_referenced_uid, "
            "source_instance_id, resolved FROM instance_references WHERE center_id = ?"
        )
        params: list = [center_id]
        if resolved is not None:
            sql += " AND resolved = ?"
            params.append(1 if resolved else 0)
        rows = conn.execute(sql, params).fetchall()
        return [PendingReference(**dict(r)) for r in rows]
    finally:
        conn.close()


@app.get("/healthz", tags=["system"], summary="Liveness/readiness probe")
def healthz() -> dict:
    conn = get_connection()
    try:
        conn.execute("SELECT 1")
    finally:
        conn.close()
    return {"status": "ok"}
