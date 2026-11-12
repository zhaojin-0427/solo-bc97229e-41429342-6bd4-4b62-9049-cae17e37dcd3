"""FastAPI app: paginated manifest, object download and fault injection."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from . import config, faults
from .registry import StudyNotFound, registry


@asynccontextmanager
async def lifespan(_app: FastAPI):
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = registry.load()
    # faults.json is created lazily; ensure the directory exists now.
    print(f"[source-service] indexed {count} DICOM object(s) under "
          f"{config.STUDIES_DIR}")
    yield


app = FastAPI(
    title="Local Center-Imaging Source Service",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Test double for a remote center-imaging service. Serves an immutable "
        "cursor-paginated SOP manifest and Explicit VR Little Endian DICOM "
        "downloads, with runtime fault injection (truncation, corruption, "
        "HTTP 500, mid-stream disconnect)."
    ),
)


class ManifestItem(BaseModel):
    sop_uid: str
    file_url: str


class ManifestResponse(BaseModel):
    study_uid: str
    page_size: int
    next_cursor: str
    items: list[ManifestItem]


class FaultIn(BaseModel):
    sop_uid: str
    kind: str  # truncate | corrupt | http_500 | disconnect


@app.get("/healthz", tags=["system"], summary="Liveness probe")
def healthz() -> dict:
    return {"status": "ok", "studies": registry.study_uids()}


@app.get(
    "/manifest",
    response_model=ManifestResponse,
    tags=["manifest"],
    summary="Cursor-paginated SOP manifest for one study (1-20 items per page)",
)
def manifest(
    study_uid: str = Query(..., description="Original Study Instance UID."),
    cursor: str = Query("", description="Opaque cursor; empty string = first page."),
    page_size: int = Query(5, ge=1, le=config.MAX_PAGE_SIZE),
) -> ManifestResponse:
    try:
        window, next_cursor = registry.page(study_uid, cursor, page_size)
    except StudyNotFound as exc:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"study {study_uid!r} not found"
        ) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    return ManifestResponse(
        study_uid=study_uid,
        page_size=page_size,
        next_cursor=next_cursor,
        items=[
            ManifestItem(sop_uid=item.sop_uid, file_url=f"/objects/{item.object_id}")
            for item in window
        ],
    )


def _chunked(data: bytes, chunk_size: int = 4096):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


@app.get(
    "/objects/{object_id:path}",
    tags=["objects"],
    summary="Download one DICOM object (fault injection aware)",
)
def get_object(object_id: str):
    try:
        item = registry.get_object(object_id)
    except KeyError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "object not found") from exc

    raw = item.file_path.read_bytes()
    kind = faults.get_fault(item.sop_uid)
    if kind == "http_500":
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "injected fault: simulated source server error",
        )
    if kind == "truncate":
        # Half the bytes: the backfill worker must reject this as bad DICOM
        # and pause without advancing the cursor.
        raw = raw[: max(1, len(raw) // 2)]
        return StreamingResponse(
            _chunked(raw),
            media_type="application/dicom",
            headers={"X-Injected-Fault": "truncate"},
        )
    if kind == "corrupt":
        # Valid DICOM container but different bytes -> deterministic SHA-256
        # change triggers the same-center byte-conflict rule.
        mutated = bytearray(raw)
        mutated[-1] = (mutated[-1] + 1) % 256
        raw = bytes(mutated)
        return StreamingResponse(
            _chunked(raw),
            media_type="application/dicom",
            headers={"X-Injected-Fault": "corrupt"},
        )
    if kind == "disconnect":
        # Stream half the body as chunked data and then raise a non-cancelled
        # error: uvicorn aborts the response without sending a terminating
        # zero-length chunk, so the client's read fails at the transport level
        # — a faithful model of a TCP hang-up mid-download.
        cut = max(1, len(raw) // 2)

        async def broken_stream():
            yield raw[:cut]
            raise OSError("injected fault: mid-stream connection reset")

        return StreamingResponse(
            broken_stream(),
            media_type="application/dicom",
            headers={
                "X-Injected-Fault": "disconnect",
                "Connection": "close",
            },
        )

    return StreamingResponse(
        _chunked(raw),
        media_type="application/dicom",
        headers={"Content-Length": str(len(raw))},
    )


# --------------------------------------------------------------------------- #
# Fault injection control
# --------------------------------------------------------------------------- #
@app.get("/faults", tags=["faults"], summary="List active injected faults")
def list_faults() -> dict:
    return {"faults": faults.all_faults()}


@app.post(
    "/faults",
    tags=["faults"],
    status_code=status.HTTP_201_CREATED,
    summary="Inject a fault for one SOP UID",
)
def add_fault(payload: FaultIn) -> JSONResponse:
    try:
        updated = faults.set_fault(payload.sop_uid, payload.kind)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    return JSONResponse({"faults": updated}, status_code=status.HTTP_201_CREATED)


@app.delete("/faults/{sop_uid}", tags=["faults"], summary="Clear one fault")
def remove_fault(sop_uid: str) -> dict:
    return {"faults": faults.clear_fault(sop_uid)}


@app.delete("/faults", tags=["faults"], summary="Clear all faults")
def remove_all_faults() -> dict:
    return {"faults": faults.clear_all()}
