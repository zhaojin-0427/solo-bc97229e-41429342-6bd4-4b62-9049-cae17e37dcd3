"""Runtime configuration.

Everything can be overridden through environment variables so the same image
runs locally, under docker compose and in CI.
"""
from __future__ import annotations

import os
from pathlib import Path

# SQLite database file and rewritten DICOM objects live here.
DATA_DIR = Path(os.getenv("DICOM_INGEST_DATA_DIR", "/data")).resolve()
DB_PATH = DATA_DIR / "ingest.db"
STORAGE_DIR = DATA_DIR / "objects"

# A batch carries at most this many files (hard product requirement: <= 20).
MAX_FILES_PER_BATCH = int(os.getenv("DICOM_INGEST_MAX_FILES", "20"))

# Max accepted upload size per file (50 MiB by default, generous for synthetic
# samples and small real slices).
MAX_FILE_BYTES = int(os.getenv("DICOM_INGEST_MAX_FILE_BYTES", str(50 * 1024 * 1024)))

# DICOM UID root for deterministic remapping. 2.25 is the ISO/ITU root reserved
# for UUID-derived UIDs (DICOM PS3.5 B.2); the suffix is a 128-bit UUID so no
# central UID registration authority is required.
UID_ROOT = os.getenv("DICOM_INGEST_UID_ROOT", "2.25")

# Explicit VR Little Endian is the only accepted transfer syntax.
EXPLICIT_VR_LITTLE_ENDIAN = "1.2.840.10008.1.2.1"

# SQLite busy timeout (milliseconds) for concurrent writers.
BUSY_TIMEOUT_MS = int(os.getenv("DICOM_INGEST_BUSY_TIMEOUT_MS", "30000"))


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
