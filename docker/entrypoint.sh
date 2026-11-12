#!/bin/sh
# Ensure the data volume is writable by the unprivileged runtime user, then
# exec the given command (uvicorn by default).
set -e

mkdir -p "${DICOM_INGEST_DATA_DIR}/objects" 2>/dev/null || true

exec "$@"
