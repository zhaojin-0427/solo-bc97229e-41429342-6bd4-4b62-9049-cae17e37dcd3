"""Local stand-in for a remote center-imaging service.

It serves an immutable, cursor-paginated manifest plus DICOM object downloads,
and supports deterministic fault injection so the backfill worker's atomicity
and retry behavior can be demonstrated end to end:

* ``GET /manifest?study_uid=&cursor=&page_size=`` -> JSON manifest page
* ``GET /objects/{object_id}``                   -> raw DICOM bytes
* ``GET/POST/DELETE /faults``                    -> fault injection control

Faults are stored in a JSON file under the data directory, so they survive
restarts and can be toggled while the demo is running.
"""
from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(
    os.getenv("SOURCE_SERVICE_DATA_DIR", "/source-data")
).resolve()
STUDIES_DIR = DATA_DIR / "studies"
FAULTS_PATH = DATA_DIR / "faults.json"
MAX_PAGE_SIZE = 20
