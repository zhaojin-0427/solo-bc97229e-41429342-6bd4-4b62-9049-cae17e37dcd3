"""Fault injection bookkeeping for the local source service.

A fault is keyed by SOP UID and describes what the object endpoint should do
when that object is requested:

* ``truncate``  -> return the first half of the bytes (bad DICOM on arrival)
* ``corrupt``   -> flip the last byte (valid container, wrong content hash)
* ``http_500``  -> answer HTTP 500 (simulated server failure)
* ``disconnect``-> close the connection mid-stream (broken download)
"""
from __future__ import annotations

import json
import threading

from . import config

_LOCK = threading.Lock()


def _read() -> dict:
    if not config.FAULTS_PATH.is_file():
        return {}
    try:
        return json.loads(config.FAULTS_PATH.read_text())
    except (ValueError, OSError):
        return {}


def _write(payload: dict) -> None:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = config.FAULTS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
    tmp.replace(config.FAULTS_PATH)


def all_faults() -> dict:
    with _LOCK:
        return _read()


def get_fault(sop_uid: str) -> str | None:
    with _LOCK:
        return _read().get(sop_uid)


def set_fault(sop_uid: str, kind: str) -> dict:
    allowed = {"truncate", "corrupt", "http_500", "disconnect"}
    if kind not in allowed:
        raise ValueError(f"unknown fault kind {kind!r}; allowed: {sorted(allowed)}")
    with _LOCK:
        payload = _read()
        payload[sop_uid] = kind
        _write(payload)
        return payload


def clear_fault(sop_uid: str) -> dict:
    with _LOCK:
        payload = _read()
        payload.pop(sop_uid, None)
        _write(payload)
        return payload


def clear_all() -> dict:
    with _LOCK:
        _write({})
        return {}
