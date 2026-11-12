"""Study registry: scan DICOM files once at startup and serve stable cursors.

The manifest contract is intentionally identical to what a real center-imaging
service would expose:

    {
      "study_uid": "...",
      "page_size": 5,
      "next_cursor": "5" | "",
      "items": [
        {"sop_uid": "...", "file_url": "/objects/<object_id>"},
        ...
      ]
    }

An empty ``next_cursor`` marks the end of the study. The backing content is
treated as immutable for the lifetime of a backfill job.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path

from pydicom import dcmread

from . import config


@dataclass(frozen=True)
class SourceObject:
    object_id: str
    study_uid: str
    sop_uid: str
    file_path: Path


class StudyNotFound(KeyError):
    pass


class Registry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._studies: dict[str, list[SourceObject]] = {}

    def load(self) -> int:
        """Index every ``*.dcm`` under ``STUDIES_DIR`` (sorted for stable order)."""
        studies: dict[str, list[SourceObject]] = {}
        if config.STUDIES_DIR.is_dir():
            for path in sorted(config.STUDIES_DIR.rglob("*.dcm")):
                ds = dcmread(str(path), stop_before_pixels=True, force=False)
                study_uid = str(ds.StudyInstanceUID).strip()
                sop_uid = str(ds.SOPInstanceUID).strip()
                studies.setdefault(study_uid, []).append(
                    SourceObject(
                        object_id=f"{study_uid}/{sop_uid}",
                        study_uid=study_uid,
                        sop_uid=sop_uid,
                        file_path=path,
                    )
                )
        with self._lock:
            self._studies = studies
        return sum(len(items) for items in studies.values())

    def study_uids(self) -> list[str]:
        with self._lock:
            return sorted(self._studies)

    def page(
        self, study_uid: str, cursor: str, page_size: int
    ) -> tuple[list[SourceObject], str]:
        with self._lock:
            items = self._studies.get(study_uid)
        if items is None:
            raise StudyNotFound(study_uid)
        offset = int(cursor or "0")
        if offset < 0 or offset > len(items):
            raise ValueError("cursor out of range")
        page_size = max(1, min(page_size, config.MAX_PAGE_SIZE))
        window = items[offset:offset + page_size]
        next_offset = offset + len(window)
        next_cursor = "" if next_offset >= len(items) else str(next_offset)
        return window, next_cursor

    def get_object(self, object_id: str) -> SourceObject:
        with self._lock:
            for items in self._studies.values():
                for item in items:
                    if item.object_id == object_id:
                        return item
        raise KeyError(object_id)


registry = Registry()
