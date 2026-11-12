"""Deterministic UID remapping.

The new UID is a pure function of *source center id* and *original UID*:

    new_uid = "2.25." + decimal(uuid5(NAMESPACE, center_id + "|" + original_uid))

Properties this guarantees:

* The same (center, original UID) pair always maps to the same new UID, even
  across processes and restarts, because no database state is needed to
  compute it. The database stores the mapping anyway for auditing, reference
  bookkeeping and lookup.
* The same original UID coming from two different centers yields two
  different new UIDs, so references can never be wired across centers.
* Resulting UIDs comply with DICOM PS3.5 (digits and dots, <= 64 chars):
  a 128-bit UUID rendered as a decimal number is at most 39 digits, and the
  "2.25." prefix adds 5 characters -> at most 44 characters.
"""
from __future__ import annotations

import uuid

from .config import UID_ROOT

# Fixed namespace: makes the uuid5 computation self-contained and stable.
NAMESPACE = uuid.UUID("6f9619ff-8b86-d011-b42d-00cf4fc964ff")

# DICOM UID: digits 0-9 separated by dots, max 64 characters, no trailing dot.
_MAX_UID_LEN = 64


def remap_uid(center_id: str, original_uid: str, uid_root: str = UID_ROOT) -> str:
    """Return the stable remapped UID for a center / original-UID pair."""
    original_uid = original_uid.strip()
    if not original_uid:
        raise ValueError("original UID must not be empty")
    digest = uuid.uuid5(NAMESPACE, f"{center_id}|{original_uid}")
    new_uid = f"{uid_root}.{digest.int}"
    if len(new_uid) > _MAX_UID_LEN:  # pragma: no cover - structurally impossible for 2.25
        raise ValueError(f"remapped UID exceeds {_MAX_UID_LEN} characters: {new_uid}")
    return new_uid


def is_valid_dicom_uid(value: str) -> bool:
    """Lightweight DICOM PS3.5 UID validator."""
    if not value or len(value) > _MAX_UID_LEN:
        return False
    if value.startswith(".") or value.endswith("."):
        return False
    allowed = set("0123456789.")
    return all(ch in allowed for ch in value)
