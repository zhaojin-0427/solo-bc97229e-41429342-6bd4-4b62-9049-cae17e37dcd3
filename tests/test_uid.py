"""Unit tests for deterministic UID remapping."""
from __future__ import annotations

from app.uid import is_valid_dicom_uid, remap_uid


def test_remap_is_deterministic():
    a = remap_uid("center-a", "1.2.3.4")
    b = remap_uid("center-a", "1.2.3.4")
    assert a == b
    assert a.startswith("2.25.")


def test_same_uid_different_centers_collision_avoided():
    original = "1.9.84.100"
    assert remap_uid("center-a", original) != remap_uid("center-b", original)


def test_different_uids_same_center_differ():
    assert remap_uid("c", "1.2.3.4") != remap_uid("c", "1.2.3.5")


def test_remapped_uid_is_valid_dicom_uid():
    uid = remap_uid("center-a", "1.2.3.4")
    assert is_valid_dicom_uid(uid)
    assert len(uid) <= 64
    assert all(ch in "0123456789." for ch in uid)


def test_validator():
    assert is_valid_dicom_uid("1.2.840.10008.1.2.1")
    assert not is_valid_dicom_uid("")
    assert not is_valid_dicom_uid(".1.2.3")
    assert not is_valid_dicom_uid("1.2.3.")
    assert not is_valid_dicom_uid("1.2.x")
    assert not is_valid_dicom_uid("1." + "1" * 64)
