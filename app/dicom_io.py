"""DICOM parsing, validation and deterministic UID rewriting.

Only Explicit VR Little Endian (transfer syntax 1.2.840.10008.1.2.1) objects
are accepted. Rewriting touches UID-valued elements only:

* top-level StudyInstanceUID / SeriesInstanceUID / SOPInstanceUID
* MediaStorageSOPInstanceUID in the file meta information header
* every nested ReferencedSOPInstanceUID (recursively, at any sequence depth)
* SeriesInstanceUID inside reference sequences, FrameOfReferenceUID and
  ReferencedFrameOfReferenceUID

Everything else is preserved byte-for-byte where it matters: the transfer
syntax UID, the modality and all other attributes, and the PixelData bytes.
SOP-class UIDs are *not* remapped: they identify a DICOM class, not an
instance, and rewriting them would make the object non-conformant.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from io import BytesIO
from typing import Callable

import pydicom
from pydicom.dataset import Dataset
from pydicom.uid import UID

from .config import EXPLICIT_VR_LITTLE_ENDIAN
from .uid import is_valid_dicom_uid


class DicomValidationError(ValueError):
    """Raised when an uploaded object cannot be ingested as-is."""


# UID elements that must be remapped. SOP-class / transfer-syntax UIDs are
# deliberately absent: they are class identifiers, not instance identifiers.
REWRITABLE_KINDS: dict[str, str] = {
    "StudyInstanceUID": "study",
    "SeriesInstanceUID": "series",
    "SOPInstanceUID": "sop",
    "ReferencedSOPInstanceUID": "sop",
    "FrameOfReferenceUID": "other_ui",
    "ReferencedFrameOfReferenceUID": "other_ui",
}

REQUIRED_TOP_LEVEL = ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID")


@dataclass
class ParsedObject:
    """Result of parsing one uploaded file, before any rewriting."""

    raw: bytes
    sha256: str
    ds: Dataset
    original_study_uid: str
    original_series_uid: str
    original_sop_uid: str
    modality: str | None
    transfer_syntax_uid: str
    pixel_data: bytes | None
    # Original UIDs found in ReferencedSOPInstanceUID elements (any depth).
    referenced_sop_uids: list[str] = field(default_factory=list)


def _iter_ui_elements(ds: Dataset):
    """Yield every UI-valated DataElement outside the file meta header,
    descending recursively into sequences."""

    for elem in ds:
        if elem.VR == "SQ":
            for item in elem.value:
                yield from _iter_ui_elements(item)
        elif elem.VR == "UI":
            yield elem


def _collect_referenced_sop_uids(ds: Dataset) -> list[str]:
    refs: list[str] = []
    for elem in _iter_ui_elements(ds):
        if elem.keyword == "ReferencedSOPInstanceUID":
            if elem.VM > 1:
                refs.extend(str(v).strip() for v in elem.value)
            else:
                refs.append(str(elem.value).strip())
    return refs


def parse_and_validate(raw: bytes) -> ParsedObject:
    """Parse uploaded bytes and enforce every ingestion precondition."""

    if not raw:
        raise DicomValidationError("empty file")
    try:
        ds = pydicom.dcmread(BytesIO(raw), force=False)
    except Exception as exc:  # pydicom raises a variety of error types
        raise DicomValidationError(f"not a readable DICOM object: {exc}") from exc

    # File meta information + DICM preamble are mandatory for the explicit-VR
    # media storage profile.
    if not hasattr(ds, "file_meta") or ds.file_meta is None:
        raise DicomValidationError("missing (0002,eeee) file meta information")
    tsuid = str(getattr(ds.file_meta, "TransferSyntaxUID", "")).strip()
    if tsuid != EXPLICIT_VR_LITTLE_ENDIAN:
        raise DicomValidationError(
            f"unsupported transfer syntax {tsuid!r}; only Explicit VR Little "
            f"Endian ({EXPLICIT_VR_LITTLE_ENDIAN}) is accepted"
        )
    if ds.is_implicit_VR or not ds.is_little_endian:
        raise DicomValidationError(
            "object is not encoded as Explicit VR Little Endian"
        )

    for keyword in REQUIRED_TOP_LEVEL:
        value = str(getattr(ds, keyword, "") or "").strip()
        if not value:
            raise DicomValidationError(f"missing required UID attribute {keyword}")
        if not is_valid_dicom_uid(value):
            raise DicomValidationError(f"invalid DICOM UID in {keyword}: {value!r}")

    meta_sop = str(getattr(ds.file_meta, "MediaStorageSOPInstanceUID", "") or "").strip()
    if meta_sop and not is_valid_dicom_uid(meta_sop):
        raise DicomValidationError(
            f"invalid DICOM UID in MediaStorageSOPInstanceUID: {meta_sop!r}"
        )

    # Every UID we are about to rewrite must itself be a valid DICOM UID.
    for elem in _iter_ui_elements(ds):
        if elem.keyword in REWRITABLE_KINDS:
            values = [str(v).strip() for v in elem.value] if elem.VM > 1 else [
                str(elem.value).strip()
            ]
            for value in values:
                if not is_valid_dicom_uid(value):
                    raise DicomValidationError(
                        f"invalid DICOM UID in {elem.keyword}: {value!r}"
                    )

    sha256 = hashlib.sha256(raw).hexdigest()
    return ParsedObject(
        raw=raw,
        sha256=sha256,
        ds=ds,
        original_study_uid=str(ds.StudyInstanceUID).strip(),
        original_series_uid=str(ds.SeriesInstanceUID).strip(),
        original_sop_uid=str(ds.SOPInstanceUID).strip(),
        modality=str(getattr(ds, "Modality", "") or "").strip() or None,
        transfer_syntax_uid=tsuid,
        pixel_data=getattr(ds, "PixelData", None),
        referenced_sop_uids=_collect_referenced_sop_uids(ds),
    )


def rewrite_uids(ds: Dataset, map_fn: Callable[[str, str], str]) -> None:
    """Remap every rewritable UID in place, recursively including sequences.

    ``map_fn(original_uid, kind)`` returns the new UID; ``kind`` is one of
    ``study`` / ``series`` / ``sop`` / ``other_ui`` and is intended for
    bookkeeping, while the mapping value itself depends only on
    (center, original UID).
    """

    for elem in _iter_ui_elements(ds):
        kind = REWRITABLE_KINDS.get(elem.keyword)
        if kind is None:
            continue
        if elem.VM > 1:
            elem.value = [map_fn(str(v).strip(), kind) for v in elem.value]
        else:
            elem.value = map_fn(str(elem.value).strip(), kind)

    file_meta = getattr(ds, "file_meta", None)
    if file_meta is not None and "MediaStorageSOPInstanceUID" in file_meta:
        original = str(file_meta.MediaStorageSOPInstanceUID).strip()
        if original:
            file_meta.MediaStorageSOPInstanceUID = UID(map_fn(original, "sop"))


def encode_and_verify(parsed: ParsedObject) -> bytes:
    """Serialize the rewritten dataset and verify invariants on the result."""

    buffer = BytesIO()
    parsed.ds.save_as(buffer, enforce_file_format=True)
    encoded = buffer.getvalue()

    # Re-read the produced object and prove the promises we make to callers.
    check = pydicom.dcmread(BytesIO(encoded), force=False)
    check_tsuid = str(check.file_meta.TransferSyntaxUID).strip()
    if check_tsuid != parsed.transfer_syntax_uid:
        raise DicomValidationError(
            "internal error: transfer syntax changed during rewriting"
        )
    if check.is_implicit_VR or not check.is_little_endian:
        raise DicomValidationError(
            "internal error: output is not Explicit VR Little Endian"
        )
    if parsed.pixel_data is not None:
        if getattr(check, "PixelData", None) != parsed.pixel_data:
            raise DicomValidationError(
                "internal error: PixelData bytes changed during rewriting"
            )
    for keyword in REQUIRED_TOP_LEVEL:
        if not str(getattr(check, keyword, "") or "").strip():
            raise DicomValidationError(
                f"internal error: {keyword} missing after rewriting"
            )
    return encoded
