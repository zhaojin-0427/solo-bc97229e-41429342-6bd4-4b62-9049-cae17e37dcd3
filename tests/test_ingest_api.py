"""End-to-end requirement tests for the DICOM ingestion platform."""
from __future__ import annotations

import io
import os
from pathlib import Path

import pydicom

from app.uid import remap_uid

import generate_samples as gen

from conftest import upload


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


# --------------------------------------------------------------------------- #
# Basic ingestion + deterministic UID rewriting
# --------------------------------------------------------------------------- #
def test_ingest_rewrites_uids_and_preserves_pixels_and_ts(client, samples_dir):
    image = samples_dir / "reference_first" / "image_001.dcm"
    original = pydicom.dcmread(str(image))
    original_px = original.PixelData

    resp = upload(client, "center-a", [image])
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "committed"
    assert body["reused_count"] == 0

    expected_sop = remap_uid("center-a", gen.REF_TARGET_SOP_UID)
    assert body["files"][0]["new_sop_uid"] == expected_sop

    # Batch query returns the mapping.
    batch = client.get(f"/api/v1/batches/{body['batch_id']}").json()
    assert batch["status"] == "committed"
    assert batch["files"][0]["new_sop_uid"] == expected_sop

    # Download and verify the rewritten object.
    instance_id = batch["files"][0]["id"]
    dl = client.get(f"/api/v1/instances/{instance_id}/download")
    assert dl.status_code == 200
    assert dl.headers["content-type"] == "application/dicom"
    rewritten = pydicom.dcmread(io.BytesIO(dl.content))
    assert rewritten.SOPInstanceUID == expected_sop
    assert str(rewritten.file_meta.MediaStorageSOPInstanceUID) == expected_sop
    assert str(rewritten.file_meta.TransferSyntaxUID) == (
        "1.2.840.10008.1.2.1"
    )
    assert rewritten.is_implicit_VR is False
    assert rewritten.is_little_endian is True
    assert rewritten.Modality == "CT"
    assert rewritten.PixelData == original_px  # pixel bytes untouched


def test_mapping_is_stable_across_restarts(client, samples_dir, data_dir):
    image = samples_dir / "reference_first" / "image_001.dcm"
    first = upload(client, "center-a", [image]).json()
    new_uid = first["files"][0]["new_sop_uid"]

    # Pure-function property: no state needed, survives process restart.
    assert remap_uid("center-a", gen.REF_TARGET_SOP_UID) == new_uid


# --------------------------------------------------------------------------- #
# Cross-center UID collision: same original UIDs, different centers
# --------------------------------------------------------------------------- #
def test_cross_center_same_original_uid_gets_distinct_new_uids(
    client, samples_dir
):
    a = samples_dir / "cross_center_uid_collision" / "center-a" / "image_001.dcm"
    b = samples_dir / "cross_center_uid_collision" / "center-b" / "image_001.dcm"

    ra = upload(client, "center-a", [a]).json()
    rb = upload(client, "center-b", [b]).json()
    uid_a = ra["files"][0]["new_sop_uid"]
    uid_b = rb["files"][0]["new_sop_uid"]
    assert uid_a != uid_b
    # Deterministic per-center values.
    assert uid_a == remap_uid("center-a", gen.COLLISION_SOP_UID)
    assert uid_b == remap_uid("center-b", gen.COLLISION_SOP_UID)

    # Pixel content stays center-specific (no cross-center mix-up).
    for center, expected_px in (("center-a", 1000), ("center-b", 2000)):
        rows = client.get(
            "/api/v1/instances",
            params={"center_id": center,
                    "original_sop_uid": gen.COLLISION_SOP_UID},
        ).json()
        assert len(rows) == 1
        dl = client.get(
            f"/api/v1/instances/{rows[0]['id']}/download"
        ).content
        ds = pydicom.dcmread(io.BytesIO(dl))
        value = int.from_bytes(ds.PixelData[:2], "little")
        assert value == expected_px


def test_references_never_resolve_across_centers(client, samples_dir):
    struct = samples_dir / "reference_first" / "struct_001.dcm"
    image = samples_dir / "reference_first" / "image_001.dcm"

    # Center A uploads the reference-first object (target missing).
    upload(client, "center-a", [struct])
    pending_a = client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-a", "resolved": False},
    ).json()
    assert len(pending_a) >= 1
    target_new_a = remap_uid("center-a", gen.REF_TARGET_SOP_UID)
    assert pending_a[0]["new_referenced_uid"] == target_new_a

    # Center B uploads an image carrying the SAME original target SOP UID.
    # It must not resolve center A's pending reference.
    upload(client, "center-b", [image])
    still_pending = client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-a", "resolved": False},
    ).json()
    assert len(still_pending) == len(pending_a)

    # Now center A uploads its own image -> its reference resolves.
    upload(client, "center-a", [image])
    assert client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-a", "resolved": False},
    ).json() == []
    resolved = client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-a", "resolved": True},
    ).json()
    assert any(r["new_referenced_uid"] == target_new_a for r in resolved)


# --------------------------------------------------------------------------- #
# Reference-first: nested UID rewriting at every sequence depth
# --------------------------------------------------------------------------- #
def test_reference_first_then_image_resolves_and_rewrites_nested(
    client, samples_dir
):
    struct = samples_dir / "reference_first" / "struct_001.dcm"
    image = samples_dir / "reference_first" / "image_001.dcm"

    first = upload(client, "center-c", [struct]).json()
    pending = client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-c", "resolved": False},
    ).json()
    # Both nested elements point at the same target UID, which is stored once.
    assert len(pending) == 1
    assert pending[0]["resolved"] is False

    # Download the SR and confirm nested references were remapped everywhere.
    inst = client.get(
        "/api/v1/instances", params={"center_id": "center-c"}
    ).json()[0]
    ds = pydicom.dcmread(
        io.BytesIO(client.get(f"/api/v1/instances/{inst['id']}/download").content)
    )
    expected_ref = remap_uid("center-c", gen.REF_TARGET_SOP_UID)
    expected_series = remap_uid("center-c", gen.REF_SERIES_UID)
    top = ds.ReferencedSOPSequence[0]
    assert top.ReferencedSOPInstanceUID == expected_ref
    nested_series = top.ReferencedSeriesSequence[0]
    assert nested_series.SeriesInstanceUID == expected_series
    assert nested_series.ReferencedSOPSequence[0].ReferencedSOPInstanceUID == (
        expected_ref
    )
    # SOP class UIDs are not remapped.
    assert top.ReferencedSOPClassUID == "1.2.840.10008.5.1.4.1.1.2"

    upload(client, "center-c", [image])
    assert client.get(
        "/api/v1/references/pending",
        params={"center_id": "center-c", "resolved": False},
    ).json() == []
    assert first["batch_id"]  # first batch remains queryable


# --------------------------------------------------------------------------- #
# Idempotent retransmit vs. byte conflict
# --------------------------------------------------------------------------- #
def test_same_bytes_retransmit_is_reused(client, samples_dir):
    image = samples_dir / "reference_first" / "image_001.dcm"
    first = upload(client, "center-d", [image]).json()
    second = upload(client, "center-d", [image]).json()
    assert second["reused_count"] == 1
    assert second["files"][0]["new_sop_uid"] == first["files"][0]["new_sop_uid"]

    rows = client.get(
        "/api/v1/instances", params={"center_id": "center-d"}
    ).json()
    assert len(rows) == 1  # no duplicate instance row / file


def test_different_bytes_same_sop_rejects_whole_batch(
    client, samples_dir, tmp_path
):
    image = samples_dir / "reference_first" / "image_001.dcm"
    upload(client, "center-e", [image])

    # Mutate one pixel byte; SOP UID stays the same. The mutated copy lives in
    # the per-test temp dir so the shared session samples stay pristine.
    ds = pydicom.dcmread(str(image))
    raw = bytearray(image.read_bytes())
    raw[-1] = (raw[-1] + 1) % 256
    mutated = tmp_path / "mutated.dcm"
    mutated.write_bytes(bytes(raw))
    # Sanity: SOP UID unchanged.
    assert pydicom.dcmread(str(mutated)).SOPInstanceUID == ds.SOPInstanceUID

    other = samples_dir / "reference_first" / "struct_001.dcm"
    resp = upload(client, "center-e", [mutated, other])
    assert resp.status_code == 409
    assert "different bytes" in resp.text

    # Nothing from the failed batch is queryable or downloadable.
    rows = client.get(
        "/api/v1/instances", params={"center_id": "center-e"}
    ).json()
    assert len(rows) == 1  # only the original image remains
    batches = client.get(
        "/api/v1/batches", params={"center_id": "center-e"}
    ).json()
    assert all(b["status"] == "committed" for b in batches)
    # No half-written object files survive the rolled-back batch.
    objects_dir = Path(os.environ["DICOM_INGEST_DATA_DIR"]) / "objects"
    assert not list(objects_dir.rglob("*.part"))
    # Exactly one stored object remains: the original image, nothing new.
    assert len(list(objects_dir.rglob("*.dcm"))) == 1


def test_invalid_dicom_rejects_whole_batch(client, samples_dir):
    good = samples_dir / "reference_first" / "image_001.dcm"
    bad = samples_dir / "invalid" / "not_a_dicom.dcm"
    resp = upload(client, "center-f", [good, bad])
    assert resp.status_code == 422
    assert "not_a_dicom.dcm" in resp.text

    rows = client.get(
        "/api/v1/instances", params={"center_id": "center-f"}
    ).json()
    assert rows == []
    assert client.get(
        "/api/v1/batches", params={"center_id": "center-f"}
    ).json() == []


# --------------------------------------------------------------------------- #
# Batch size limit + transfer syntax enforcement
# --------------------------------------------------------------------------- #
def test_more_than_twenty_files_rejected(client, samples_dir):
    image = samples_dir / "reference_first" / "image_001.dcm"
    resp = upload(client, "center-g", [image] * 21)
    assert resp.status_code in (413, 422)


def test_duplicate_sop_within_batch_rejected(client, samples_dir):
    image = samples_dir / "reference_first" / "image_001.dcm"
    # Re-uploading the same object twice in one batch is a client error even
    # though identical-byte retransmit in separate batches is idempotent.
    resp = upload(client, "center-g2", [image, image])
    assert resp.status_code == 422
    assert "duplicates SOP UID" in resp.text


def test_batch_of_twenty_distinct_files_accepted(client, samples_dir, tmp_path):
    import generate_samples as gen

    paths = []
    for i in range(20):
        p = tmp_path / f"img_{i:02d}.dcm"
        gen.make_ct_image(
            p,
            study_uid=f"1.9.84.20261112.70.{i}",
            series_uid=f"1.9.84.20261112.71.{i}",
            sop_uid=f"1.9.84.20261112.72.{i}",
            pixel_value=100 + i,
        )
        paths.append(p)
    resp = upload(client, "center-g3", paths)
    assert resp.status_code == 201, resp.text
    assert resp.json()["file_count"] == 20


def test_empty_upload_rejected(client):
    resp = client.post(
        "/api/v1/batches", data={"center_id": "center-i"}
    )
    assert resp.status_code == 422


def test_non_explicit_vr_transfer_syntax_rejected(client, samples_dir, tmp_path):
    from pydicom.uid import ImplicitVRLittleEndian

    src = pydicom.dcmread(
        str(samples_dir / "reference_first" / "image_001.dcm")
    )
    src.file_meta.TransferSyntaxUID = ImplicitVRLittleEndian
    out = tmp_path / "implicit.dcm"
    src.save_as(str(out), enforce_file_format=True, implicit_vr=True)
    resp = upload(client, "center-h", [out])
    assert resp.status_code == 422
    assert "transfer syntax" in resp.text
