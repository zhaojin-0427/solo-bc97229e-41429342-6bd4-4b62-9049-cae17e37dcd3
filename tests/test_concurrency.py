"""Concurrency tests: parallel batches must not corrupt the mapping."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from app.uid import remap_uid

from conftest import upload


def test_parallel_distinct_batches_all_commit(client, samples_dir):
    image = samples_dir / "reference_first" / "image_001.dcm"
    struct = samples_dir / "reference_first" / "struct_001.dcm"

    def send(center: str):
        return upload(client, center, [image, struct])

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(send, [f"center-{i}" for i in range(8)]))

    assert all(r.status_code == 201 for r in responses), [r.text for r in responses]
    # Every center gets its own deterministic SOP mapping.
    for i, resp in enumerate(responses):
        body = resp.json()
        assert body["file_count"] == 2
        sops = {f["original_sop_uid"] for f in body["files"]}
        for original in sops:
            assert remap_uid(f"center-{i}", original) in {
                f["new_sop_uid"] for f in body["files"]
            }


def test_parallel_retransmit_of_same_bytes(client, samples_dir):
    image = samples_dir / "reference_first" / "image_001.dcm"
    results = []
    errors = []

    def send():
        try:
            results.append(upload(client, "same-center", [image]))
        except Exception as exc:  # pragma: no cover - failure diagnostic
            errors.append(exc)

    threads = [threading.Thread(target=send) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert all(r.status_code == 201 for r in results)
    # All concurrent retransmits converge on one stored instance.
    rows = client.get(
        "/api/v1/instances", params={"center_id": "same-center"}
    ).json()
    assert len(rows) == 1
