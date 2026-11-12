"""Shared pytest fixtures: isolated data dir, test client, synthetic files."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.fixture()
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    target = (tmp_path / "data").resolve()
    # Keep the env var and every module-level path binding in sync. All
    # modules read these attributes at call time.
    monkeypatch.setenv("DICOM_INGEST_DATA_DIR", str(target))
    from app import config, db
    storage = target / "objects"
    monkeypatch.setattr(config, "DATA_DIR", target)
    monkeypatch.setattr(config, "DB_PATH", target / "ingest.db")
    monkeypatch.setattr(config, "STORAGE_DIR", storage)
    monkeypatch.setattr(db, "DB_PATH", target / "ingest.db")
    # Fast worker polling keeps the tests snappy without sleeping on retries.
    monkeypatch.setattr(config, "BACKFILL_POLL_INTERVAL_S", 0.02)
    yield target


@pytest.fixture()
def client(data_dir: Path):
    from fastapi.testclient import TestClient

    from app import backfill, main
    main.init_db()
    with TestClient(main.app) as test_client:
        # The worker is a module-level singleton; restart it inside this
        # TestClient's event loop so every test gets a healthy worker and
        # monkeypatched HTTP factories apply to its per-job clients.
        test_client.portal.call(backfill.worker.stop)
        test_client.portal.call(backfill.worker.start)
        try:
            yield test_client
        finally:
            test_client.portal.call(backfill.worker.stop)


@pytest.fixture(scope="session")
def samples_dir(tmp_path_factory) -> Path:
    """Generate the synthetic DICOM samples once per test session."""
    import generate_samples

    root = tmp_path_factory.mktemp("samples")
    generate_samples.generate(root)
    return root


@pytest.fixture()
def source_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A private source-service data directory with the multi-page study."""
    import generate_samples
    from source_app import config as source_config

    root = (tmp_path / "source-data").resolve()
    generate_samples.generate_source_data(root)
    monkeypatch.setattr(source_config, "DATA_DIR", root)
    monkeypatch.setattr(source_config, "STUDIES_DIR", root / "studies")
    monkeypatch.setattr(source_config, "FAULTS_PATH", root / "faults.json")
    yield root


@pytest.fixture()
def source_client(source_data_dir: Path):
    from fastapi.testclient import TestClient

    from source_app import main as source_main
    source_main.registry.load()
    # Start without a lifespan-managed registry reload (already loaded above).
    with TestClient(source_main.app) as test_client:
        yield test_client


def upload(client, center: str, paths: list[Path]):
    files = [
        ("files", (p.name, p.read_bytes(), "application/dicom"))
        for p in paths
    ]
    return client.post(
        "/api/v1/batches",
        data={"center_id": center},
        files=files,
    )


def wait_for_status(client, job_id: str, expected: set[str], timeout: float = 20.0):
    """Poll a backfill job until it reaches a terminal/expected status."""
    import time

    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.get(f"/api/v1/backfills/{job_id}").json()
        if last["status"] in expected:
            return last
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not reach {expected}; last={last}")

