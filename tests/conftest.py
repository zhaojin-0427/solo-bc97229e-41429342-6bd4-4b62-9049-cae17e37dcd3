"""Shared pytest fixtures: isolated data dir, test client, synthetic files."""
from __future__ import annotations

import os
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
    yield target


@pytest.fixture()
def client(data_dir: Path):
    from fastapi.testclient import TestClient

    from app import main
    main.init_db()
    with TestClient(main.app) as test_client:
        yield test_client


@pytest.fixture(scope="session")
def samples_dir(tmp_path_factory) -> Path:
    """Generate the synthetic DICOM samples once per test session."""
    import generate_samples

    root = tmp_path_factory.mktemp("samples")
    generate_samples.generate(root)
    return root


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
