import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Make the project root importable when running pytest from the repo root.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _reset_app_state() -> None:
    from app.main import registry, store

    registry.reset()
    store.reset()


@pytest.fixture()
def client():
    from app.main import app

    _reset_app_state()
    with TestClient(app) as c:
        yield c
    _reset_app_state()


@pytest.fixture()
def sample_path() -> Path:
    return ROOT / "sample_data" / "observations.csv"


@pytest.fixture()
def other_sample_path(tmp_path: Path) -> Path:
    """A different upload batch for ST001 (4 rows instead of 5)."""
    path = tmp_path / "observations_v2.csv"
    path.write_text(
        "站号,UTC时刻,温度\n"
        "ST001,2026-11-02T00:00:00Z,10.0\n"
        "ST001,2026-11-02T01:00:00Z,11.0\n"
        "ST001,2026-11-02T02:00:00Z,12.0\n"
        "ST001,2026-11-02T03:00:00Z,13.0\n"
        "ST002,2026-11-02T00:00:00Z,30.0\n",
        encoding="utf-8",
    )
    return path
