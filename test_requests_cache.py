import os
from datetime import timedelta
from pathlib import Path

import pytest

from requests_cache import Session


@pytest.fixture
def cache_dir(tmpdir: Path) -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"]) / "test_requests_cache"
    return tmpdir


def test_get_httpbin_delay(cache_dir: Path) -> None:
    session = Session(cache_dir=cache_dir, base_url="https://httpbin.org")
    for _i in range(60):
        r = session.get("/delay/1", cache_expires=timedelta(hours=1))
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "application/json"
        assert r.json()["url"] == "https://httpbin.org/delay/1"
