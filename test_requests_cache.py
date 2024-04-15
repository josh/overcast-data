import os
import tempfile
from datetime import timedelta
from pathlib import Path

import pytest

from requests_cache import Session, bytes_to_response, response_to_bytes


@pytest.fixture(scope="module")
def cache_dir() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"]) / "test_requests_cache"
    return Path(tempfile.mkdtemp())


def test_get_httpbin_delay(cache_dir: Path) -> None:
    session = Session(cache_dir=cache_dir, base_url="https://httpbin.org")
    for _i in range(60):
        r = session.get("/delay/1", cache_expires=timedelta(days=360))
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "application/json"
        assert r.json()["url"] == "https://httpbin.org/delay/1"


def test_response_bytes_roundtrip() -> None:
    response_bytes = b"HTTP/1.1 200 OK\nContent-Type: text/plain\n\nHello, World!"

    response = bytes_to_response(response_bytes)
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == "Hello, World!"

    assert response_to_bytes(response) == response_bytes


def test_cache_entries(cache_dir: Path) -> None:
    session = Session(cache_dir=cache_dir, base_url="https://httpbin.org")
    session.get("/get", cache_expires=timedelta(days=360))
    assert len(list(session.cache_entries())) >= 1


def test_purge_cache(cache_dir: Path) -> None:
    session = Session(cache_dir=cache_dir, base_url="https://httpbin.org")
    session.purge_cache(older_than=timedelta(days=90))
