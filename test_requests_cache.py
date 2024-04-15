import os
import tempfile
from datetime import timedelta
from pathlib import Path

import pytest

from requests_cache import Session, response_to_str, str_to_response


@pytest.fixture(scope="module")
def cache_dir() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"]) / "test_requests_cache"
    return Path(tempfile.mkdtemp())


def test_get_httpbin_delay(cache_dir: Path) -> None:
    session = Session(cache_dir=cache_dir, base_url="https://httpbin.org")
    for _i in range(60):
        r = session.get("/delay/1", cache_expires=timedelta(hours=1))
        assert r.status_code == 200
        assert r.headers["Content-Type"] == "application/json"
        assert r.json()["url"] == "https://httpbin.org/delay/1"


def test_response_str_roundtrip() -> None:
    response_str = "HTTP/1.1 200 OK\nContent-Type: text/plain\n\nHello, World!"

    response = str_to_response(response_str)
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == "Hello, World!"

    assert response_to_str(response) == response_str
