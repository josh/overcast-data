import os
from datetime import timedelta
from pathlib import Path

import pytest

from requests_cache import Session, bytes_to_response, response_to_bytes

_OFFLINE = "PYTEST_OFFLINE" in os.environ


@pytest.fixture(scope="module")
def module_cache_dir(request: pytest.FixtureRequest) -> Path:
    cache_home = os.environ.get("XDG_CACHE_HOME") or "/tmp/pytest"
    return Path(cache_home) / str(request.module.__name__)


@pytest.fixture(scope="function")
def function_cache_dir(request: pytest.FixtureRequest, module_cache_dir: Path) -> Path:
    return module_cache_dir / str(request.node.name)


def test_get_httpbin_delay(module_cache_dir: Path) -> None:
    session = Session(
        cache_dir=module_cache_dir,
        base_url="https://httpbin.org",
        offline=_OFFLINE,
    )
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


def test_cache_entries(module_cache_dir: Path) -> None:
    session = Session(
        cache_dir=module_cache_dir,
        base_url="https://httpbin.org",
        offline=_OFFLINE,
    )
    session.get("/get", cache_expires=timedelta(days=360))
    assert len(list(session.cache_entries())) >= 1


def test_purge_cache(module_cache_dir: Path) -> None:
    session = Session(
        cache_dir=module_cache_dir,
        base_url="https://httpbin.org",
        offline=_OFFLINE,
    )
    session.purge_cache(older_than=timedelta(days=90))
