import os
from datetime import timedelta
from pathlib import Path

import pytest
import requests

from overcast_data.requests_cache import Session, bytes_to_response, response_to_bytes

_OFFLINE = "PYTEST_OFFLINE" in os.environ


@pytest.fixture(scope="module")
def module_cache_dir(request: pytest.FixtureRequest) -> Path:
    cache_home = os.environ.get("XDG_CACHE_HOME") or "/tmp/pytest"
    return Path(cache_home) / str(request.module.__name__)


@pytest.fixture(scope="function")
def function_cache_dir(request: pytest.FixtureRequest, module_cache_dir: Path) -> Path:
    return module_cache_dir / str(request.node.name)


@pytest.fixture(scope="module")
def session(module_cache_dir: Path) -> Session:
    return Session(
        cache_dir=module_cache_dir,
        base_url="https://httpbin.org",
        offline=_OFFLINE,
    )


def test_get_httpbin_delay(session: Session) -> None:
    for i in range(60):
        res, from_cache = session.get(
            "/delay/1",
            request_accept="application/json",
            response_expires_in=timedelta(days=30),
        )

        assert res.status_code == 200
        assert res.headers["Content-Type"] == "application/json"
        assert res.json()["url"] == "https://httpbin.org/delay/1"

        if i > 0:
            assert from_cache is True


def test_get_httpbin_inject_expires_response_header(session: Session) -> None:
    res, _from_cache = session.get(
        "/get",
        request_accept="application/json",
        response_expires_in=timedelta(days=30),
    )

    assert res.status_code == 200
    assert res.headers["Expires"]

    request = requests.Request(
        "GET",
        "https://httpbin.org/get",
        headers={"Accept": "application/json"},
    )
    assert session.is_cache_fresh(request)


def test_response_bytes_roundtrip() -> None:
    response_bytes = b"HTTP/1.1 200 OK\nContent-Type: text/plain\n\nHello, World!"

    response = bytes_to_response(response_bytes)
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == "Hello, World!"

    assert response_to_bytes(response) == response_bytes


def test_cache_entries(session: Session) -> None:
    session.get(
        "/get",
        request_accept="application/json",
        response_expires_in=timedelta(days=30),
    )
    assert len(list(session.cache_entries())) >= 1


def test_purge_cache(session: Session) -> None:
    session.purge_cache(older_than=timedelta(days=30))


def test_cache_path(module_cache_dir: Path, session: Session) -> None:
    request = requests.Request("GET", "https://httpbin.org/get")
    path = session.cache_path(request=request)
    assert path == module_cache_dir / "httpbin.org" / "get"

    request = requests.Request(
        "GET",
        "https://httpbin.org/get",
        headers={"Accept": "application/json"},
    )
    path = session.cache_path(request=request)
    assert path == module_cache_dir / "httpbin.org" / "get.json"

    request = requests.Request(
        "GET",
        "https://httpbin.org/get",
        params={"foo": "bar"},
        headers={"Accept": "application/json"},
    )
    path = session.cache_path(request=request)
    assert path == module_cache_dir / "httpbin.org" / "get?foo=bar.json"

    request = requests.Request(
        "GET",
        "https://httpbin.org/status/200",
        headers={"Accept": "application/json"},
    )
    path = session.cache_path(request=request)
    assert path == module_cache_dir / "httpbin.org" / "status/200.json"
