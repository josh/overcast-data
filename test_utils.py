import pytest

from utils import HTTPURL, URL


def _is_str(s: str) -> None:
    assert s.removeprefix("foo")


def _is_url(url: URL) -> None:
    assert url.removeprefix("foo")


def _is_http_url(url: HTTPURL) -> None:
    assert url.removeprefix("foo")


def test_url() -> None:
    url = URL("http://www.example.com")
    assert url == "http://www.example.com"
    _is_str(url)
    _is_url(url)

    with pytest.raises(ValueError):
        URL("example.com")


def test_http_url() -> None:
    url = HTTPURL("http://www.example.com")
    assert url == "http://www.example.com"
    _is_str(url)
    _is_url(url)
    _is_http_url(url)

    url = HTTPURL("https://www.example.com")
    assert url == "https://www.example.com"
    _is_str(url)
    _is_url(url)
    _is_http_url(url)

    with pytest.raises(ValueError):
        HTTPURL("example.com")
    with pytest.raises(ValueError):
        HTTPURL("ftp://www.example.com")
