import pytest

from overcast_data.utils import (
    HTTPURL,
    URL,
    Ciphertext,
    EncryptionKey,
    decrypt,
    encrypt,
    generate_encryption_key,
)


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


def test_generate_encryption_key() -> None:
    assert len(generate_encryption_key()) == 64


_KEY = EncryptionKey("O9NDmG2cSd4sI3REWjp17M7gboscMKBHD9qFLyrMUk41KVzyuKd/3/PtNs9VUb++")


def test_encrypt() -> None:
    assert encrypt(_KEY, "Hello, World!") == Ciphertext("pXpHLUxmGI0TtR+GPE43sg==")


def test_decrypt() -> None:
    assert decrypt(_KEY, Ciphertext("pXpHLUxmGI0TtR+GPE43sg==")) == "Hello, World!"
