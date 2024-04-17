import pytest

from lru_cache import LRUCache


@pytest.fixture(scope="function")
def cache() -> LRUCache:
    return LRUCache(max_bytesize=1024)


def test_item_get_set(cache: LRUCache) -> None:
    assert cache["key"] is None

    cache["key"] = 1
    assert cache["key"] == 1
    assert cache["key"] == 1

    cache["key"] = 2
    assert cache["key"] == 2


def test_contains(cache: LRUCache) -> None:
    assert "key" not in cache
    cache["key"] = 1
    assert "key" in cache


def test_len(cache: LRUCache) -> None:
    assert len(cache) == 0
    cache["key"] = 1
    assert len(cache) == 1


def test_get_or_load(cache: LRUCache) -> None:
    def load_value() -> int:
        return 42

    assert len(cache) == 0
    assert cache.get("key", load_value) == 42
    assert len(cache) == 1
    assert cache.get("key", load_value) == 42
    assert len(cache) == 1


def test_trim(cache: LRUCache) -> None:
    for i in range(300):
        cache[i] = i
    assert cache.bytesize() > 1024
    cache.trim()
    assert cache.bytesize() <= 1024
