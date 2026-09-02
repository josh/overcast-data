from collections.abc import Callable
from pathlib import Path

from overcast_data.lru_cache import LRUCache

_1MB = 1024 * 1024


def _boom() -> str:
    raise AssertionError("expected a cache hit, but the loader ran")


def _loader(i: int) -> Callable[[], str]:
    return lambda: f"value{i}" * 10


def test_get_or_load_loads_once(tmp_path: Path) -> None:
    cache = LRUCache(filename=tmp_path / "cache.pickle", max_bytesize=_1MB)
    assert cache.get_or_load("a", lambda: "1") == "1"
    assert cache.get_or_load("a", _boom) == "1"


def test_persists_across_close(tmp_path: Path) -> None:
    filename = tmp_path / "nested" / "cache.pickle"

    cache = LRUCache(filename=filename, max_bytesize=_1MB)
    cache.get_or_load("a", lambda: "1")
    cache.close()

    assert filename.exists()
    reopened = LRUCache(filename=filename, max_bytesize=_1MB)
    assert reopened.get_or_load("a", _boom) == "1"


def test_close_evicts_oldest_until_under_max_bytesize(tmp_path: Path) -> None:
    filename = tmp_path / "cache.pickle"

    cache = LRUCache(filename=filename, max_bytesize=512)
    for i in range(50):
        cache.get_or_load(f"key{i}", _loader(i))
    cache.close()

    assert filename.stat().st_size <= 512

    reopened = LRUCache(filename=filename, max_bytesize=512)
    assert reopened.get_or_load("key49", _boom) == "value49" * 10
    assert reopened.get_or_load("key0", lambda: "reloaded") == "reloaded"


def test_reading_a_key_protects_it_from_eviction(tmp_path: Path) -> None:
    filename = tmp_path / "cache.pickle"

    cache = LRUCache(filename=filename, max_bytesize=512)
    for i in range(50):
        cache.get_or_load(f"key{i}", _loader(i))
    cache.get_or_load("key0", _boom)
    cache.close()

    reopened = LRUCache(filename=filename, max_bytesize=512)
    assert reopened.get_or_load("key0", _boom) == "value0" * 10
