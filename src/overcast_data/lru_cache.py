import logging
import pickle
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

logger = logging.getLogger("lru_cache")

T = TypeVar("T")


class LRUCache:
    """A dict persisted as a pickle.

    Oldest entries are evicted on close until the pickle fits in max_bytesize.
    """

    _filename: Path
    _max_bytesize: int
    _data: OrderedDict[str, Any]

    def __init__(self, filename: Path, max_bytesize: int) -> None:
        self._filename = filename
        self._max_bytesize = max_bytesize
        self._data = OrderedDict()
        if filename.exists():
            self._data.update(pickle.loads(filename.read_bytes()))
            logger.debug("Loaded cache: %s (%i items)", filename, len(self._data))

    def get_or_load(self, key: str, load_value: Callable[[], T]) -> T:
        if key in self._data:
            self._data.move_to_end(key)
            cached: T = self._data[key]
            return cached
        self._data[key] = value = load_value()
        return value

    def close(self) -> None:
        data = pickle.dumps(self._data, pickle.HIGHEST_PROTOCOL)
        evicted = 0
        while len(data) > self._max_bytesize and self._data:
            self._data.popitem(last=False)
            evicted += 1
            data = pickle.dumps(self._data, pickle.HIGHEST_PROTOCOL)
        if evicted:
            logger.warning("Trimmed %i cache items", evicted)
        self._filename.parent.mkdir(parents=True, exist_ok=True)
        self._filename.write_bytes(data)
        logger.debug("Saved cache: %s (%i items)", self._filename, len(self._data))
