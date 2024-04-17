import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

import requests
from requests.structures import CaseInsensitiveDict

from lru_cache import LRUCache

logger = logging.getLogger("requests_cache")


class OfflineError(Exception):
    pass


class Session:
    _cache_dir: Path
    _base_url: str
    _session: requests.Session
    _min_time_between_requests: timedelta
    _last_request_at: datetime = datetime.min
    _offline: bool

    simple_cache: LRUCache

    def __init__(
        self,
        cache_dir: Path,
        base_url: str,
        headers: dict[str, str] = {},
        min_time_between_requests: timedelta = timedelta(seconds=0),
        offline: bool = False,
    ):
        assert not base_url.endswith("/")
        self._cache_dir = cache_dir
        self._base_url = base_url
        self._session = requests.Session()
        self._session.headers.update(headers)
        self._min_time_between_requests = min_time_between_requests
        self._offline = offline
        self.simple_cache = LRUCache(
            path=cache_dir / "cache.pickle",
            max_bytesize=1024 * 1024,  # 1 MB
            save_on_exit=True,
        )

    def get(
        self,
        path: str,
        cache_expires: timedelta = timedelta(seconds=0),
        stale_cache_on_error: bool = True,
    ) -> requests.Response:
        assert path.startswith("/")

        urlsafe_path = path.lstrip("/").replace("?", "?_")
        filepath = Path(self._cache_dir, urlsafe_path)
        logger.debug(f"Request cache path: {filepath} exists: {filepath.exists()}")

        cached_response: requests.Response | None = None
        if filepath.exists():
            cached_response = bytes_to_response(filepath.read_bytes())
            cache_response_date = _response_date(cached_response)
            logger.debug("Found cache response date: %s", cache_response_date)

            if datetime.now() - cache_response_date < cache_expires:
                logger.debug("Cache valid")
                return cached_response
            else:
                logger.debug("Cache expired, ignoring")

        if self._offline is True:
            if cached_response:
                logger.warning("Offline mode, returning stale cache")
                return cached_response
            logger.error("Offline mode, no cache available")
            raise OfflineError()

        url = self._base_url + path
        self._throttle()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("GET %s", url)
        else:
            logger.info("GET %s", self._base_url)
        r = self._session.get(url)

        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            if stale_cache_on_error and cached_response:
                logger.warning("Request failed, returning stale cache")
                return cached_response

            raise e

        assert "Date" in r.headers, "Response must have a Date header"

        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open("wb") as f:
            f.write(response_to_bytes(r))

        return r

    def _throttle(self) -> None:
        seconds_to_wait = (
            self._last_request_at + self._min_time_between_requests - datetime.now()
        ).total_seconds()
        if seconds_to_wait > 0:
            logger.info("Waiting %s seconds...", seconds_to_wait)
            time.sleep(seconds_to_wait)
        self._last_request_at = datetime.now()

    def cache_entries(self) -> Iterator[tuple[Path, requests.Response]]:
        for path in self._cache_dir.rglob("*"):
            if not path.is_file():
                continue
            if path.name == "cache.pickle":
                continue
            try:
                response = bytes_to_response(path.read_bytes())
            except Exception as e:
                logger.error("Failed to read cache entry %s: %s", path, e)
                continue
            yield path, response

    def purge_cache(self, older_than: timedelta) -> None:
        now = datetime.now()
        for path, response in self.cache_entries():
            response_date = _response_date(response)
            if now - response_date > older_than:
                logger.info("Purging cache entry %s", path)
                path.unlink()

        for path in self._cache_dir.rglob("*"):
            if path.is_dir() and not any(path.iterdir()):
                logger.info("Removing empty cache directory %s", path)
                path.rmdir()


def response_to_bytes(response: requests.Response) -> bytes:
    """
    Serialize a requests.Response back to plain HTTP/1.1 over the wire data.
    $ curl -i http://example.com
    """
    headers = f"HTTP/1.1 {response.status_code} {response.reason}\n"
    for k, v in response.headers.items():
        headers += f"{k}: {v}\n"
    headers += "\n"
    return headers.encode("ascii") + response.content


def bytes_to_response(data: bytes) -> requests.Response:
    """
    Parse raw HTTP/1.1 response into a requests.Response object.
    """
    lines = data.splitlines()
    _, status_code, reason = lines[0].decode("ascii").split(" ", 2)
    headers: CaseInsensitiveDict[str] = CaseInsensitiveDict()
    body_index = lines.index(b"")
    for line in lines[1:body_index]:
        k, v = line.decode("ascii").split(": ", 1)
        headers[k] = v
    body = b"\n".join(lines[body_index + 1 :])

    response = requests.Response()
    response.status_code = int(status_code)
    response.reason = reason
    response.headers = headers
    response._content = body

    return response


def _response_date(response: requests.Response) -> datetime:
    return datetime.strptime(response.headers["Date"], "%a, %d %b %Y %H:%M:%S GMT")
