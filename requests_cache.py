import logging
import time
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.structures import CaseInsensitiveDict

logger = logging.getLogger("requests_cache")


class OfflineError(Exception):
    pass


_DEFAULT_MIME_TYPE_EXTNAMES = {
    "application/json": "json",
    "application/xml": "xml",
    "text/html": "html",
}


class Session:
    _cache_dir: Path
    _base_url: str
    _base_netloc: str
    _session: requests.Session
    _min_time_between_requests: timedelta
    _last_request_at: datetime = datetime.min
    _offline: bool

    mime_type_extnames: dict[str, str]

    def __init__(
        self,
        cache_dir: Path,
        base_url: str,
        headers: dict[str, str] = {},
        min_time_between_requests: timedelta = timedelta(seconds=0),
        offline: bool = False,
        mime_type_extnames: dict[str, str] = {},
    ):
        assert not base_url.endswith("/")
        self._base_url = base_url
        self._base_netloc = str(urlparse(base_url).netloc)
        self._cache_dir = cache_dir / self._base_netloc
        self._session = requests.Session()
        self._session.headers.update(headers)
        self._min_time_between_requests = min_time_between_requests
        self._offline = offline

        self.mime_type_extnames = _DEFAULT_MIME_TYPE_EXTNAMES.copy()
        self.mime_type_extnames.update(mime_type_extnames)

    def get_request(
        self, url: str, request_accept: str | None = None
    ) -> requests.Request:
        if not url.startswith(self._base_url):
            assert url.startswith("/"), f"Path must start with /: {url}"
            url = self._base_url + url

        headers: dict[str, str] = {}
        if request_accept:
            headers["Accept"] = request_accept
        request = requests.Request(method="GET", url=url, headers=headers)
        return request

    def get(
        self,
        path: str,
        request_accept: str | None = None,
        response_expires_in: timedelta = timedelta(seconds=0),
        stale_cache_on_error: bool = True,
    ) -> tuple[requests.Response, bool]:
        request = self.get_request(url=path, request_accept=request_accept)

        filepath = self.cache_path(request=request)
        logger.debug("Retrieving request cache: %s", filepath)

        cached_response: requests.Response | None = None
        if filepath.exists():
            cached_response = bytes_to_response(filepath.read_bytes())
            cache_response_date = response_date(cached_response)
            cache_expires = response_expires(cached_response)
            logger.debug(
                "Found cache response date: %s, expires: %s",
                cache_response_date,
                cache_expires,
            )

            if cache_expires > datetime.now():
                logger.debug("Cache valid")
                return cached_response, True
            else:
                logger.debug("Cache expired at %s", cache_expires)

        if self._offline is True:
            if cached_response:
                logger.warning("Offline mode, returning stale cache")
                return cached_response, True
            logger.error("Offline mode, no cache available")
            raise OfflineError()

        self._throttle()
        logger.warning("GET %s", request.url)
        prepped = self._session.prepare_request(request)
        r = self._session.send(prepped)

        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            if stale_cache_on_error and cached_response:
                logger.warning("Request failed, returning stale cache")
                return cached_response, True

            raise e

        response_expires_at = response_date(r) + response_expires_in
        logger.debug("Response will expire at %s", response_expires_at)
        r.headers["Expires"] = response_expires_at.strftime("%a, %d %b %Y %H:%M:%S GMT")

        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open("wb") as f:
            f.write(response_to_bytes(r))

        return r, False

    def _throttle(self) -> None:
        seconds_to_wait = (
            self._last_request_at + self._min_time_between_requests - datetime.now()
        ).total_seconds()
        if seconds_to_wait > 0:
            logger.warning("Waiting %s seconds...", seconds_to_wait)
            time.sleep(seconds_to_wait)
        self._last_request_at = datetime.now()

    def cache_path(self, request: requests.Request) -> Path:
        assert request.url.startswith(self._base_url), request.url
        prepped = self._session.prepare_request(request)
        url_components = urlparse(prepped.url)

        url_path: str = str(url_components.path).removeprefix("/")
        file_path = self._cache_dir / url_path

        if url_components.query:
            file_path = file_path.with_name(
                f"{file_path.name}?{str(url_components.query)}"
            )

        if accept := prepped.headers.get("Accept"):
            if extname := self.mime_type_extnames.get(accept):
                file_path = file_path.with_suffix(f".{extname}")
            elif accept != "*/*":
                logger.warning("No extname for Accept: %s", accept)

        return file_path

    def cached_response(self, request: requests.Request) -> requests.Response | None:
        path = self.cache_path(request)
        if not path.exists():
            return None
        return bytes_to_response(path.read_bytes())

    def is_cache_fresh(self, request: requests.Request) -> bool:
        if response := self.cached_response(request):
            expires = response_expires(response)
            return datetime.now() < expires
        return False

    def cache_entries(self) -> Iterator[tuple[Path, requests.Response]]:
        for path in self._cache_dir.rglob("*"):
            if not path.is_file():
                continue
            try:
                response = bytes_to_response(path.read_bytes())
            except Exception as e:
                logger.error("Failed to read cache entry %s: %s", path, e)
                continue
            yield path, response

    def purge_cache(self, older_than: timedelta = timedelta.max) -> None:
        now = datetime.now()
        oldest_date = now + older_than

        for path, response in self.cache_entries():
            date = response_date(response)
            expires = response_expires(response)
            if expires < now:
                logger.debug("Purging expired cache entry %s", path)
                path.unlink()
            elif date > oldest_date:
                logger.debug("Purging old cache entry %s", path)
                path.unlink()

        for path in self._cache_dir.rglob("*"):
            if path.is_dir() and not any(path.iterdir()):
                logger.debug("Removing empty cache directory %s", path)
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


def response_date(response: requests.Response) -> datetime:
    return datetime.strptime(response.headers["Date"], "%a, %d %b %Y %H:%M:%S GMT")


def response_expires(response: requests.Response) -> datetime:
    if "Expires" not in response.headers:
        return datetime.min
    return datetime.strptime(response.headers["Expires"], "%a, %d %b %Y %H:%M:%S GMT")
