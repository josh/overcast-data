import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.structures import CaseInsensitiveDict

logger = logging.getLogger("requests_cache")


class Session:
    _cache_dir: Path
    _base_url: str
    _session: requests.Session
    _min_time_between_requests: timedelta
    _last_request_at: datetime = datetime.min

    def __init__(
        self,
        cache_dir: Path,
        base_url: str,
        headers: dict[str, str] = {},
        min_time_between_requests: timedelta = timedelta(seconds=0),
    ):
        assert not base_url.endswith("/")
        self._cache_dir = cache_dir
        self._base_url = base_url
        self._session = requests.Session()
        self._session.headers.update(headers)
        self._min_time_between_requests = min_time_between_requests

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
            cached_response = str_to_response(filepath.read_text())
            cache_response_date = _response_date(cached_response)
            logger.debug("Found cache response date: %s", cache_response_date)

            if datetime.now() - cache_response_date < cache_expires:
                logger.debug("Cache valid")
                return cached_response
            else:
                logger.debug("Cache expired, ignoring")

        url = self._base_url + path
        self._throttle()
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
        with filepath.open("w") as f:
            f.write(response_to_str(r))

        return r

    def _throttle(self) -> None:
        seconds_to_wait = (
            self._last_request_at + self._min_time_between_requests - datetime.now()
        ).total_seconds()
        if seconds_to_wait > 0:
            logger.info("Waiting %s seconds...", seconds_to_wait)
            time.sleep(seconds_to_wait)
        self._last_request_at = datetime.now()


def response_to_str(response: requests.Response) -> str:
    text = f"HTTP/1.1 {response.status_code} {response.reason}\n"
    for k, v in response.headers.items():
        text += f"{k}: {v}\n"
    text += "\n"
    text += response.text
    return text


def str_to_response(text: str) -> requests.Response:
    lines = text.splitlines()
    _, status_code, reason = lines[0].split(" ", 2)
    headers: CaseInsensitiveDict[str] = CaseInsensitiveDict()
    body_index = lines.index("")
    for line in lines[1:body_index]:
        k, v = line.split(": ", 1)
        headers[k] = v
    body = "\n".join(lines[body_index + 1 :])

    response = requests.Response()
    response.status_code = int(status_code)
    response.reason = reason
    response.headers = headers
    response._content = body.encode()

    return response


def _response_date(response: requests.Response) -> datetime:
    return datetime.strptime(response.headers["Date"], "%a, %d %b %Y %H:%M:%S GMT")
