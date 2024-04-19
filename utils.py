import logging
import sys
from urllib.parse import urlparse

_RAISE_VALIDATION_ERRORS = "pytest" in sys.modules

logger = logging.getLogger("overcast")


class URL(str):
    def __new__(cls, urlstring: str) -> "URL":
        try:
            components = urlparse(urlstring)
            if not components.scheme:
                raise ValueError(f"Invalid URL: {urlstring}")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)


class HTTPURL(URL):
    def __new__(cls, urlstring: str) -> "HTTPURL":
        try:
            components = urlparse(urlstring)
            if components.scheme not in ["http", "https"]:
                raise ValueError(f"Invalid HTTP URL: {urlstring}")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)
