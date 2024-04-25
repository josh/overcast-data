import logging
import os
from pathlib import Path

import click

logger = logging.getLogger("cache_migration")


def _xdg_cache_home() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"])
    else:
        return Path.home() / ".cache"


@click.command()
def main() -> None:
    cache_dir = _xdg_cache_home()

    _rm(cache_dir / "test_requests_cache" / "cache.pickle")
    _mv(
        cache_dir / "test_requests_cache" / "get.json",
        cache_dir / "test_requests_cache" / "httpbin.org" / "get.json",
    )
    _mv(
        cache_dir / "test_requests_cache" / "delay",
        cache_dir / "test_requests_cache" / "httpbin.org" / "delay",
    )

    _mv(cache_dir / "overcast" / "cache.pickle", cache_dir / "overcast.pickle")
    for src_path in (cache_dir / "overcast").rglob("*"):
        _mv(
            src_path,
            cache_dir / "overcast.fm" / src_path.relative_to(cache_dir / "overcast"),
        )
    _rm(cache_dir / "overcast")

    _mv(cache_dir / "overcast.fm" / "cache.pickle", cache_dir / "overcast.pickle")

    _mv(
        cache_dir / "overcast.fm" / "test_fetch_podcasts_bad_cookie" / "podcasts.html",
        cache_dir / "test_fetch_podcasts_bad_cookie" / "overcast.fm" / "podcasts.html",
    )
    _rm(cache_dir / "overcast.fm" / "test_fetch_podcasts_bad_cookie" / "podcasts.html")
    _rm(cache_dir / "overcast.fm" / "test_fetch_podcasts_bad_cookie" / "cache.pickle")
    _rm(cache_dir / "overcast.fm" / "test_fetch_podcasts_bad_cookie")

    for path in (cache_dir / "overcast.fm").rglob("*"):
        if path.is_file() and path.suffix == "":
            _rm(path)

    for path in (cache_dir / "test_overcast").rglob("*"):
        if path.is_file():
            _rm(path)

    _rm(cache_dir / "overcast.fm" / "cache.pickle")
    _rm(cache_dir / "cache.pickle")
    _rm(cache_dir / "test_fetch_podcasts_bad_cookie" / "cache.pickle")
    _rm(cache_dir / "test_fetch_podcasts_bad_cookie" / "overcast.pickle")


def _rm(path: Path) -> None:
    if path.exists():
        logger.warning("rm %s", path)
        if path.is_file():
            path.unlink()
        else:
            try:
                path.rmdir()
            except OSError:
                logger.error("Failed to remove %s", path)


def _mv(src: Path, dst: Path) -> None:
    if src.exists() and not dst.exists():
        logger.warning("mv %s %s", src, dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.rename(dst)


if __name__ == "__main__":
    main()
