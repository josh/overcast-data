import logging
import os
from pathlib import Path

import click

import database
import overcast
from database import save_feeds
from overcast import (
    export_account_data,
    fetch_podcasts,
    zip_html_and_export_feeds,
)


def _xdg_cache_home() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"])
    else:
        return Path.home() / ".cache"


@click.command()
@click.option("--overcast-cookie", envvar="OVERCAST_COOKIE", required=True)
@click.option(
    "--db-file",
    type=click.Path(path_type=Path, dir_okay=False, writable=True),
    required=True,
)
@click.option(
    "--cache-dir",
    default=_xdg_cache_home() / "overcast",
    show_default=True,
    type=Path,
)
@click.option("--verbose", "-v", is_flag=True)
def main(overcast_cookie: str, db_file: Path, cache_dir: Path, verbose: bool) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level)

    session = overcast.session(cache_dir=cache_dir, cookie=overcast_cookie)

    export_data = export_account_data(session=session, extended=True)
    html_feeds = fetch_podcasts(session=session)

    db_feeds: list[database.Feed] = []

    for html_feed, export_feed in zip_html_and_export_feeds(
        html_feeds=html_feeds, export_feeds=export_data.feeds
    ):
        db_feed = database.Feed(
            numeric_id=export_feed.numeric_id,
            id=html_feed.id,
            title=database.Feed.clean_title(export_feed.title),
            added_at=export_feed.added_at,
        )
        db_feeds.append(db_feed)

    save_feeds(filename=db_file, feeds=db_feeds)


if __name__ == "__main__":
    main()
