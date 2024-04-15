import logging
import os
import random
from datetime import timedelta
from pathlib import Path

import click

import db
import overcast
from db import EpisodeCollection, FeedCollection


def _xdg_cache_home() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"])
    else:
        return Path.home() / ".cache"


@click.command()
@click.option("--overcast-cookie", envvar="OVERCAST_COOKIE", required=True)
@click.option(
    "--feeds-path",
    type=click.Path(path_type=Path, dir_okay=False, writable=True),
    required=True,
)
@click.option(
    "--episodes-path",
    type=click.Path(path_type=Path, dir_okay=False, writable=True),
    required=True,
)
@click.option(
    "--cache-dir",
    default=_xdg_cache_home() / "overcast",
    show_default=True,
    type=Path,
)
@click.option("--offline", is_flag=True)
@click.option("--verbose", "-v", is_flag=True)
def main(
    overcast_cookie: str,
    feeds_path: Path,
    episodes_path: Path,
    cache_dir: Path,
    offline: bool,
    verbose: bool,
) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level)

    session = overcast.session(
        cache_dir=cache_dir,
        cookie=overcast_cookie,
        offline=offline,
    )

    db_feeds = FeedCollection.load(feeds_path)
    db_episodes = EpisodeCollection.load(episodes_path)

    db_feed = random.choice(list(db_feeds))

    for html_episode in overcast.fetch_podcast(
        session=session, feed_id=db_feed.id
    ).episodes:
        db_episode = db.Episode(
            id=html_episode.id,
            feed_id=db_feed.id,
            title=html_episode.title,
        )
        db_episodes.insert(db_episode)

    # export_data = export_account_data(session=session, extended=True)
    # html_feeds = fetch_podcasts(session=session)

    # db_feeds: list[db.Feed] = []

    # for html_feed, export_feed in zip_html_and_export_feeds(
    #     html_feeds=html_feeds, export_feeds=export_data.feeds
    # ):
    #     db_feed = db.Feed(
    #         numeric_id=export_feed.numeric_id,
    #         id=html_feed.id,
    #         title=db.Feed.clean_title(export_feed.title),
    #         added_at=export_feed.added_at,
    #     )
    #     db_feeds.append(db_feed)

    db_feeds.save(feeds_path)
    db_episodes.save(episodes_path)

    session.purge_cache(older_than=timedelta(days=90))


if __name__ == "__main__":
    main()
