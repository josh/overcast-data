import logging
import os
import random
from datetime import timedelta
from itertools import islice
from pathlib import Path
from random import shuffle

import click

import db
import overcast
from db import EpisodeCollection, FeedCollection
from utils import HTTPURL

logger = logging.getLogger("overcast-data")


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

    html_feeds = overcast.fetch_podcasts(session=session)
    for html_feed in html_feeds:
        db_feeds.insert(db.Feed.from_html_feed(html_feed))

    export_data = overcast.export_account_extended_data(session=session)
    for export_feed in export_data.feeds:
        db_feeds.insert(db.Feed.from_export_feed(export_feed))

    _refresh_random_feed(session=session, db_feeds=db_feeds, db_episodes=db_episodes)

    _refresh_missing_episodes_duration(
        session=session,
        db_episodes=db_episodes,
        export_feeds=export_data.feeds,
        times=5,
    )

    db_feeds.save(feeds_path)
    db_episodes.save(episodes_path)

    session.purge_cache(older_than=timedelta(days=90))


def _refresh_random_feed(
    session: overcast.Session,
    db_feeds: FeedCollection,
    db_episodes: EpisodeCollection,
) -> None:
    db_feed = random.choice(list(db_feeds))

    feed_url = db_feed.overcast_url
    if not feed_url:
        logger.warning("Feed '%s' has no Overcast URL", db_feed.id)
        return

    html_podcast = overcast.fetch_podcast(session=session, feed_url=feed_url)

    for html_episode in html_podcast.episodes:
        db_episode = db.Episode(
            overcast_url=html_episode.overcast_url,
            feed_url=feed_url,
            title=html_episode.title,
            duration=html_episode.duration,
        )
        db_episodes.insert(db_episode)


def _refresh_missing_episodes_duration(
    session: overcast.Session,
    db_episodes: EpisodeCollection,
    export_feeds: list[overcast.ExtendedExportFeed],
    times: int,
) -> None:
    db_episodes_missing_duration = [e for e in db_episodes if e.duration is None]
    logger.info("Episodes missing duration: %d", len(db_episodes_missing_duration))
    if not db_episodes_missing_duration:
        return

    shuffle(db_episodes_missing_duration)

    for db_episode_missing_duration in islice(db_episodes_missing_duration, times):
        if enclosure_url := _enclosure_url_for_episode_url(
            session=session,
            export_feeds=export_feeds,
            episode_url=db_episode_missing_duration.overcast_url,
        ):
            duration = overcast.fetch_audio_duration(session, enclosure_url)
            db_episode_missing_duration.duration = duration


def _enclosure_url_for_episode_url(
    session: overcast.Session,
    export_feeds: list[overcast.ExtendedExportFeed],
    episode_url: overcast.OvercastEpisodeURL,
) -> HTTPURL | None:
    for export_feed in export_feeds:
        for export_episode in export_feed.episodes:
            if export_episode.overcast_url == episode_url:
                return export_episode.enclosure_url

    if episode := overcast.fetch_episode(session=session, episode_url=episode_url):
        return episode.audio_url

    return None


if __name__ == "__main__":
    main()
