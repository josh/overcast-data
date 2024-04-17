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

    _refresh_random_feed(session=session, db_feeds=db_feeds, db_episodes=db_episodes)
    _refresh_missing_episode_duration(session=session, db_episodes=db_episodes)

    db_feeds.save(feeds_path)
    db_episodes.save(episodes_path)

    session.purge_cache(older_than=timedelta(days=90))


def _refresh_random_feed(
    session: overcast.Session,
    db_feeds: FeedCollection,
    db_episodes: EpisodeCollection,
) -> None:
    db_feed = random.choice(list(db_feeds))

    html_podcast = overcast.fetch_podcast(session=session, feed_id=db_feed.id)

    for html_episode in html_podcast.episodes:
        db_episode = db.Episode(
            id=html_episode.id,
            feed_id=db_feed.id,
            title=html_episode.title,
            duration=html_episode.duration,
        )
        db_episodes.insert(db_episode)


def _refresh_missing_episode_duration(
    session: overcast.Session,
    db_episodes: EpisodeCollection,
) -> None:
    db_episodes_missing_duration = [e for e in db_episodes if e.duration is None]
    if not db_episodes_missing_duration:
        return
    db_episode_missing_duration = random.choice(db_episodes_missing_duration)
    html_episode = overcast.fetch_episode(session, db_episode_missing_duration.id)
    duration = overcast.fetch_audio_duration(session, html_episode.audio_url)
    db_episode_missing_duration.duration = duration


if __name__ == "__main__":
    main()
