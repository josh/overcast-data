import logging
import os
from dataclasses import dataclass
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


@dataclass
class Context:
    feeds_path: Path
    episodes_path: Path
    cache_dir: Path
    session: overcast.Session
    db_feeds: FeedCollection
    db_episodes: EpisodeCollection

    def save(self) -> None:
        self.db_feeds.save(self.feeds_path)
        self.db_episodes.save(self.episodes_path)


@click.group(chain=True)
@click.option("--overcast-cookie", envvar="OVERCAST_COOKIE", required=True)
@click.option("--offline", is_flag=True)
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
@click.option("--verbose", "-v", is_flag=True)
@click.pass_context
def cli(
    ctx: click.Context,
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

    ctx.obj = Context(
        feeds_path=feeds_path,
        episodes_path=episodes_path,
        cache_dir=cache_dir,
        session=session,
        db_feeds=db_feeds,
        db_episodes=db_episodes,
    )


@cli.command("refresh-opml-export")
@click.pass_obj
def refresh_opml_export(ctx: Context) -> None:
    logger.info("[refresh-opml-export]")
    export_data = overcast.export_account_extended_data(session=ctx.session)
    for export_feed in export_data.feeds:
        ctx.db_feeds.insert(db.Feed.from_export_feed(export_feed))
    ctx.save()


@cli.command("refresh-feeds-index")
@click.pass_obj
def refresh_feeds_index(ctx: Context) -> None:
    logger.info("[refresh-feeds-index]")
    db_feeds = ctx.db_feeds
    html_feeds = overcast.fetch_podcasts(session=ctx.session)
    for html_feed in html_feeds:
        db_feeds.insert(db.Feed.from_html_feed(html_feed))
    ctx.save()


@cli.command("refresh-feeds")
@click.option("--limit", type=int, default=1, show_default=True)
@click.pass_obj
def refresh_feeds(ctx: Context, limit: int) -> None:
    logger.info("[refresh-feeds]")

    db_feeds = ctx.db_feeds
    db_episodes = ctx.db_episodes

    db_feeds_to_refresh = list(db_feeds)
    shuffle(db_feeds_to_refresh)

    for db_feed in islice(db_feeds_to_refresh, limit):
        feed_url = db_feed.overcast_url
        if not feed_url:
            logger.warning("Feed '%s' has no Overcast URL", db_feed.id)
            break

        html_podcast = overcast.fetch_podcast(session=ctx.session, feed_url=feed_url)

        for html_episode in html_podcast.episodes:
            db_episode = db.Episode(
                overcast_url=html_episode.overcast_url,
                feed_url=feed_url,
                title=html_episode.title,
                duration=html_episode.duration,
            )
            db_episodes.insert(db_episode)

    ctx.save()


@cli.command("backfill-duration")
@click.option("--limit", type=int, default=1, show_default=True)
@click.pass_obj
def backfill_duration(ctx: Context, limit: int) -> None:
    logger.info("[backfill-duration]")

    db_episodes_missing_duration = [e for e in ctx.db_episodes if e.duration is None]
    if not db_episodes_missing_duration:
        return
    shuffle(db_episodes_missing_duration)
    logger.warning("Episodes missing duration: %d", len(db_episodes_missing_duration))

    export_data = overcast.export_account_extended_data(session=ctx.session)

    for db_episode_missing_duration in islice(db_episodes_missing_duration, limit):
        if enclosure_url := _enclosure_url_for_episode_url(
            session=ctx.session,
            export_feeds=export_data.feeds,
            episode_url=db_episode_missing_duration.overcast_url,
        ):
            duration = overcast.fetch_audio_duration(ctx.session, enclosure_url)
            db_episode_missing_duration.duration = duration

    ctx.save()


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


@cli.command("purge-cache")
@click.pass_obj
def purge_cache(ctx: Context) -> None:
    logger.info("[purge-cache]")
    ctx.session.purge_cache(older_than=timedelta(days=90))


if __name__ == "__main__":
    cli()
