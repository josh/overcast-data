import logging
import os
from contextlib import AbstractContextManager
from datetime import timedelta
from itertools import islice
from pathlib import Path
from random import shuffle
from types import TracebackType

import click
from prometheus_client import (
    CollectorRegistry,
    Gauge,
    generate_latest,
    write_to_textfile,
)

import db
import overcast
from db import Database
from utils import HTTPURL

logger = logging.getLogger("overcast-data")


def _xdg_cache_home() -> Path:
    if "XDG_CACHE_HOME" in os.environ:
        return Path(os.environ["XDG_CACHE_HOME"])
    else:
        return Path.home() / ".cache"


class Context(AbstractContextManager["Context"]):
    session: overcast.Session
    db: Database

    def __init__(self, session: overcast.Session, db_path: Path) -> None:
        self.session = session
        self.db = Database(path=db_path)

    def __enter__(self) -> "Context":
        logger.debug("Entering cli context")
        self.db.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        logger.debug("Exiting cli context")
        self.db.__exit__(exc_type, exc_value, traceback)


@click.group(chain=True)
@click.option("--overcast-cookie", envvar="OVERCAST_COOKIE", required=True)
@click.option("--offline", is_flag=True)
@click.option(
    "--db-path",
    type=click.Path(path_type=Path, dir_okay=True, file_okay=False, writable=True),
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
    db_path: Path,
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

    context = Context(
        session=session,
        db_path=db_path,
    )
    ctx.obj = ctx.with_resource(context)


@cli.command("refresh-opml-export")
@click.pass_obj
def refresh_opml_export(ctx: Context) -> None:
    logger.info("[refresh-opml-export]")
    export_data = overcast.export_account_extended_data(session=ctx.session)
    for export_feed in export_data.feeds:
        ctx.db.feeds.insert(db.Feed.from_export_feed(export_feed))

        feed_url = _feed_url_for_feed_id(
            feeds=ctx.db.feeds,
            feed_id=export_feed.item_id,
        )
        if not feed_url:
            logger.warning("Feed '%s' has no Overcast URL", export_feed.item_id)
            continue

        for export_episode in export_feed.episodes:
            ctx.db.episodes.insert(
                db.Episode.from_export_episode(export_episode, export_feed, feed_url)
            )


# TMP
def _feed_url_for_feed_id(
    feeds: db.FeedCollection,
    feed_id: overcast.OvercastFeedItemID,
) -> overcast.OvercastFeedURL | None:
    for db_feed in feeds:
        if db_feed.id == feed_id:
            return db_feed.overcast_url
    return None


@cli.command("refresh-feeds-index")
@click.pass_obj
def refresh_feeds_index(ctx: Context) -> None:
    logger.info("[refresh-feeds-index]")
    db_feeds = ctx.db.feeds

    for db_feed in db_feeds:
        db_feed.is_subscribed = False

    html_feeds = overcast.fetch_podcasts(session=ctx.session)
    for html_feed in html_feeds:
        db_feeds.insert(db.Feed.from_html_feed(html_feed))


@cli.command("refresh-feeds")
@click.option("--limit", type=int, default=1, show_default=True)
@click.pass_obj
def refresh_feeds(ctx: Context, limit: int) -> None:
    logger.info("[refresh-feeds]")

    db_feeds_to_refresh = [f for f in ctx.db.feeds if f.is_subscribed]
    shuffle(db_feeds_to_refresh)

    for db_feed in islice(db_feeds_to_refresh, limit):
        feed_id = db_feed.id
        feed_url = db_feed.overcast_url
        if not feed_url:
            logger.warning("Feed '%s' has no Overcast URL", db_feed.id)
            break

        html_podcast = overcast.fetch_podcast(session=ctx.session, feed_url=feed_url)

        for html_episode in html_podcast.episodes:
            db_episode = db.Episode.from_html_episode(
                html_episode,
                feed_url=feed_url,
                feed_id=feed_id,
            )
            ctx.db.episodes.insert(db_episode)


@cli.command("backfill-duration")
@click.option("--limit", type=int, default=1, show_default=True)
@click.pass_obj
def backfill_duration(ctx: Context, limit: int) -> None:
    logger.info("[backfill-duration]")

    db_episodes_missing_duration = [e for e in ctx.db.episodes if e.duration is None]
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


@cli.command("metrics")
@click.option("--metrics-filename", type=click.Path(path_type=Path))
@click.pass_obj
def metrics(ctx: Context, metrics_filename: str | None) -> None:
    registry = CollectorRegistry()

    overcast_episode_count = Gauge(
        "overcast_episode_count",
        "Count of Overcast episodes",
        labelnames=["feed_slug"],
        registry=registry,
    )
    overcast_episode_minutes = Gauge(
        "overcast_episode_minutes",
        "Minutes of Overcast episodes",
        labelnames=["feed_slug"],
        registry=registry,
    )

    logger.info("[metrics]")

    feed_slugs: dict[overcast.OvercastFeedURL, str] = {}
    for db_feed in ctx.db.feeds:
        if db_feed.overcast_url:
            feed_slugs[db_feed.overcast_url] = db_feed.slug()
        else:
            logger.warning("Feed '%s' has no Overcast URL", db_feed.id)

    for db_episode in ctx.db.episodes:
        feed_slug = feed_slugs[db_episode.feed_url]
        overcast_episode_count.labels(feed_slug=feed_slug).inc()
        if db_episode.duration:
            minutes = db_episode.duration.total_seconds() / 60
            overcast_episode_minutes.labels(feed_slug=feed_slug).inc(minutes)

    for line in generate_latest(registry=registry).splitlines():
        logger.info(line.decode())

    if metrics_filename:
        logger.debug("Writing metrics to %s", metrics_filename)
        write_to_textfile(metrics_filename, registry)


@cli.command("purge-cache")
@click.pass_obj
def purge_cache(ctx: Context) -> None:
    logger.info("[purge-cache]")
    ctx.session.purge_cache(older_than=timedelta(days=90))


if __name__ == "__main__":
    cli()
