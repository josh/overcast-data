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
    default=_xdg_cache_home(),
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

    try:
        export_data = overcast.export_account_extended_data(session=ctx.session)

        # Reset following and use current value from export data
        for db_feed in ctx.db.feeds:
            db_feed.is_following = False

        for export_feed in export_data.feeds:
            ctx.db.feeds.insert(db.Feed.from_export_feed(export_feed))

            for export_episode in export_feed.episodes:
                ctx.db.episodes.insert(
                    db.Episode.from_export_episode(
                        export_episode, feed_id=export_feed.item_id
                    )
                )
    except overcast.RatedLimitedError:
        logger.error("Rate limited")
        return


@cli.command("refresh-feeds-index")
@click.pass_obj
def refresh_feeds_index(ctx: Context) -> None:
    logger.info("[refresh-feeds-index]")

    try:
        html_feeds = overcast.fetch_podcasts(session=ctx.session)

        # Reset is added and use current value from index
        for db_feed in ctx.db.feeds:
            db_feed.is_added = False

        for html_feed in html_feeds:
            ctx.db.feeds.insert(db.Feed.from_html_feed(html_feed))

        # Clear download flag on episodes for feeds that don't have any unplayed episodes
        for html_feed in html_feeds:
            if html_feed.has_unplayed_episodes is False:
                for db_episode in ctx.db.episodes:
                    if db_episode.feed_id == html_feed.item_id:
                        db_episode.is_downloaded = False

    except overcast.RatedLimitedError:
        logger.error("Rate limited")
        return


@cli.command("refresh-feeds")
@click.option("--limit", type=int, default=1, show_default=True)
@click.pass_obj
def refresh_feeds(ctx: Context, limit: int) -> None:
    logger.info("[refresh-feeds]")

    db_feeds_to_refresh = [f for f in ctx.db.feeds if f.is_added]
    shuffle(db_feeds_to_refresh)

    for db_feed in islice(db_feeds_to_refresh, limit):
        feed_id = db_feed.id
        feed_url = db_feed.overcast_url
        if not feed_url:
            logger.warning("Feed '%s' has no Overcast URL", db_feed.id)
            continue

        try:
            html_podcast = overcast.fetch_podcast(
                session=ctx.session, feed_url=feed_url
            )
            for html_episode in html_podcast.episodes:
                db_episode = db.Episode.from_html_podcast_episode(
                    html_episode,
                    feed_id=feed_id,
                )
                ctx.db.episodes.insert(db_episode)
        except overcast.RatedLimitedError:
            logger.error("Rate limited")
            continue


@cli.command("backfill-episode")
@click.option("--limit", type=int, default=1, show_default=True)
@click.pass_obj
def backfill_episode(ctx: Context, limit: int) -> None:
    logger.info("[backfill-episode] %s", limit)

    db_episodes_missing_info = [
        e for e in ctx.db.episodes if e.is_missing_optional_info
    ]
    if not db_episodes_missing_info:
        return
    shuffle(db_episodes_missing_info)
    logger.warning("Episodes missing optional info: %d", len(db_episodes_missing_info))

    for db_episode in islice(db_episodes_missing_info, limit):
        try:
            html_episode = overcast.fetch_episode(
                session=ctx.session,
                episode_url=db_episode.overcast_url,
            )
            new_db_episode = db.Episode.from_html_episode(html_episode)

            if db_episode.duration is None:
                new_db_episode.duration = overcast.fetch_audio_duration(
                    ctx.session, html_episode.audio_url
                )

            ctx.db.episodes.insert(new_db_episode)
        except overcast.RatedLimitedError:
            logger.error("Rate limited")
            continue


@cli.command("metrics")
@click.option("--metrics-filename", type=click.Path(path_type=Path))
@click.pass_obj
def metrics(ctx: Context, metrics_filename: str | None) -> None:
    registry = CollectorRegistry()

    episode_labelnames = ["feed_slug", "played", "downloaded"]

    overcast_episode_count = Gauge(
        "overcast_episode_count",
        "Count of Overcast episodes",
        labelnames=episode_labelnames,
        registry=registry,
    )
    overcast_episode_minutes = Gauge(
        "overcast_episode_minutes",
        "Minutes of Overcast episodes",
        labelnames=episode_labelnames,
        registry=registry,
    )

    logger.info("[metrics]")

    feed_slugs: dict[overcast.OvercastFeedItemID, str] = {}
    for db_feed in ctx.db.feeds:
        feed_slug = db_feed.slug()
        feed_slugs[db_feed.id] = feed_slug

        overcast_episode_count.labels(
            feed_slug=feed_slug, played="true", downloaded="true"
        ).set(0)
        overcast_episode_count.labels(
            feed_slug=feed_slug, played="true", downloaded="false"
        ).set(0)
        overcast_episode_count.labels(
            feed_slug=feed_slug, played="false", downloaded="true"
        ).set(0)
        overcast_episode_count.labels(
            feed_slug=feed_slug, played="false", downloaded="false"
        ).set(0)

    for db_episode in ctx.db.episodes:
        feed_slug = feed_slugs[db_episode.feed_id]
        played: str = "true" if db_episode.is_played is True else "false"
        downloaded: str = "true" if db_episode.is_downloaded is True else "false"
        overcast_episode_count.labels(
            feed_slug=feed_slug, played=played, downloaded=downloaded
        ).inc()
        if db_episode.duration:
            minutes = db_episode.duration.total_seconds() / 60
            overcast_episode_minutes.labels(
                feed_slug=feed_slug, played=played, downloaded=downloaded
            ).inc(minutes)

    for line in generate_latest(registry=registry).splitlines():
        logger.info(line.decode())

    if metrics_filename:
        logger.debug("Writing metrics to %s", metrics_filename)
        write_to_textfile(metrics_filename, registry)


@cli.command("purge-cache")
@click.pass_obj
def purge_cache(ctx: Context) -> None:
    logger.info("[purge-cache]")
    ctx.session.requests_session.purge_cache(older_than=timedelta(days=90))


if __name__ == "__main__":
    cli()
