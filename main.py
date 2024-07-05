import logging
import os
import random
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from functools import partial
from itertools import islice
from pathlib import Path
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
@click.option("--encryption-key", envvar="ENCRYPTION_KEY", required=True)
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
    encryption_key: str,
    db_path: Path,
    cache_dir: Path,
    offline: bool,
    verbose: bool,
) -> None:
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level)

    os.environ["ENCRYPTION_KEY"] = encryption_key

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

        def on_feed_insert(
            feed_id: overcast.OvercastFeedItemID,
            export_feed: overcast.ExtendedExportFeed,
        ) -> db.Feed:
            assert feed_id == export_feed.item_id
            return db.Feed(
                id=feed_id,
                overcast_url=None,
                title=export_feed.title,
                html_url=export_feed.html_url,
                added_at=export_feed.added_at,
                is_added=True,
                is_following=export_feed.is_subscribed,
            )

        def on_feed_update(
            db_feed: db.Feed,
            export_feed: overcast.ExtendedExportFeed,
        ) -> db.Feed:
            assert db_feed.id == export_feed.item_id
            db_feed.html_url = export_feed.html_url
            db_feed.added_at = export_feed.added_at
            db_feed.is_added = True
            db_feed.is_following = export_feed.is_subscribed
            return db_feed

        def on_episode_insert(
            episode_url: overcast.OvercastEpisodeURL,
            export_feed: overcast.ExtendedExportFeed,
            export_episode: overcast.ExtendedExportEpisode,
        ) -> db.Episode:
            assert episode_url == export_episode.overcast_url
            duration = overcast.fetch_audio_duration(
                ctx.session, export_episode.enclosure_url
            )
            return db.Episode(
                id=export_episode.item_id,
                overcast_url=episode_url,
                feed_id=export_feed.item_id,
                title=export_episode.title,
                enclosure_url=export_episode.enclosure_url,
                duration=duration,
                date_published=export_episode.date_published,
                is_played=export_episode.is_played,
                is_downloaded=not export_episode.is_deleted,
                did_download=True,
            )

        def on_episode_update(
            db_episode: db.Episode,
            export_feed: overcast.ExtendedExportFeed,
            export_episode: overcast.ExtendedExportEpisode,
        ) -> db.Episode:
            assert db_episode.overcast_url == export_episode.overcast_url
            db_episode.id = export_episode.item_id
            db_episode.feed_id = export_feed.item_id
            db_episode.date_published = export_episode.date_published
            db_episode.enclosure_url = export_episode.enclosure_url
            db_episode.did_download = True

            if db_episode.is_played is None:
                db_episode.is_played = export_episode.is_played

            return db_episode

        for export_feed in export_data.feeds:
            ctx.db.feeds.insert_or_update(
                feed_id=export_feed.item_id,
                on_insert=partial(on_feed_insert, export_feed=export_feed),
                on_update=partial(on_feed_update, export_feed=export_feed),
            )

            for export_episode in export_feed.episodes:
                ctx.db.episodes.insert_or_update(
                    episode_url=export_episode.overcast_url,
                    on_insert=partial(
                        on_episode_insert,
                        export_feed=export_feed,
                        export_episode=export_episode,
                    ),
                    on_update=partial(
                        on_episode_update,
                        export_feed=export_feed,
                        export_episode=export_episode,
                    ),
                )

        # If still missing downloaded state, fill with False
        for db_episode in ctx.db.episodes:
            if db_episode.is_played is None:
                db_episode.is_played = False

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

        def on_feed_insert(
            feed_id: overcast.OvercastFeedItemID,
            html_feed: overcast.HTMLPodcastsFeed,
        ) -> db.Feed:
            assert feed_id == html_feed.item_id
            return db.Feed(
                id=feed_id,
                overcast_url=html_feed.overcast_url,
                title=html_feed.title,
                html_url=None,
                added_at=None,
                is_added=True,
                is_following=None,
            )

        def on_feed_update(
            db_feed: db.Feed,
            html_feed: overcast.HTMLPodcastsFeed,
        ) -> db.Feed:
            assert db_feed.id == html_feed.item_id
            db_feed.title = html_feed.title
            db_feed.overcast_url = html_feed.overcast_url
            db_feed.is_added = True
            return db_feed

        for html_feed in html_feeds:
            ctx.db.feeds.insert_or_update(
                feed_id=html_feed.item_id,
                on_insert=partial(on_feed_insert, html_feed=html_feed),
                on_update=partial(on_feed_update, html_feed=html_feed),
            )

        # Clear download flag on episodes for feeds that don't have any unplayed episodes
        for html_feed in html_feeds:
            if html_feed.is_played:
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
    for db_feed in islice(_feeds_to_refresh(ctx), limit):
        _refresh_feed(ctx, db_feed)


def _feeds_to_refresh(ctx: Context) -> Iterator[db.Feed]:
    try:
        html_feeds = overcast.fetch_podcasts(session=ctx.session)
    except overcast.RatedLimitedError:
        logger.error("Rate limited")
        return

    feeds = list(_zip_html_db_feeds(html_feeds=html_feeds, db_feeds=ctx.db.feeds))

    db_download_counts = ctx.db.episodes.download_counts
    for html_feed, db_feed in feeds:
        db_feed_downloads = db_download_counts.get(db_feed.id, 0)
        if html_feed.is_current != (db_feed_downloads > 0):
            logger.info(
                "Feed out of sync: %s; current: %s but downloads count: %i",
                db_feed.clean_title,
                html_feed.is_current,
                db_feed_downloads,
            )
            overcast.expire_podcast(ctx.session, html_feed.overcast_url)
            yield db_feed

    def cache_request_date(feed: tuple[overcast.HTMLPodcastsFeed, db.Feed]) -> datetime:
        url = feed[0].overcast_url
        cache_date = overcast.last_request_date(ctx.session, url)
        logger.debug("%s last request date: %s", url, cache_date)
        return cache_date

    for html_feed, db_feed in sorted(feeds, key=cache_request_date):
        yield db_feed


def _zip_html_db_feeds(
    html_feeds: Iterable[overcast.HTMLPodcastsFeed],
    db_feeds: Iterable[db.Feed],
) -> Iterator[tuple[overcast.HTMLPodcastsFeed, db.Feed]]:
    id_to_db_feed = {f.id: f for f in db_feeds}
    for html_feed in html_feeds:
        db_feed = id_to_db_feed.get(html_feed.item_id)
        if not db_feed:
            logger.warning("Feed '%s' not found in database", html_feed.item_id)
            continue
        yield html_feed, db_feed


def _refresh_feed(ctx: Context, db_feed: db.Feed) -> None:
    logger.info("Refreshing feed '%s'", db_feed.clean_title)

    feed_id = db_feed.id
    feed_url = db_feed.overcast_url
    if not feed_url:
        logger.warning("Feed '%s' has no Overcast URL", db_feed.id)
        return

    def on_episode_insert(
        episode_url: overcast.OvercastEpisodeURL,
        html_episode: overcast.HTMLPodcastEpisode,
    ) -> db.Episode:
        assert episode_url == html_episode.overcast_url
        return db.Episode(
            id=None,
            overcast_url=episode_url,
            feed_id=feed_id,
            title=html_episode.title,
            duration=html_episode.duration,
            enclosure_url=None,
            date_published=html_episode.date_published_datetime,
            is_played=html_episode.is_played,
            is_downloaded=html_episode.is_new,
            did_download=html_episode.is_new,
        )

    def on_episode_update(
        db_episode: db.Episode,
        html_episode: overcast.HTMLPodcastEpisode,
    ) -> db.Episode:
        assert db_episode.overcast_url == html_episode.overcast_url

        if db_episode.duration is None and html_episode.duration is not None:
            db_episode.duration = html_episode.duration

        if db_episode.date_published is None:
            db_episode.date_published = html_episode.date_published_datetime

        if html_episode.is_played is not None:
            db_episode.is_played = html_episode.is_played
        if html_episode.is_new:
            db_episode.is_played = False

        db_episode.is_downloaded = html_episode.is_new
        if html_episode.is_new or html_episode.is_played:
            db_episode.did_download = True

        return db_episode

    try:
        html_podcast = overcast.fetch_podcast(session=ctx.session, feed_url=feed_url)
        for html_episode in html_podcast.episodes:
            ctx.db.episodes.insert_or_update(
                episode_url=html_episode.overcast_url,
                on_insert=partial(on_episode_insert, html_episode=html_episode),
                on_update=partial(on_episode_update, html_episode=html_episode),
            )

    except overcast.RatedLimitedError:
        logger.error("Rate limited")
        return


@cli.command("backfill-episode")
@click.option("--limit", type=int, default=1, show_default=True)
@click.option("--randomize-order", is_flag=True, default=False)
@click.pass_obj
def backfill_episode(ctx: Context, limit: int, randomize_order: bool) -> None:
    logger.info("[backfill-episode] %s", limit)

    episodes = list(_episodes_missing_optional_info(ctx))
    if randomize_order:
        random.shuffle(episodes)

    for db_episode in islice(episodes, limit):
        try:
            html_episode = overcast.fetch_episode(
                session=ctx.session,
                episode_url=db_episode.overcast_url,
            )

            if db_episode.id is None:
                db_episode.id = html_episode.item_id

            if db_episode.enclosure_url is None:
                db_episode.enclosure_url = html_episode.enclosure_url

            if db_episode.duration is None:
                db_episode.duration = overcast.fetch_audio_duration(
                    ctx.session, html_episode.enclosure_url
                )

        except overcast.RatedLimitedError:
            logger.error("Rate limited")
            continue


def _episodes_missing_optional_info(ctx: Context) -> Iterator[db.Episode]:
    episodes = sorted(ctx.db.episodes, key=lambda e: e.date_published, reverse=True)

    db_episodes_missing_duration = [e for e in episodes if e.duration is None]
    db_episodes_missing_enclosure_url = [e for e in episodes if e.enclosure_url is None]
    db_episodes_missing_id = [e for e in episodes if e.id is None]

    if db_episodes_missing_duration:
        logger.info(
            "[backfill-episode] %i episodes missing duration",
            len(db_episodes_missing_duration),
        )
    if db_episodes_missing_enclosure_url:
        logger.info(
            "[backfill-episode] %i episodes missing enclosure URL",
            len(db_episodes_missing_enclosure_url),
        )
    if db_episodes_missing_id:
        logger.info(
            "[backfill-episode] %i episodes missing ID", len(db_episodes_missing_id)
        )

    yield from db_episodes_missing_duration
    yield from db_episodes_missing_enclosure_url
    yield from db_episodes_missing_id


@cli.command("metrics")
@click.option("--metrics-filename", type=click.Path(path_type=Path))
@click.pass_obj
def metrics(ctx: Context, metrics_filename: str | None) -> None:
    registry = CollectorRegistry()

    episode_labelnames = ["feed_slug", "played", "downloaded", "did_download"]
    label_combinations = [
        # New
        {"played": "false", "downloaded": "true", "did_download": "true"},
        # Skipped
        {"played": "false", "downloaded": "false", "did_download": "true"},
        # Played
        {"played": "true", "downloaded": "false", "did_download": "true"},
        # Unfollowed
        {"played": "false", "downloaded": "false", "did_download": "false"},
    ]

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
        feed_slug = db_feed.slug
        feed_slugs[db_feed.id] = feed_slug

        for label_combination in label_combinations:
            overcast_episode_count.labels(
                feed_slug=feed_slug,
                played=label_combination["played"],
                downloaded=label_combination["downloaded"],
                did_download=label_combination["did_download"],
            ).set(0)
            overcast_episode_minutes.labels(
                feed_slug=feed_slug,
                played=label_combination["played"],
                downloaded=label_combination["downloaded"],
                did_download=label_combination["did_download"],
            ).set(0)

    for db_episode in ctx.db.episodes:
        feed_slug = feed_slugs[db_episode.feed_id]
        played: str = "true" if db_episode.is_played is True else "false"
        downloaded: str = "true" if db_episode.is_downloaded is True else "false"
        did_download: str = "true" if db_episode.did_download is True else "false"

        if played == "true" and downloaded == "true":
            logger.warning(
                "Episode %s is played and downloaded",
                db_episode.overcast_url,
            )
        if downloaded == "true" and did_download == "false":
            logger.warning(
                "Episode %s is downloaded but marked as not downloaded",
                db_episode.overcast_url,
            )

        overcast_episode_count.labels(
            feed_slug=feed_slug,
            played=played,
            downloaded=downloaded,
            did_download=did_download,
        ).inc()
        if db_episode.duration:
            minutes = db_episode.duration.total_seconds() / 60
            overcast_episode_minutes.labels(
                feed_slug=feed_slug,
                played=played,
                downloaded=downloaded,
                did_download=did_download,
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
    ctx.session.requests_session.purge_cache(older_than=timedelta(days=30))


if __name__ == "__main__":
    cli()
