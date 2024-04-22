import csv
import logging
import re
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Iterable, Iterator

from overcast import (
    ExtendedExportEpisode,
    ExtendedExportFeed,
    HTMLEpisode,
    HTMLPodcastEpisode,
    HTMLPodcastsFeed,
    OvercastEpisodeItemID,
    OvercastEpisodeURL,
    OvercastFeedItemID,
    OvercastFeedURL,
)

logger = logging.getLogger("db")


@dataclass
class Feed:
    id: OvercastFeedItemID
    overcast_url: OvercastFeedURL | None
    title: str
    html_url: str | None
    added_at: datetime | None
    is_subscribed: bool

    def slug(self) -> str:
        title = re.sub(r"[^\w\s]", "", self.title)
        title = re.sub(r"\s+", "-", title)
        title = title.lower().removesuffix("-")
        return title

    @staticmethod
    def clean_title(title: str) -> str:
        title = re.sub(r" — Private to .+", "", title)
        title = re.sub(r"\s*\([^)]*\)\s*", "", title)
        title = re.sub(r"\s*\[[^]]*\]\s*", "", title)
        title = re.sub(r"\s*:[^:]*$", "", title)
        title = re.sub(r"\s*- Patreon Exclusive Feed$", "", title)
        title = title.split(" | ")[0]
        title = title.strip()
        return title

    @staticmethod
    def fieldnames() -> list[str]:
        return [
            "id",
            "overcast_url",
            "title",
            "slug",
            "html_url",
            "added_at",
            "is_subscribed",
        ]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Feed":
        id = OvercastFeedItemID(int(data["id"]))
        overcast_url: OvercastFeedURL | None = None
        title = data.get("title", "")
        html_url: str | None = None
        added_at: datetime | None = None
        is_subscribed: bool = False

        if data.get("overcast_url"):
            overcast_url = OvercastFeedURL(data["overcast_url"])

        if data.get("html_url"):
            html_url = data["html_url"]

        if data.get("added_at"):
            added_at = datetime.fromisoformat(data["added_at"])

        if data.get("is_subscribed"):
            is_subscribed = data["is_subscribed"] == "1"

        return Feed(
            id=id,
            overcast_url=overcast_url,
            title=title,
            html_url=html_url,
            added_at=added_at,
            is_subscribed=is_subscribed,
        )

    def to_dict(self) -> dict[str, str]:
        d: dict[str, str] = {}

        d["id"] = str(self.id)
        if self.overcast_url:
            d["overcast_url"] = str(self.overcast_url)
        d["title"] = self.title
        d["slug"] = self.slug()
        if self.html_url:
            d["html_url"] = self.html_url
        if self.added_at:
            d["added_at"] = self.added_at.isoformat()
        d["is_subscribed"] = "1" if self.is_subscribed else "0"

        return d

    @staticmethod
    def from_html_feed(feed: HTMLPodcastsFeed) -> "Feed":
        return Feed(
            id=feed.item_id,
            overcast_url=feed.overcast_url,
            title=Feed.clean_title(feed.title),
            html_url=None,
            added_at=None,
            is_subscribed=True,
        )

    @staticmethod
    def from_export_feed(feed: ExtendedExportFeed) -> "Feed":
        return Feed(
            id=feed.item_id,
            overcast_url=None,
            title=Feed.clean_title(feed.title),
            html_url=feed.html_url,
            added_at=feed.added_at,
            is_subscribed=feed.is_subscribed,
        )


class FeedCollection:
    @staticmethod
    def load(filename: Path) -> "FeedCollection":
        with filename.open("r") as csvfile:
            rows = csv.DictReader(csvfile)
            return FeedCollection(Feed.from_dict(row) for row in rows)

    _feeds: list[Feed]

    def __init__(self, feeds: Iterable[Feed] = []) -> None:
        self._feeds = list(feeds)

    def __len__(self) -> int:
        return len(self._feeds)

    def __iter__(self) -> Iterator[Feed]:
        yield from self._feeds

    def sort(self) -> None:
        self._feeds.sort(key=lambda f: f.added_at or datetime.max)

    def save(self, filename: Path) -> None:
        feeds_lst = list(self._feeds)
        assert len(set(f.id for f in feeds_lst)) == len(feeds_lst), "Duplicate IDs"

        with filename.open("w") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=Feed.fieldnames(),
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()
            for feed in feeds_lst:
                writer.writerow(feed.to_dict())

    def insert(self, feed: Feed) -> None:
        for i, f in enumerate(self._feeds):
            if f.id == feed.id:
                if feed.overcast_url:
                    self._feeds[i].overcast_url = feed.overcast_url
                if feed.title:
                    self._feeds[i].title = feed.title
                if feed.html_url:
                    self._feeds[i].html_url = feed.html_url
                if feed.added_at:
                    self._feeds[i].added_at = feed.added_at
                self._feeds[i].is_subscribed = feed.is_subscribed
                return

        if not feed.id:
            logger.warning("Can't insert feed without Overcast ID: %s", feed)
            return

        self._feeds.append(feed)
        self.sort()


@dataclass
class Episode:
    overcast_url: OvercastEpisodeURL
    id: OvercastEpisodeItemID | None
    feed_id: OvercastFeedItemID
    title: str
    duration: timedelta | None
    date_published: datetime | None

    @staticmethod
    def fieldnames() -> list[str]:
        return [
            "overcast_url",
            "id",
            "feed_id",
            "title",
            "duration",
            "date_published",
        ]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Episode":
        id: OvercastEpisodeItemID | None = None
        overcast_url = OvercastEpisodeURL(data["overcast_url"])
        feed_id = OvercastFeedItemID(int(data["feed_id"]))
        title = ""
        duration = None
        date_published = None

        if data.get("id"):
            id = OvercastEpisodeItemID(int(data["id"]))

        if data.get("title"):
            title = data["title"]

        if data.get("duration"):
            duration = _seconds_str_to_timedelta(data["duration"])

        if data.get("date_published"):
            date_published = datetime.fromisoformat(data["date_published"])

        return Episode(
            id=id,
            overcast_url=overcast_url,
            feed_id=feed_id,
            title=title,
            duration=duration,
            date_published=date_published,
        )

    def to_dict(self) -> dict[str, str]:
        d: dict[str, str] = {}

        if self.id:
            d["id"] = str(self.id)
        d["overcast_url"] = str(self.overcast_url)
        d["feed_id"] = str(self.feed_id)
        d["title"] = self.title
        if self.duration:
            d["duration"] = _timedelta_to_seconds_str(self.duration)
        if self.date_published:
            d["date_published"] = self.date_published.isoformat()

        return d

    @staticmethod
    def from_html_podcast_episode(
        episode: HTMLPodcastEpisode,
        feed_id: OvercastFeedItemID,
        episode_id: OvercastEpisodeItemID | None = None,
    ) -> "Episode":
        return Episode(
            id=episode_id,
            overcast_url=episode.overcast_url,
            feed_id=feed_id,
            title=episode.title,
            duration=episode.duration,
            date_published=_date_to_datetime(episode.date_published),
        )

    @staticmethod
    def from_html_episode(episode: HTMLEpisode) -> "Episode":
        return Episode(
            id=episode.item_id,
            overcast_url=episode.overcast_url,
            feed_id=episode.feed_item_id,
            title=episode.title,
            duration=None,
            date_published=_date_to_datetime(episode.date_published),
        )

    @staticmethod
    def from_export_episode(
        episode: ExtendedExportEpisode,
        feed_id: OvercastFeedItemID,
    ) -> "Episode":
        return Episode(
            id=episode.item_id,
            overcast_url=episode.overcast_url,
            feed_id=feed_id,
            title=episode.title,
            duration=None,
            date_published=episode.date_published,
        )


def _timedelta_to_seconds_str(td: timedelta | None) -> str:
    if td is None:
        return ""
    return str(int(td.total_seconds()))


def _seconds_str_to_timedelta(s: str | None) -> timedelta | None:
    if not s:
        return None
    return timedelta(seconds=int(s))


def _date_to_datetime(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time())


def _datetime_has_time_components(dt: datetime | None) -> bool:
    if not dt:
        return False
    return dt.time() != datetime.min.time()


class EpisodeCollection:
    @staticmethod
    def load(filename: Path) -> "EpisodeCollection":
        with filename.open("r") as csvfile:
            rows = csv.DictReader(csvfile)
            return EpisodeCollection(Episode.from_dict(row) for row in rows)

    _episodes: list[Episode]

    def __init__(self, episodes: Iterable[Episode] = []) -> None:
        self._episodes = list(episodes)

    def __len__(self) -> int:
        return len(self._episodes)

    def __iter__(self) -> Iterator[Episode]:
        yield from self._episodes

    def insert(self, episode: Episode) -> None:
        for i, e in enumerate(self._episodes):
            if e.overcast_url == episode.overcast_url:
                if episode.id:
                    self._episodes[i].id = episode.id
                if episode.feed_id:
                    self._episodes[i].feed_id = episode.feed_id
                if episode.title:
                    self._episodes[i].title = episode.title
                if episode.duration:
                    self._episodes[i].duration = episode.duration
                if episode.date_published:
                    if _datetime_has_time_components(e.date_published) and (
                        not _datetime_has_time_components(episode.date_published)
                    ):
                        logger.debug(
                            "Not replacing existing date published with less precision: (%s, %s)",
                            e.date_published,
                            episode.date_published,
                        )
                    else:
                        self._episodes[i].date_published = episode.date_published
                return

        self._episodes.append(episode)
        self.sort()

    def sort(self) -> None:
        self._episodes.sort(key=lambda e: (e.feed_id, (e.date_published or datetime.max)))

    def save(self, filename: Path) -> None:
        episodes_lst = list(self._episodes)

        assert len(set(e.overcast_url for e in episodes_lst)) == len(
            episodes_lst
        ), "Duplicate Overcast URLs"

        with filename.open("w") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=Episode.fieldnames(),
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()
            for episode in episodes_lst:
                writer.writerow(episode.to_dict())


class Database(AbstractContextManager["Database"]):
    path: Path
    feeds: FeedCollection
    episodes: EpisodeCollection

    def __init__(self, path: Path) -> None:
        self.path = path
        self.feeds = FeedCollection()
        self.episodes = EpisodeCollection()

    def __enter__(self) -> "Database":
        logger.debug("loading database: %s", self.path)
        self.feeds = FeedCollection.load(self.path / "feeds.csv")
        self.episodes = EpisodeCollection.load(self.path / "episodes.csv")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            logger.debug("saving database: %s", self.path)
            self.feeds.save(self.path / "feeds.csv")
            self.episodes.save(self.path / "episodes.csv")
        else:
            logger.error("not saving database due to exception")
