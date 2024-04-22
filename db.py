import csv
import logging
import re
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Iterable, Iterator

from overcast import (
    ExtendedExportFeed,
    HTMLPodcastsFeed,
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
    added_at: datetime | None

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
        return ["id", "overcast_url", "title", "slug", "added_at"]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Feed":
        id = OvercastFeedItemID(int(data["id"]))
        overcast_url: OvercastFeedURL | None = None
        title = data.get("title", "")
        added_at: datetime | None = None

        if data.get("overcast_url"):
            overcast_url = OvercastFeedURL(data["overcast_url"])

        if data.get("added_at"):
            added_at = datetime.fromisoformat(data["added_at"])

        return Feed(
            id=id,
            overcast_url=overcast_url,
            title=title,
            added_at=added_at,
        )

    def to_dict(self) -> dict[str, str]:
        d: dict[str, str] = {}

        d["id"] = str(self.id)
        if self.overcast_url:
            d["overcast_url"] = str(self.overcast_url)
        d["title"] = self.title
        d["slug"] = self.slug()
        if self.added_at:
            d["added_at"] = self.added_at.isoformat()

        return d

    @staticmethod
    def from_html_feed(feed: HTMLPodcastsFeed) -> "Feed":
        return Feed(
            id=feed.item_id,
            overcast_url=feed.overcast_url,
            title=Feed.clean_title(feed.title),
            added_at=None,
        )

    @staticmethod
    def from_export_feed(feed: ExtendedExportFeed) -> "Feed":
        return Feed(
            id=feed.item_id,
            overcast_url=None,
            title=Feed.clean_title(feed.title),
            added_at=feed.added_at,
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
        self._feeds.sort(key=lambda f: f.added_at or 0)

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
                if feed.added_at:
                    self._feeds[i].added_at = feed.added_at
                return

        if not feed.id:
            logger.warning("Can't insert feed without Overcast ID: %s", feed)
            return

        self._feeds.append(feed)
        self.sort()


@dataclass
class Episode:
    overcast_url: OvercastEpisodeURL
    feed_url: OvercastFeedURL
    title: str
    duration: timedelta | None

    @staticmethod
    def fieldnames() -> list[str]:
        return ["overcast_url", "feed_url", "title", "duration"]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Episode":
        overcast_url = OvercastEpisodeURL(data["overcast_url"])
        feed_url = OvercastFeedURL(data["feed_url"])
        title = ""
        duration = None

        if data.get("title"):
            title = data["title"]

        if data.get("duration"):
            duration = _seconds_str_to_timedelta(data["duration"])

        return Episode(
            overcast_url=overcast_url,
            feed_url=feed_url,
            title=title,
            duration=duration,
        )

    def to_dict(self) -> dict[str, str]:
        d: dict[str, str] = {}

        d["overcast_url"] = str(self.overcast_url)
        d["feed_url"] = str(self.feed_url)
        d["title"] = self.title
        if self.duration:
            d["duration"] = _timedelta_to_seconds_str(self.duration)

        return d


def _timedelta_to_seconds_str(td: timedelta | None) -> str:
    if td is None:
        return ""
    return str(int(td.total_seconds()))


def _seconds_str_to_timedelta(s: str | None) -> timedelta | None:
    if not s:
        return None
    return timedelta(seconds=int(s))


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
                if episode.feed_url:
                    self._episodes[i].feed_url = episode.feed_url
                if episode.title:
                    self._episodes[i].title = episode.title
                if episode.duration:
                    self._episodes[i].duration = episode.duration
                return

        self._episodes.append(episode)
        self.sort()

    def sort(self) -> None:
        # TODO: Sort by pubdate
        self._episodes.sort(key=lambda e: e.overcast_url)

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
