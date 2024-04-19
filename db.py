import csv
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator

from overcast import (
    EpisodeWebID,
    HTMLPodcastsFeed,
    OvercastFeedURL,
    PodcastItemID,
    PodcastWebID,
)

logger = logging.getLogger("db")


@dataclass
class Feed:
    overcast_url: OvercastFeedURL
    id: PodcastWebID
    numeric_id: PodcastItemID | None
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
        return ["overcast_url", "id", "numeric_id", "title", "slug", "added_at"]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Feed":
        numeric_id: PodcastItemID | None = None
        added_at: datetime | None = None

        if n := data.get("numeric_id"):
            numeric_id = PodcastItemID(int(n))

        if a := data.get("added_at"):
            added_at = datetime.fromisoformat(a)

        return Feed(
            overcast_url=OvercastFeedURL(data.get("overcast_url", "")),
            id=PodcastWebID(data.get("id", "")),
            numeric_id=numeric_id,
            title=data.get("title", ""),
            added_at=added_at,
        )

    def to_dict(self) -> dict[str, str]:
        d: dict[str, str] = {}

        if self.overcast_url:
            d["overcast_url"] = str(self.overcast_url)
        if self.numeric_id:
            d["numeric_id"] = str(self.numeric_id)
        if self.id:
            d["id"] = self.id
        if self.title:
            d["title"] = self.title
            d["slug"] = self.slug()
        if self.added_at:
            d["added_at"] = self.added_at.isoformat()

        return d

    @staticmethod
    def from_html_feed(feed: HTMLPodcastsFeed) -> "Feed":
        return Feed(
            overcast_url=feed.html_url,
            id=feed.id,
            numeric_id=feed.item_id,
            title=Feed.clean_title(feed.title),
            added_at=None,
        )


class FeedCollection:
    @staticmethod
    def load(filename: Path) -> "FeedCollection":
        with filename.open("r") as csvfile:
            rows = csv.DictReader(csvfile)
            return FeedCollection(Feed.from_dict(row) for row in rows)

    _feeds: list[Feed]

    def __init__(self, feeds: Iterable[Feed]) -> None:
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
        assert len(set(f.numeric_id for f in feeds_lst)) == len(
            feeds_lst
        ), "Duplicate numeric IDs"

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
                if feed.numeric_id:
                    self._feeds[i].numeric_id = feed.numeric_id
                if feed.title:
                    self._feeds[i].title = feed.title
                if feed.added_at:
                    self._feeds[i].added_at = feed.added_at
                return

        if not feed.id:
            logger.warning("Can't insert feed without ID: %s", feed)
            return

        self._feeds.append(feed)
        self.sort()


@dataclass
class Episode:
    id: EpisodeWebID
    feed_id: PodcastWebID
    title: str
    duration: timedelta | None

    @staticmethod
    def fieldnames() -> list[str]:
        return ["id", "feed_id", "title", "duration"]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Episode":
        return Episode(
            id=EpisodeWebID(data["id"]),
            feed_id=PodcastWebID(data["feed_id"]),
            title=data["title"],
            duration=_seconds_str_to_timedelta(data.get("duration")),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "id": self.id,
            "feed_id": self.feed_id,
            "title": self.title,
            "duration": _timedelta_to_seconds_str(self.duration),
        }


def _timedelta_to_seconds_str(td: timedelta | None) -> str:
    if not td:
        return ""
    return str(int(td.total_seconds()))


def _seconds_str_to_timedelta(s: str | None) -> timedelta | None:
    if not s:
        return None

    seconds = int(s)
    minutes = 0
    if seconds >= 60:
        minutes = seconds // 60
        seconds %= 60

    return timedelta(minutes=minutes, seconds=seconds)


class EpisodeCollection:
    @staticmethod
    def load(filename: Path) -> "EpisodeCollection":
        with filename.open("r") as csvfile:
            rows = csv.DictReader(csvfile)
            return EpisodeCollection(Episode.from_dict(row) for row in rows)

    _episodes: list[Episode]

    def __init__(self, episodes: Iterable[Episode]) -> None:
        self._episodes = list(episodes)

    def __len__(self) -> int:
        return len(self._episodes)

    def __iter__(self) -> Iterator[Episode]:
        yield from self._episodes

    def insert(self, episode: Episode) -> None:
        for i, e in enumerate(self._episodes):
            if e.id == episode.id:
                self._episodes[i].feed_id = episode.feed_id
                self._episodes[i].title = episode.title
                if episode.duration:
                    self._episodes[i].duration = episode.duration
                return

        self._episodes.append(episode)
        self.sort()

    def sort(self) -> None:
        # TODO: Sort by pubdate
        self._episodes.sort(key=lambda e: e.id)

    def save(self, filename: Path) -> None:
        episodes_lst = list(self._episodes)

        assert len(set(e.id for e in episodes_lst)) == len(
            episodes_lst
        ), "Duplicate IDs"

        with filename.open("w") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=Episode.fieldnames(),
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()
            for episode in episodes_lst:
                writer.writerow(episode.to_dict())
