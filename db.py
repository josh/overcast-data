import csv
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator


@dataclass
class Feed:
    numeric_id: int
    id: str
    title: str
    added_at: datetime

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
        return ["id", "numeric_id", "title", "slug", "added_at"]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Feed":
        return Feed(
            numeric_id=int(data["numeric_id"]),
            id=data["id"],
            title=data["title"],
            added_at=datetime.fromisoformat(data["added_at"]),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "numeric_id": str(self.numeric_id),
            "id": self.id,
            "title": self.title,
            "slug": self.slug(),
            "added_at": self.added_at.isoformat(),
        }


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
        self._feeds.sort(key=lambda f: f.numeric_id)

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


@dataclass
class Episode:
    id: str
    feed_id: str
    title: str
    duration: timedelta | None

    @staticmethod
    def fieldnames() -> list[str]:
        return ["id", "feed_id", "title", "duration"]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Episode":
        return Episode(
            id=data["id"],
            feed_id=data["feed_id"],
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
