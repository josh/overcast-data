import csv
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


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


@dataclass
class Episode:
    id: str
    feed_id: str
    title: str

    @staticmethod
    def fieldnames() -> list[str]:
        return ["id", "feed_id", "title"]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Episode":
        return Episode(
            id=data["id"],
            feed_id=data["feed_id"],
            title=data["title"],
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "id": self.id,
            "feed_id": self.feed_id,
            "title": self.title,
        }


def load_feeds(filename: Path) -> list[Feed]:
    with filename.open("r") as csvfile:
        rows = csv.DictReader(csvfile)
        return [Feed.from_dict(row) for row in rows]


def load_episodes(filename: Path) -> list[Episode]:
    with filename.open("r") as csvfile:
        rows = csv.DictReader(csvfile)
        return [Episode.from_dict(row) for row in rows]


def save_feeds(filename: Path, feeds: Iterable[Feed]) -> None:
    feeds_lst = list(feeds)

    assert len(set(f.id for f in feeds_lst)) == len(feeds_lst), "Duplicate IDs"
    assert len(set(f.numeric_id for f in feeds_lst)) == len(
        feeds_lst
    ), "Duplicate numeric IDs"

    feeds_lst.sort(key=lambda e: e.numeric_id)

    with filename.open("w") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=Feed.fieldnames(),
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for feed in feeds_lst:
            writer.writerow(feed.to_dict())


def save_episodes(filename: Path, episodes: Iterable[Episode]) -> None:
    episodes_lst = list(episodes)

    assert len(set(e.id for e in episodes_lst)) == len(episodes_lst), "Duplicate IDs"

    # TODO: Sort by pubdate
    episodes_lst.sort(key=lambda e: e.id)

    with filename.open("w") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=Episode.fieldnames(),
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for episode in episodes_lst:
            writer.writerow(episode.to_dict())


def insert_or_update_episode(episodes: list[Episode], episode: Episode) -> None:
    for i, e in enumerate(episodes):
        if e.id == episode.id:
            episodes[i] = episode
            return

    episodes.append(episode)
