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


def load_feeds(filename: Path) -> list[Feed]:
    with filename.open("r") as csvfile:
        rows = csv.DictReader(csvfile)
        return [Feed.from_dict(row) for row in rows]


def save_feeds(filename: Path, feeds: Iterable[Feed]) -> None:
    with filename.open("w") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=Feed.fieldnames(),
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for feed in feeds:
            writer.writerow(feed.to_dict())
