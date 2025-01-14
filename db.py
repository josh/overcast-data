import csv
import logging
import re
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import cache
from pathlib import Path
from types import TracebackType

from csvmodel import ascsvdict, fromcsvdict, register_cast
from overcast import (
    OvercastEpisodeItemID,
    OvercastEpisodeURL,
    OvercastFeedItemID,
    OvercastFeedURL,
)
from utils import (
    HTTPURL,
    Ciphertext,
    EncryptionKey,
    decrypt,
    encrypt,
    environ_encryption_key,
)

logger = logging.getLogger("db")


_DATETIME_MAX_TZ_AWARE = datetime.max.replace(tzinfo=timezone.utc)

register_cast(OvercastFeedURL, fromstr=OvercastFeedURL)
register_cast(OvercastEpisodeURL, fromstr=OvercastEpisodeURL)
register_cast(HTTPURL, fromstr=HTTPURL)


@dataclass
class Feed:
    id: OvercastFeedItemID
    overcast_url: OvercastFeedURL | None
    title: str
    html_url: str | None
    added_at: datetime | None

    # Is in "All Podcast" list
    is_added: bool

    # Is "Follow All New Episodes" checked
    is_following: bool | None

    @property
    def is_private(self) -> bool:
        if self.overcast_url is None:
            return True
        return self.overcast_url.startswith("https://overcast.fm/p")

    @property
    def clean_title(self) -> str:
        if not self.is_private:
            return self.title
        title = re.sub(r" — Private to .+", "", self.title)
        title = re.sub(r"\s*\([^)]*\)\s*", "", title)
        title = re.sub(r"\s*\[[^]]*\]\s*", "", title)
        title = re.sub(r"\s*:[^:]*$", "", title)
        title = re.sub(r"\s*- Patreon Exclusive Feed$", "", title)
        title = title.split(" | ")[0]
        title = title.strip()
        return title

    @property
    def slug(self) -> str:
        title = re.sub(r"[^\w\s]", "", self.clean_title)
        title = re.sub(r"\s+", "-", title)
        title = title.lower().removesuffix("-")
        return title

    def _sort_key(self) -> datetime:
        return self.added_at or _DATETIME_MAX_TZ_AWARE

    @staticmethod
    def fieldnames() -> list[str]:
        return [
            "id",
            "overcast_url",
            "encrypted_title",
            "clean_title",
            "slug",
            "html_url",
            "added_at",
            "is_added",
            "is_following",
        ]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Feed":
        data = data.copy()
        del data["clean_title"]
        del data["slug"]
        _decrypt_csv_field(data, "title")
        return fromcsvdict(Feed, data)

    def to_dict(self) -> dict[str, str]:
        d = ascsvdict(self)
        d["clean_title"] = self.clean_title
        d["slug"] = self.slug
        _encrypt_csv_field(d, "title")
        return d


class FeedCollection:
    @staticmethod
    def load(filename: Path) -> "FeedCollection":
        with filename.open("r") as csvfile:
            rows = csv.DictReader(csvfile)
            return FeedCollection(Feed.from_dict(row) for row in rows)

    _feeds: list[Feed]
    _initial_nonnull_counts: dict[str, int]

    def __init__(self, feeds: Iterable[Feed] = []) -> None:
        self._feeds = list(feeds)
        self._initial_nonnull_counts = self._nonnull_counts()

    def _nonnull_counts(self) -> dict[str, int]:
        counts = {}
        for field_name in Feed.fieldnames():
            if field_name.startswith("encrypted_"):
                field_name = field_name.removeprefix("encrypted_")
            count = len([f for f in self._feeds if getattr(f, field_name) is not None])
            counts[field_name] = count
        return counts

    def __len__(self) -> int:
        return len(self._feeds)

    def __iter__(self) -> Iterator[Feed]:
        yield from self._feeds

    def sort(self) -> None:
        self._feeds.sort(key=Feed._sort_key)

    def save(self, filename: Path) -> None:
        feeds_lst = list(self._feeds)

        for field, count in self._nonnull_counts().items():
            assert count >= self._initial_nonnull_counts[field], (
                f"{field} non-null count decreased"
            )

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

    def insert_or_update(
        self,
        feed_id: OvercastFeedItemID,
        on_insert: Callable[[OvercastFeedItemID], Feed],
        on_update: Callable[[Feed], Feed],
    ) -> None:
        append = True

        for i, f in enumerate(self._feeds):
            if f.id == feed_id:
                self._feeds[i] = on_update(f)
                append = False
                break

        if append:
            feed = on_insert(feed_id)
            self._feeds.append(feed)

        self.sort()


@dataclass
class Episode:
    id: OvercastEpisodeItemID | None
    overcast_url: OvercastEpisodeURL
    feed_id: OvercastFeedItemID
    title: str
    enclosure_url: HTTPURL | None
    duration: timedelta | None
    date_published: datetime
    is_played: bool | None
    is_downloaded: bool
    did_download: bool

    def _sort_key(self) -> tuple[int, datetime]:
        return (self.feed_id, self.date_published)

    @staticmethod
    def fieldnames() -> list[str]:
        return [
            "id",
            "encrypted_overcast_url",
            "feed_id",
            "title",
            "encrypted_enclosure_url",
            "duration",
            "date_published",
            "is_played",
            "is_downloaded",
            "did_download",
        ]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Episode":
        data = data.copy()
        _decrypt_csv_field(data, "overcast_url")
        _decrypt_csv_field(data, "enclosure_url")
        return fromcsvdict(Episode, data)

    def to_dict(self) -> dict[str, str]:
        d = ascsvdict(self)
        _encrypt_csv_field(d, "overcast_url")
        _encrypt_csv_field(d, "enclosure_url")
        return d


class EpisodeCollection:
    @staticmethod
    def load(filename: Path) -> "EpisodeCollection":
        with filename.open("r") as csvfile:
            rows = csv.DictReader(csvfile)
            return EpisodeCollection(Episode.from_dict(row) for row in rows)

    _episodes: list[Episode]
    _initial_nonnull_counts: dict[str, int]

    def __init__(self, episodes: Iterable[Episode] = []) -> None:
        self._episodes = list(episodes)
        self._initial_nonnull_counts = self._nonnull_counts()

    def _nonnull_counts(self) -> dict[str, int]:
        counts = {}
        for field_name in Episode.fieldnames():
            if field_name.startswith("encrypted_"):
                field_name = field_name.removeprefix("encrypted_")
            count = len(
                [f for f in self._episodes if getattr(f, field_name) is not None]
            )
            counts[field_name] = count
        return counts

    def __len__(self) -> int:
        return len(self._episodes)

    def __iter__(self) -> Iterator[Episode]:
        yield from self._episodes

    def insert_or_update(
        self,
        episode_url: OvercastEpisodeURL,
        on_insert: Callable[[OvercastEpisodeURL], Episode],
        on_update: Callable[[Episode], Episode],
    ) -> None:
        append = True

        for i, e in enumerate(self._episodes):
            if e.overcast_url == episode_url:
                self._episodes[i] = on_update(e)
                append = False
                break

        if append:
            episode = on_insert(episode_url)
            self._episodes.append(episode)

        self.sort()

    def sort(self) -> None:
        self._episodes.sort(key=Episode._sort_key)

    def save(self, filename: Path) -> None:
        episodes_lst = list(self._episodes)

        for field, count in self._nonnull_counts().items():
            assert count >= self._initial_nonnull_counts[field], (
                f"{field} non-null count decreased"
            )

        assert len(set(e.overcast_url for e in episodes_lst)) == len(episodes_lst), (
            "Duplicate Overcast URLs"
        )

        with filename.open("w") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=Episode.fieldnames(),
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()
            for episode in episodes_lst:
                writer.writerow(episode.to_dict())

    @property
    def download_counts(self) -> dict[OvercastFeedItemID, int]:
        counts: dict[OvercastFeedItemID, int] = {}
        for episode in self._episodes:
            if episode.is_downloaded:
                counts[episode.feed_id] = counts.get(episode.feed_id, 0) + 1
        return counts


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


@cache
def _encryption_key() -> EncryptionKey:
    key = environ_encryption_key()
    assert key, "ENCRYPTION_KEY is not set"
    return key


def _decrypt_csv_field(data: dict[str, str], name: str) -> None:
    encrypted_name = f"encrypted_{name}"
    if data.get(encrypted_name):
        data[name] = decrypt(_encryption_key(), Ciphertext(data[encrypted_name]))
    else:
        data[name] = ""
    if encrypted_name in data:
        del data[encrypted_name]


def _encrypt_csv_field(data: dict[str, str], name: str) -> None:
    encrypted_name = f"encrypted_{name}"
    if data.get(name):
        data[encrypted_name] = encrypt(_encryption_key(), str(data[name]))
    else:
        data[encrypted_name] = ""
    if name in data:
        del data[name]
