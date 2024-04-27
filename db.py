import csv
import logging
import re
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import TracebackType
from typing import Callable, Iterable, Iterator

from overcast import (
    OvercastEpisodeItemID,
    OvercastEpisodeURL,
    OvercastFeedItemID,
    OvercastFeedURL,
)

logger = logging.getLogger("db")

_DATETIME_MAX_TZ_AWARE = datetime.max.replace(tzinfo=timezone.utc)


@dataclass
class Feed:
    id: OvercastFeedItemID
    overcast_url: OvercastFeedURL | None
    clean_title: str
    html_url: str | None
    added_at: datetime | None

    # Is in "All Podcast" list
    is_added: bool

    # Is "Follow All New Episodes" checked
    is_following: bool | None

    def slug(self) -> str:
        title = re.sub(r"[^\w\s]", "", self.clean_title)
        title = re.sub(r"\s+", "-", title)
        title = title.lower().removesuffix("-")
        return title

    @staticmethod
    def _clean_title(title: str) -> str:
        title = re.sub(r" — Private to .+", "", title)
        title = re.sub(r"\s*\([^)]*\)\s*", "", title)
        title = re.sub(r"\s*\[[^]]*\]\s*", "", title)
        title = re.sub(r"\s*:[^:]*$", "", title)
        title = re.sub(r"\s*- Patreon Exclusive Feed$", "", title)
        title = title.split(" | ")[0]
        title = title.strip()
        return title

    def _sort_key(self) -> datetime:
        return self.added_at or _DATETIME_MAX_TZ_AWARE

    @staticmethod
    def fieldnames() -> list[str]:
        return [
            "id",
            "overcast_url",
            "clean_title",
            "slug",
            "html_url",
            "added_at",
            "is_added",
            "is_following",
        ]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Feed":
        id = OvercastFeedItemID(int(data["id"]))
        overcast_url: OvercastFeedURL | None = None
        clean_title = data.get("clean_title", "")
        html_url: str | None = None
        added_at: datetime | None = None
        is_added: bool = False
        is_following: bool = False

        if data.get("overcast_url"):
            overcast_url = OvercastFeedURL(data["overcast_url"])

        if data.get("html_url"):
            html_url = data["html_url"]

        if data.get("added_at"):
            added_at = datetime.fromisoformat(data["added_at"])
            if added_at.tzinfo is None:
                logger.warning(
                    "Feed '%s' added_at is not timezone-aware: %s",
                    clean_title,
                    added_at,
                )

        if data.get("is_added"):
            is_added = data["is_added"] == "1"

        if data.get("is_following"):
            is_following = data["is_following"] == "1"

        if is_following is True and is_added is False:
            logger.warning(
                "Feed '%s' is_following is True but is_added is False", clean_title
            )

        return Feed(
            id=id,
            overcast_url=overcast_url,
            clean_title=clean_title,
            html_url=html_url,
            added_at=added_at,
            is_added=is_added,
            is_following=is_following,
        )

    def to_dict(self) -> dict[str, str]:
        d: dict[str, str] = {}

        d["id"] = str(self.id)

        d["overcast_url"] = ""
        if self.overcast_url:
            d["overcast_url"] = str(self.overcast_url)

        d["clean_title"] = self.clean_title
        d["slug"] = self.slug()

        d["html_url"] = ""
        if self.html_url:
            d["html_url"] = self.html_url

        d["added_at"] = ""
        if self.added_at:
            d["added_at"] = self.added_at.isoformat()

        d["is_added"] = "1" if self.is_added else "0"

        d["is_following"] = ""
        if self.is_following is not None:
            d["is_following"] = "1" if self.is_following else "0"

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
            assert (
                count >= self._initial_nonnull_counts[field]
            ), f"{field} non-null count decreased"

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
    overcast_url: OvercastEpisodeURL
    id: OvercastEpisodeItemID | None
    feed_id: OvercastFeedItemID
    title: str
    duration: timedelta | None
    date_published: datetime
    is_played: bool | None
    is_downloaded: bool

    def _sort_key(self) -> tuple[int, datetime]:
        return (self.feed_id, self.date_published)

    @staticmethod
    def fieldnames() -> list[str]:
        return [
            "overcast_url",
            "id",
            "feed_id",
            "title",
            "duration",
            "date_published",
            "is_played",
            "is_downloaded",
        ]

    @staticmethod
    def from_dict(data: dict[str, str]) -> "Episode":
        id: OvercastEpisodeItemID | None = None
        overcast_url = OvercastEpisodeURL(data["overcast_url"])
        feed_id = OvercastFeedItemID(int(data["feed_id"]))
        title = ""
        duration = None
        date_published = datetime.fromisoformat(data["date_published"])
        is_played: bool | None = None
        is_downloaded: bool = data["is_downloaded"] == "1"

        if data.get("id"):
            id = OvercastEpisodeItemID(int(data["id"]))

        if data.get("title"):
            title = data["title"]

        if data.get("duration"):
            duration = _seconds_str_to_timedelta(data["duration"])

        if date_published.tzinfo is None:
            logger.warning(
                "Episode '%s' date_published is not timezone-aware: %s",
                title,
                date_published,
            )

        if data.get("is_played"):
            is_played = data["is_played"] == "1"

        if is_downloaded is True and is_played is True:
            logger.warning("Episode is downloaded but already played: %s", title)

        return Episode(
            id=id,
            overcast_url=overcast_url,
            feed_id=feed_id,
            title=title,
            duration=duration,
            date_published=date_published,
            is_played=is_played,
            is_downloaded=is_downloaded,
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
        d["date_published"] = self.date_published.isoformat()
        if self.is_played is not None:
            d["is_played"] = "1" if self.is_played else "0"
        d["is_downloaded"] = "1" if self.is_downloaded else "0"

        return d


def _timedelta_to_seconds_str(td: timedelta | None) -> str:
    if td is None:
        return ""
    return str(int(td.total_seconds()))


def _seconds_str_to_timedelta(s: str | None) -> timedelta | None:
    if not s:
        return None
    return timedelta(seconds=int(s))


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
    _initial_nonnull_counts: dict[str, int]

    def __init__(self, episodes: Iterable[Episode] = []) -> None:
        self._episodes = list(episodes)
        self._initial_nonnull_counts = self._nonnull_counts()

    def _nonnull_counts(self) -> dict[str, int]:
        counts = {}
        for field_name in Episode.fieldnames():
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
            assert (
                count >= self._initial_nonnull_counts[field]
            ), f"{field} non-null count decreased"

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
