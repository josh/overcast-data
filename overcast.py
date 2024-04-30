import logging
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Literal, NewType, cast
from urllib.parse import urlparse

import dateutil.parser
import mutagen
import requests
from bs4 import BeautifulSoup, Tag

import requests_cache
from lru_cache import LRUCache
from utils import HTTPURL, URL

logger = logging.getLogger("overcast")

_RAISE_VALIDATION_ERRORS = "pytest" in sys.modules

_SAFARI_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.3 "
    "Safari/605.1.15"
)

_SAFARI_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "User-Agent": _SAFARI_UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
}

# America/New_York
_SERVER_TZINFO = timezone(-timedelta(hours=5))


class OvercastURL(HTTPURL):
    """
    An https://overcast.fm/ URL.
    """

    def __new__(cls, urlstring: str) -> "OvercastURL":
        try:
            if not urlstring.startswith("https://overcast.fm/"):
                raise ValueError(f"Invalid overcast.fm URL: {urlstring}")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)


def _overcast_fm_url_from_path(path: str) -> OvercastURL:
    if not path.startswith("/"):
        return OvercastURL("")
    return OvercastURL(f"https://overcast.fm{path}")


class OvercastCDNURL(HTTPURL):
    """
    An https://public.overcast-cdn.com/ URL.
    """

    def __new__(cls, urlstring: str) -> "OvercastCDNURL":
        try:
            if not urlstring.startswith("https://public.overcast-cdn.com/"):
                raise ValueError(f"Invalid public.overcast-cdn.com URL: {urlstring}")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)


class OvercastAppURI(URL):
    """
    An overcast:// URL.
    """

    def __new__(cls, urlstring: str) -> "OvercastAppURI":
        try:
            if not urlstring.startswith("overcast://"):
                raise ValueError(f"Invalid overcast: URL: {urlstring}")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)


class OvercastFeedURL(OvercastURL):
    """
    An https://overcast.fm/ feed URL.
    """

    def __new__(cls, urlstring: str) -> "OvercastFeedURL":
        try:
            components = urlparse(urlstring)
            if components.scheme != "https":
                raise ValueError(f"Invalid overcast.fm feed URL: {urlstring}")
            elif not components.hostname == "overcast.fm":
                raise ValueError(f"Invalid overcast.fm feed URL: {urlstring}")
            elif not re.match(
                r"^/(p\d+-[A-Za-z0-9]+|itunes\d+/[A-Za-z0-9-]+)$", components.path
            ):
                raise ValueError(f"Got overcast.fm episode URL: {urlstring}")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)


class OvercastEpisodeURL(OvercastURL):
    """
    An https://overcast.fm/+ episode URL.
    """

    def __new__(cls, urlstring: str) -> "OvercastEpisodeURL":
        try:
            components = urlparse(urlstring)
            if components.scheme != "https":
                raise ValueError(f"Invalid overcast.fm episode URL: {urlstring}")
            elif not components.hostname == "overcast.fm":
                raise ValueError(f"Invalid overcast.fm episode URL: {urlstring}")
            elif not re.match(r"^/(\+[A-Za-z0-9_-]+)$", components.path):
                raise ValueError(f"Invalid overcast.fm episode URL: {urlstring}")
        except ValueError as e:
            if _RAISE_VALIDATION_ERRORS:
                raise e
            else:
                logger.error(e)

        return str.__new__(cls, urlstring)


OvercastFeedItemID = NewType("OvercastFeedItemID", int)
OvercastEpisodeItemID = NewType("OvercastEpisodeItemID", int)


class LoggedOutError(Exception):
    pass


class RatedLimitedError(Exception):
    pass


@dataclass
class Session:
    requests_session: requests_cache.Session
    lru_cache: LRUCache


def session(cache_dir: Path, cookie: str, offline: bool = False) -> Session:
    headers = _SAFARI_HEADERS.copy()
    headers["Cookie"] = f"o={cookie}; qr=-"

    lru_cache = LRUCache(
        path=cache_dir / "overcast.pickle",
        max_bytesize=1024 * 1024,  # 1 MB
        save_on_exit=True,
    )

    requests_session = requests_cache.Session(
        cache_dir=cache_dir,
        base_url="https://overcast.fm",
        headers=headers,
        min_time_between_requests=timedelta(seconds=10),
        offline=offline,
    )

    return Session(requests_session=requests_session, lru_cache=lru_cache)


@dataclass
class HTMLPodcastsFeed:
    fetched_at: datetime
    overcast_url: OvercastFeedURL
    art_url: OvercastCDNURL
    title: str
    has_unplayed_episodes: bool

    @property
    def is_current(self) -> bool:
        return self.has_unplayed_episodes

    @property
    def is_played(self) -> bool:
        return not self.has_unplayed_episodes

    @property
    def is_private(self) -> bool:
        return self.overcast_url.startswith("https://overcast.fm/p")

    @property
    def item_id(self) -> OvercastFeedItemID:
        return _extract_feed_id_from_art_url(self.art_url)

    def _validate(self) -> None:
        try:
            assert self.item_id
            assert self.title
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def fetch_podcasts(session: Session) -> list[HTMLPodcastsFeed]:
    r = _request(
        session=session,
        url=OvercastURL("https://overcast.fm/podcasts"),
        controller="index",
        accept="text/html",
        response_expires_in=timedelta(hours=1),
    )
    fetched_at = requests_cache.response_date(r)

    feeds: list[HTMLPodcastsFeed] = []

    soup = BeautifulSoup(r.text, "html.parser")

    for feedcell_el in soup.select("a.feedcell[href]"):
        href = feedcell_el.attrs["href"]

        if href == "/uploads":
            continue

        overcast_url = OvercastFeedURL(_overcast_fm_url_from_path(href))

        if art_el := feedcell_el.select_one("img.art[src]"):
            art_url = OvercastCDNURL(art_el.attrs["src"])
        else:
            art_url = OvercastCDNURL("")

        title: str = ""
        if title_el := feedcell_el.select_one(".titlestack > .title"):
            title = title_el.text.strip()

        has_unplayed_episodes = (
            True if feedcell_el.select_one(".unplayed_indicator") else False
        )

        feed = HTMLPodcastsFeed(
            fetched_at=fetched_at,
            overcast_url=overcast_url,
            art_url=art_url,
            title=title,
            has_unplayed_episodes=has_unplayed_episodes,
        )
        feed._validate()
        feeds.append(feed)

    if len(feeds) == 0:
        logger.error("No feeds found")

    return feeds


@dataclass
class HTMLPodcastFeed:
    fetched_at: datetime
    title: str
    overcast_url: OvercastFeedURL
    overcast_uri: OvercastAppURI
    art_url: OvercastCDNURL
    episodes: list["HTMLPodcastEpisode"]

    @property
    def item_id(self) -> OvercastFeedItemID:
        return _extract_feed_id_from_art_url(self.art_url)

    @property
    def is_private(self) -> bool:
        return self.overcast_url.startswith("https://overcast.fm/p")

    def _validate(self) -> None:
        try:
            assert self.item_id
            assert self.title
            assert len(self.episodes) > 0
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


@dataclass
class HTMLPodcastEpisode:
    fetched_at: datetime
    overcast_url: OvercastEpisodeURL
    title: str
    description: str
    date_published: date
    duration: timedelta | None
    is_played: bool | None
    in_progress: bool | None
    download_state: Literal["new"] | Literal["deleted"]

    @property
    def is_new(self) -> bool:
        return self.download_state == "new"

    @property
    def is_deleted(self) -> bool:
        return self.download_state == "deleted"

    @property
    def date_published_datetime(self) -> datetime:
        return datetime.combine(
            self.date_published,
            datetime.min.time(),
            _SERVER_TZINFO,
        )

    def _validate(self) -> None:
        try:
            assert self.title, self.title
            assert self.date_published <= date.today(), self.date_published
            assert self.download_state is not None, "unknown download state"
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def fetch_podcast(session: Session, feed_url: OvercastFeedURL) -> HTMLPodcastFeed:
    expires_at_key = f"podcast:{feed_url}:expires_at"
    expires_in: timedelta = timedelta(days=1)
    if cache_expires_at := session.lru_cache[expires_at_key]:
        assert isinstance(
            cache_expires_at, datetime
        ), f"Invalid expires_at: {cache_expires_at}"
        # TODO: Drop log level
        logger.info(
            "Loaded '%s' expires at: %s, but not used", feed_url, cache_expires_at
        )
        # expires_in = cache_expires_at - datetime.now()

    r = _request(
        session=session,
        url=feed_url,
        controller="podcast",
        accept="text/html",
        response_expires_in=expires_in,
    )
    fetched_at = requests_cache.response_date(r)

    soup = BeautifulSoup(r.text, "html.parser")

    overcast_uri: str = ""
    for meta_el in soup.select("meta[name=apple-itunes-app]"):
        content = meta_el["content"]
        if isinstance(content, str) and content.startswith("app-id=888422857"):
            overcast_uri = content.removeprefix("app-id=888422857, app-argument=")

    feed_title: str = ""
    if title_el := soup.select_one("h2.centertext"):
        feed_title = title_el.text.strip()

    episodes: list[HTMLPodcastEpisode] = []

    for episodecell_el in soup.select("a.extendedepisodecell[href]"):
        href: str = episodecell_el.attrs["href"]
        episode_url = OvercastEpisodeURL(_overcast_fm_url_from_path(href))

        title: str = ""
        if title_el := episodecell_el.select_one(".title"):
            title = title_el.text.strip()

        download_state: Literal["new"] | Literal["deleted"] | None = None
        class_name = episodecell_el.attrs["class"]
        if "userdeletedepisode" in class_name:
            download_state = "deleted"
        elif "usernewepisode" in class_name:
            download_state = "new"
        else:
            assert False, f"Unknown download state: {class_name}"

        if caption2_el := episodecell_el.select_one(".caption2"):
            caption_result = parse_episode_caption_text(caption2_el.text)
        assert caption_result

        is_played: bool | None = None
        if "usernewepisode" in class_name:
            is_played = False
        elif caption_result.is_played is not None:
            is_played = caption_result.is_played
        else:
            is_played = None

        description: str = ""
        if description_el := episodecell_el.select_one(".lighttext"):
            description = description_el.text.strip()

        episode = HTMLPodcastEpisode(
            fetched_at=fetched_at,
            overcast_url=episode_url,
            title=title,
            description=description,
            date_published=caption_result.date_published,
            duration=caption_result.duration,
            is_played=is_played,
            in_progress=caption_result.in_progress,
            download_state=download_state,
        )
        episode._validate()
        episodes.append(episode)

    if img_el := soup.select_one("img.fullart[src]"):
        art_url = OvercastCDNURL(img_el.attrs["src"])
    else:
        art_url = OvercastCDNURL("")

    feed = HTMLPodcastFeed(
        fetched_at=fetched_at,
        overcast_url=feed_url,
        overcast_uri=OvercastAppURI(overcast_uri),
        art_url=art_url,
        title=feed_title,
        episodes=episodes,
    )
    feed._validate()

    default_expires_at = datetime.now(timezone.utc) + timedelta(days=1)
    expires_at: datetime = episodes[0].date_published_datetime + (
        _mean_date_published_interval(episodes) / 2
    )
    expires_at = max(expires_at, default_expires_at)
    # TODO: Drop log level
    logger.info("Setting '%s' expires at: %s", feed_url, expires_at)
    session.lru_cache[expires_at_key] = expires_at

    return feed


def _mean_date_published_interval(episodes: list[HTMLPodcastEpisode]) -> timedelta:
    dates: list[date] = [episode.date_published for episode in episodes]
    tds: list[timedelta] = [a - b for a, b in zip(dates, dates[1:])]
    mean_interval = sum(tds, timedelta()) / len(tds)
    assert mean_interval >= timedelta(seconds=0)
    return mean_interval


@dataclass
class CaptionResult:
    date_published: date
    duration: timedelta | None
    is_played: bool | None = None
    in_progress: bool | None = None


def parse_episode_caption_text(text: str) -> CaptionResult:
    text = text.strip()
    parts = text.split(" • ", 2)
    assert len(parts) >= 1, text

    duration: timedelta | None = None
    in_progress: bool | None
    is_played: bool | None

    date_published = dateutil.parser.parse(parts[0]).date()

    if len(parts) == 2 and parts[1] == "played":
        in_progress = False
        is_played = True

    elif len(parts) == 2 and parts[1].endswith("left"):
        in_progress = True
        is_played = False

    elif len(parts) == 2 and parts[1].startswith("at "):
        in_progress = True
        is_played = False

    elif len(parts) == 2:
        duration = _parse_duration(parts[1])
        in_progress = False
        is_played = False

    elif len(parts) == 1:
        in_progress = None
        is_played = None

    else:
        logger.warning("Unknown caption2 format: %s", text)

    return CaptionResult(
        date_published=date_published,
        duration=duration,
        is_played=is_played,
        in_progress=in_progress,
    )


@dataclass
class HTMLEpisode:
    fetched_at: datetime
    overcast_url: OvercastEpisodeURL
    overcast_uri: OvercastAppURI
    feed_art_url: OvercastCDNURL
    podcast_overcast_url: OvercastFeedURL
    title: str
    description: str
    date_published: date
    audio_url: HTTPURL
    download_state: Literal["new"] | Literal["existing"]

    @property
    def is_new(self) -> bool:
        return self.download_state == "new"

    @property
    def is_existing(self) -> bool:
        return self.download_state == "existing"

    @property
    def item_id(self) -> OvercastEpisodeItemID:
        return OvercastEpisodeItemID(
            int(self.overcast_uri.removeprefix("overcast:///"))
        )

    @property
    def feed_item_id(self) -> OvercastFeedItemID:
        return _extract_feed_id_from_art_url(self.feed_art_url)

    @property
    def date_published_datetime(self) -> datetime:
        return datetime.combine(
            self.date_published,
            datetime.min.time(),
            _SERVER_TZINFO,
        )

    def _validate(self) -> None:
        try:
            assert self.item_id, self.item_id
            assert self.feed_item_id, self.feed_art_url
            assert self.title, self.title
            assert self.date_published <= date.today(), self.date_published
            assert "#" not in self.audio_url, self.audio_url
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def fetch_episode(session: Session, episode_url: OvercastEpisodeURL) -> HTMLEpisode:
    r = _request(
        session=session,
        url=episode_url,
        controller="episode",
        accept="text/html",
        response_expires_in=timedelta(days=30),
    )
    fetched_at = requests_cache.response_date(r)

    soup = BeautifulSoup(r.text, "html.parser")

    overcast_uri: str = ""
    if meta_el := soup.select_one("meta[name=apple-itunes-app]"):
        content: str = meta_el.attrs["content"]
        if content.startswith("app-id=888422857"):
            overcast_uri = content.removeprefix("app-id=888422857, app-argument=")

    if img_el := soup.select_one("img.fullart[src]"):
        art_url = OvercastCDNURL(img_el.attrs["src"])
    else:
        art_url = OvercastCDNURL("")

    audio_url: str = ""
    if meta_el := soup.select_one("meta[name='twitter:player:stream']"):
        audio_url = meta_el.attrs["content"]
        audio_url = audio_url.split("#", 1)[0]

    if a_el := soup.select_one(".centertext > h3 > a[href]"):
        href = a_el.attrs["href"]
        podcast_overcast_url = OvercastFeedURL(_overcast_fm_url_from_path(href))
    else:
        podcast_overcast_url = OvercastFeedURL("")

    title: str = ""
    if title_el := soup.select_one(".centertext > h2"):
        title = title_el.text.strip()

    description: str = ""
    if description_el := soup.select_one("meta[name='og:description']"):
        description = description_el.attrs["content"]

    date_published: date | None = None
    if div_el := soup.select_one(".centertext > div"):
        date_published = dateutil.parser.parse(div_el.text).date()
    assert date_published

    download_state: Literal["new"] | Literal["existing"] | None = None
    if soup.select_one(".new_episode_for_user"):
        download_state = "new"
    elif soup.select_one(".existing_episode_for_user"):
        download_state = "existing"
    else:
        assert False, "Unknown download state"

    episode = HTMLEpisode(
        fetched_at=fetched_at,
        overcast_url=episode_url,
        overcast_uri=OvercastAppURI(overcast_uri),
        feed_art_url=art_url,
        podcast_overcast_url=podcast_overcast_url,
        title=title,
        description=description,
        date_published=date_published,
        audio_url=HTTPURL(audio_url),
        download_state=download_state,
    )
    episode._validate()
    return episode


def _fetch_audio_duration(url: HTTPURL) -> timedelta | None:
    response = requests.get(str(url), allow_redirects=True, headers=_SAFARI_HEADERS)
    if not response.ok:
        logger.warning("Failed to fetch audio: %s", url)
        return None
    io = BytesIO(response.content)
    try:
        f = mutagen.File(io)  # type: ignore
    except Exception:
        logger.error("Failed to parse audio: %s", url)
        return None
    if f is None:
        logger.error("Failed to parse audio: %s", url)
        return None
    seconds = int(f.info.length)
    return timedelta(seconds=seconds)


def fetch_audio_duration(session: Session, url: HTTPURL) -> timedelta | None:
    def _inner() -> timedelta | None:
        if session.requests_session._offline:
            raise requests_cache.OfflineError()
        elif duration := _fetch_audio_duration(url):
            return duration
        else:
            return None

    key = f"fetch_audio_duration:v4:{url}"
    return session.lru_cache.get(key, _inner)


@dataclass
class AccountExport:
    fetched_at: datetime
    feeds: list["ExportFeed"]


def export_account_data(session: Session) -> AccountExport:
    r = _request(
        session,
        url=OvercastURL("https://overcast.fm/account/export_opml"),
        controller="export",
        accept="application/xml",
        response_expires_in=timedelta(days=5),
    )
    fetched_at = requests_cache.response_date(r)

    soup = BeautifulSoup(r.content, "xml")
    return AccountExport(
        fetched_at=fetched_at,
        feeds=_opml_feeds(soup, fetched_at=fetched_at),
    )


@dataclass
class ExportFeed:
    fetched_at: datetime
    item_id: OvercastFeedItemID
    title: str
    xml_url: HTTPURL
    html_url: HTTPURL
    added_at: datetime

    def _validate(self) -> None:
        try:
            assert self.title, self.title
            assert self.added_at.tzinfo, "added date must be timezone-aware"
            assert self.added_at < datetime.now(timezone.utc), self.added_at
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def _opml_feeds(soup: BeautifulSoup, fetched_at: datetime) -> list[ExportFeed]:
    feeds: list[ExportFeed] = []

    for outline in soup.select("outline[text='feeds'] > outline[type='rss']"):
        item_id: int = int(outline.attrs["overcastId"])
        title: str = outline.attrs["title"]
        html_url: str = outline.attrs["htmlUrl"]
        xml_url: str = outline.attrs["xmlUrl"]
        added_at = dateutil.parser.parse(outline.attrs["overcastAddedDate"])

        feed = ExportFeed(
            fetched_at=fetched_at,
            item_id=OvercastFeedItemID(item_id),
            title=title,
            xml_url=HTTPURL(xml_url),
            html_url=HTTPURL(html_url),
            added_at=added_at,
        )
        feed._validate()
        feeds.append(feed)

    logger.debug("Found %d feeds in export", len(feeds))
    return feeds


@dataclass
class AccountExtendedExport:
    fetched_at: datetime
    playlists: list["ExtendedExportPlaylist"]
    feeds: list["ExtendedExportFeed"]


def export_account_extended_data(session: Session) -> AccountExtendedExport:
    r = _request(
        session,
        url=OvercastURL("https://overcast.fm/account/export_opml/extended"),
        controller="export",
        accept="application/xml",
        response_expires_in=timedelta(days=5),
    )
    fetched_at = requests_cache.response_date(r)

    soup = BeautifulSoup(r.content, "xml")
    return AccountExtendedExport(
        fetched_at=fetched_at,
        playlists=_opml_extended_playlists(soup, fetched_at=fetched_at),
        feeds=_opml_extended_feeds(soup, fetched_at=fetched_at),
    )


_PLAYLIST_SORTING_TYPE = Literal[
    "chronological",
    "chronological-by-podcast",
    "reverse-chronological",
    "reverse-chronological-by-podcast",
]

_PLAYLIST_SORTING_VALUES: list[str] = [
    "chronological",
    "chronological-by-podcast",
    "reverse-chronological",
    "reverse-chronological-by-podcast",
]


@dataclass
class ExtendedExportPlaylist:
    fetched_at: datetime
    title: str
    smart: bool
    sorting: _PLAYLIST_SORTING_TYPE
    episode_ids: list[OvercastEpisodeItemID]

    def _validate(self) -> None:
        try:
            assert self.title, self.title
            assert self.sorting in _PLAYLIST_SORTING_VALUES, self.sorting
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def _opml_extended_playlists(
    soup: BeautifulSoup, fetched_at: datetime
) -> list[ExtendedExportPlaylist]:
    playlists: list[ExtendedExportPlaylist] = []

    for outline in soup.select(
        "outline[text='playlists'] > outline[type='podcast-playlist']"
    ):
        title: str = outline.attrs["title"]
        smart: bool = outline.attrs["smart"] == "1"
        sorting = cast(_PLAYLIST_SORTING_TYPE, outline.attrs["sorting"])

        episode_ids: list[OvercastEpisodeItemID] = []
        if include_episode_ids_str := outline.attrs.get("includeEpisodeIds", ""):
            episode_ids = [
                OvercastEpisodeItemID(int(id))
                for id in include_episode_ids_str.split(",")
            ]
        elif sorted_episode_ids_str := outline.attrs.get("sortedEpisodeIds", ""):
            episode_ids = [
                OvercastEpisodeItemID(int(id))
                for id in sorted_episode_ids_str.split(",")
            ]

        playlist = ExtendedExportPlaylist(
            fetched_at=fetched_at,
            title=title,
            smart=smart,
            sorting=sorting,
            episode_ids=episode_ids,
        )
        playlist._validate()
        playlists.append(playlist)

    logger.debug("Found %d playlists in extended export", len(playlists))
    return playlists


@dataclass
class ExtendedExportFeed:
    fetched_at: datetime
    item_id: OvercastFeedItemID
    title: str
    xml_url: HTTPURL
    html_url: HTTPURL
    added_at: datetime
    is_subscribed: bool
    episodes: list["ExtendedExportEpisode"]

    def _validate(self) -> None:
        try:
            assert self.title, self.title
            assert self.added_at.tzinfo, "added date must be timezone-aware"
            assert self.added_at < datetime.now(timezone.utc), self.added_at
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def _opml_extended_feeds(
    soup: BeautifulSoup, fetched_at: datetime
) -> list[ExtendedExportFeed]:
    feeds: list[ExtendedExportFeed] = []

    for outline in soup.select("outline[text='feeds'] > outline[type='rss']"):
        item_id = OvercastFeedItemID(int(outline.attrs["overcastId"]))
        title: str = outline.attrs["title"]
        html_url = HTTPURL(outline.attrs["htmlUrl"])
        xml_url = HTTPURL(outline.attrs["xmlUrl"])
        added_at = dateutil.parser.parse(outline.attrs["overcastAddedDate"])
        is_subscribed: bool = outline.attrs.get("subscribed", "0") == "1"

        feed = ExtendedExportFeed(
            fetched_at=fetched_at,
            item_id=item_id,
            title=title,
            xml_url=xml_url,
            html_url=html_url,
            added_at=added_at,
            is_subscribed=is_subscribed,
            episodes=_opml_extended_episode(outline, fetched_at=fetched_at),
        )
        feed._validate()
        feeds.append(feed)

    feed_count = len(feeds)
    episode_count = sum(len(feed.episodes) for feed in feeds)
    logger.debug(
        "Found %d feeds and %d episodes in extended export", feed_count, episode_count
    )
    return feeds


@dataclass
class ExtendedExportEpisode:
    fetched_at: datetime
    date_published: datetime
    title: str
    item_id: OvercastEpisodeItemID
    url: HTTPURL
    overcast_url: OvercastEpisodeURL
    enclosure_url: HTTPURL
    user_updated_at: datetime
    user_deleted: bool
    progress: int
    is_played: bool

    @property
    def is_deleted(self) -> bool:
        return True if self.user_deleted else False

    def _validate(self) -> None:
        try:
            assert self.title, self.title
            assert self.date_published.tzinfo, "published date must be timezone-aware"
            assert self.user_updated_at.tzinfo, "updated date must be timezone-aware"
            assert self.date_published <= datetime.now(
                timezone.utc
            ), self.date_published
            assert self.user_updated_at < datetime.now(
                timezone.utc
            ), self.user_updated_at
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def _opml_extended_episode(
    rss_outline: Tag, fetched_at: datetime
) -> list[ExtendedExportEpisode]:
    episodes: list[ExtendedExportEpisode] = []

    for outline in rss_outline.select("outline[type='podcast-episode']"):
        overcast_url = OvercastEpisodeURL(outline.attrs["overcastUrl"])
        item_id = OvercastEpisodeItemID(int(outline.attrs["overcastId"]))
        date_published = dateutil.parser.parse(outline.attrs["pubDate"])
        title: str = outline.attrs["title"]
        url = HTTPURL(outline.attrs["url"])
        enclosure_url = HTTPURL(outline.attrs["enclosureUrl"])
        user_updated_at = dateutil.parser.parse(outline.attrs["userUpdatedDate"])
        user_deleted: bool = outline.attrs.get("userDeleted", "0") == "1"
        progress: int = int(outline.attrs.get("progress", "0"))
        is_played: bool = outline.attrs.get("played", "0") == "1"

        episode = ExtendedExportEpisode(
            fetched_at=fetched_at,
            item_id=item_id,
            date_published=date_published,
            title=title,
            url=url,
            overcast_url=overcast_url,
            enclosure_url=enclosure_url,
            user_updated_at=user_updated_at,
            user_deleted=user_deleted,
            progress=progress,
            is_played=is_played,
        )
        episode._validate()
        episodes.append(episode)

    return episodes


_CONTROLER = Literal["index", "podcast", "episode", "export"]


def last_request_date(session: Session, url: OvercastURL) -> datetime:
    request = requests.Request(method="GET", url=url, headers={"Accept": "text/html"})
    if cached_response := session.requests_session.cached_response(request):
        return requests_cache.response_date(cached_response)
    return datetime.min


def _request(
    session: Session,
    url: OvercastURL,
    controller: _CONTROLER,
    accept: str | None,
    response_expires_in: timedelta,
) -> requests.Response:
    path = url.removeprefix("https://overcast.fm")
    try:
        response, is_cached = session.requests_session.get(
            path=path,
            request_accept=accept,
            response_expires_in=response_expires_in,
        )
    except requests.HTTPError as e:
        if e.response.status_code == 429:
            logger.critical("Rate limited")
            raise RatedLimitedError()
        else:
            raise e

    if "Log In" in response.text:
        logger.critical("Received logged out page")
        raise LoggedOutError()

    return response


def _extract_feed_id_from_art_url(url: OvercastCDNURL) -> OvercastFeedItemID:
    """
    Extract numeric feed-id from an Overcast CDN artwork URL.
    e.g. "https://public.overcast-cdn.com/art/126160?v198"
    """
    m = re.match(r"https://public.overcast-cdn.com/art/(\d+)", url)
    assert m, f"Couldn't extract feed-id from art URL: {url}"
    id = int(m.group(1))
    return OvercastFeedItemID(id)


def _parse_duration(text: str) -> timedelta:
    text = text.strip()
    assert text.endswith(" min"), text
    text = text[:-4]
    minutes = int(text)
    return timedelta(minutes=minutes)
