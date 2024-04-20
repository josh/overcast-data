import logging
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Literal, NewType
from urllib.parse import urlparse

import dateutil.parser
import mutagen
import requests
from bs4 import BeautifulSoup, Tag

import requests_cache
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


Session = requests_cache.Session


def session(cache_dir: Path, cookie: str, offline: bool = False) -> Session:
    headers = _SAFARI_HEADERS.copy()
    headers["Cookie"] = f"o={cookie}; qr=-"

    return Session(
        cache_dir=cache_dir,
        base_url="https://overcast.fm",
        headers=headers,
        min_time_between_requests=timedelta(seconds=10),
        offline=offline,
    )


@dataclass
class HTMLPodcastsFeed:
    overcast_url: OvercastFeedURL
    art_url: OvercastCDNURL
    title: str
    has_unplayed_episodes: bool

    @property
    def is_private(self) -> bool:
        return self.overcast_url.startswith("https://overcast.fm/p")

    @property
    def item_id(self) -> OvercastFeedItemID:
        return _extract_feed_id_from_art_url(self.art_url)

    def _validate(self) -> None:
        try:
            assert self.art_url.startswith(
                "https://public.overcast-cdn.com"
            ), self.art_url
            assert self.item_id, self.art_url
            assert len(self.title) > 3
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def fetch_podcasts(session: Session) -> list[HTMLPodcastsFeed]:
    r = _request(
        session=session,
        url=OvercastURL("https://overcast.fm/podcasts"),
        accept="text/html",
        cache_expires=timedelta(hours=1),
    )

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
            assert self.item_id, self.art_url
            assert self.art_url.startswith(
                "https://public.overcast-cdn.com/"
            ), self.art_url
            assert len(self.title) > 3, self.title
            assert len(self.episodes) > 0
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


@dataclass
class HTMLPodcastEpisode:
    overcast_url: OvercastEpisodeURL
    title: str
    description: str
    pub_date: date
    duration: timedelta | None
    is_played: bool
    in_progress: bool
    download_state: Literal["new"] | Literal["deleted"] | None

    @property
    def is_new(self) -> bool:
        return self.download_state == "new"

    @property
    def is_deleted(self) -> bool:
        return self.download_state == "deleted"

    def _validate(self) -> None:
        try:
            assert len(self.title) > 3, self.title
            assert self.pub_date <= datetime.now().date(), self.pub_date
            assert (
                self.is_deleted != self.is_new
            ), "is_deleted and is_new can't be the same"
            assert self.download_state, "unknown download state"
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def fetch_podcast(session: Session, feed_url: OvercastFeedURL) -> HTMLPodcastFeed:
    r = _request(
        session=session,
        url=feed_url,
        accept="text/html",
        cache_expires=timedelta(hours=1),
    )

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

        if caption2_el := episodecell_el.select_one(".caption2"):
            caption_result = parse_episode_caption_text(caption2_el.text)
        assert caption_result

        description: str = ""
        if description_el := episodecell_el.select_one(".lighttext"):
            description = description_el.text.strip()

        episode = HTMLPodcastEpisode(
            overcast_url=episode_url,
            title=title,
            description=description,
            pub_date=caption_result.pub_date,
            duration=caption_result.duration,
            is_played=caption_result.is_played,
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
        overcast_url=feed_url,
        overcast_uri=OvercastAppURI(overcast_uri),
        art_url=art_url,
        title=feed_title,
        episodes=episodes,
    )
    feed._validate()
    return feed


@dataclass
class CaptionResult:
    pub_date: date
    duration: timedelta | None
    is_played: bool = False
    in_progress: bool = False


def parse_episode_caption_text(text: str) -> CaptionResult:
    text = text.strip()
    parts = text.split(" • ", 2)
    assert len(parts) >= 1, text

    duration: timedelta | None = None
    in_progress: bool = False
    is_played: bool = False

    pub_date = dateutil.parser.parse(parts[0]).date()

    if len(parts) == 2 and parts[1] == "played":
        is_played = True

    elif len(parts) == 2 and parts[1].endswith("left"):
        in_progress = False
        is_played = True

    elif len(parts) == 2 and parts[1].startswith("at "):
        in_progress = True
        is_played = True

    elif len(parts) == 2:
        duration = _parse_duration(parts[1])

    elif len(parts) == 1:
        pass

    else:
        logger.warning("Unknown caption2 format: %s", text)

    return CaptionResult(
        pub_date=pub_date,
        duration=duration,
        is_played=is_played,
        in_progress=in_progress,
    )


@dataclass
class HTMLEpisode:
    html_url: OvercastEpisodeURL
    overcast_uri: OvercastAppURI
    feed_art_url: OvercastCDNURL
    podcast_html_url: OvercastFeedURL
    title: str
    description: str
    date_published: date
    audio_url: HTTPURL

    @property
    def item_id(self) -> OvercastEpisodeItemID:
        return OvercastEpisodeItemID(
            int(self.overcast_uri.removeprefix("overcast:///"))
        )

    @property
    def feed_item_id(self) -> OvercastFeedItemID:
        return _extract_feed_id_from_art_url(self.feed_art_url)

    def _validate(self) -> None:
        try:
            assert self.html_url.startswith("https://overcast.fm/+"), self.html_url
            assert self.item_id, self.item_id
            assert self.feed_art_url.startswith(
                "https://public.overcast-cdn.com/"
            ), self.feed_art_url
            assert self.feed_item_id, self.feed_art_url
            assert len(self.title) > 3, self.title
            assert self.date_published <= datetime.now().date(), self.date_published
            assert self.audio_url.startswith("http"), self.audio_url
            assert "#" not in self.audio_url, self.audio_url
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def fetch_episode(session: Session, episode_url: OvercastEpisodeURL) -> HTMLEpisode:
    r = _request(
        session=session,
        url=episode_url,
        accept="text/html",
        cache_expires=timedelta(days=30),
    )

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
        podcast_html_url = OvercastFeedURL(_overcast_fm_url_from_path(href))
    else:
        podcast_html_url = OvercastFeedURL("")

    title: str = ""
    if title_el := soup.select_one("meta[name='og:title']"):
        title = title_el.attrs["content"]

    description: str = ""
    if description_el := soup.select_one("meta[name='og:description']"):
        description = description_el.attrs["content"]

    date_published: date | None = None
    if div_el := soup.select_one(".centertext > div"):
        date_published = dateutil.parser.parse(div_el.text).date()
    assert date_published

    episode = HTMLEpisode(
        html_url=episode_url,
        overcast_uri=OvercastAppURI(overcast_uri),
        feed_art_url=art_url,
        podcast_html_url=podcast_html_url,
        title=title,
        description=description,
        date_published=date_published,
        audio_url=HTTPURL(audio_url),
    )
    episode._validate()
    return episode


def _fetch_audio_duration(url: HTTPURL, max_bytes: int | None) -> timedelta | None:
    headers: dict[str, str] = {}
    if max_bytes:
        headers["Range"] = f"bytes=0-{max_bytes}"
    response = requests.get(str(url), allow_redirects=True, headers=headers)
    if not response.ok:
        logger.warning("Failed to fetch audio: %s", url)
        return None
    io = BytesIO(response.content)
    try:
        f = mutagen.File(io)  # type: ignore
    except Exception:
        logger.error("Failed to parse audio: %s, max-bytes: %i", url, max_bytes)
        return None
    seconds = f.info.length
    if seconds < 60:
        logger.error("Duration too short: %s", url)
        return None
    return timedelta(seconds=seconds)


def fetch_audio_duration(session: Session, url: HTTPURL) -> timedelta | None:
    def _inner() -> timedelta | None:
        if session._offline:
            raise requests_cache.OfflineError()
        elif duration := _fetch_audio_duration(url, max_bytes=1_000_000):
            return duration
        elif duration := _fetch_audio_duration(url, max_bytes=None):
            return duration
        else:
            return None

    key = f"fetch_audio_duration:v2:{url}"
    return session.simple_cache.get(key, _inner)


@dataclass
class AccountExport:
    feeds: list["ExportFeed"]


def export_account_data(session: Session) -> AccountExport:
    r = _request(
        session,
        url=OvercastURL("https://overcast.fm/account/export_opml"),
        accept="application/xml",
        cache_expires=timedelta(days=7),
    )

    soup = BeautifulSoup(r.content, "xml")
    return AccountExport(
        feeds=_opml_feeds(soup),
    )


@dataclass
class ExportFeed:
    item_id: OvercastFeedItemID
    title: str
    xml_url: HTTPURL
    html_url: HTTPURL
    added_at: datetime

    def _validate(self) -> None:
        try:
            assert self.item_id > 0, self.item_id
            assert len(self.title) > 3, self.title
            assert self.added_at < datetime.now(timezone.utc), self.added_at
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def _opml_feeds(soup: BeautifulSoup) -> list[ExportFeed]:
    feeds: list[ExportFeed] = []

    for outline in soup.select("outline[text='feeds'] > outline[type='rss']"):
        item_id: int = int(outline.attrs["overcastId"])
        title: str = outline.attrs["title"]
        html_url: str = outline.attrs["htmlUrl"]
        xml_url: str = outline.attrs["xmlUrl"]
        added_at = dateutil.parser.parse(outline.attrs["overcastAddedDate"])

        feed = ExportFeed(
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
    playlists: list["ExtendedExportPlaylist"]
    feeds: list["ExtendedExportFeed"]


def export_account_extended_data(session: Session) -> AccountExtendedExport:
    r = _request(
        session,
        url=OvercastURL("https://overcast.fm/account/export_opml/extended"),
        accept="application/xml",
        cache_expires=timedelta(days=7),
    )

    soup = BeautifulSoup(r.content, "xml")
    return AccountExtendedExport(
        playlists=_opml_extended_playlists(soup),
        feeds=_opml_extended_feeds(soup),
    )


@dataclass
class ExtendedExportPlaylist:
    title: str
    smart: bool
    sorting: str
    include_episode_ids: list[OvercastEpisodeItemID]

    def _validate(self) -> None:
        try:
            assert len(self.title) > 0, self.title
            assert self.sorting in ["chronological"], self.sorting
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def _opml_extended_playlists(soup: BeautifulSoup) -> list[ExtendedExportPlaylist]:
    playlists: list[ExtendedExportPlaylist] = []

    for outline in soup.select(
        "outline[text='playlists'] > outline[type='podcast-playlist']"
    ):
        title: str = outline.attrs["title"]
        smart: bool = outline.attrs["smart"] == "1"
        sorting: str = outline.attrs["sorting"]

        include_episode_ids: list[OvercastEpisodeItemID] = []
        if include_episode_ids_str := outline.attrs.get("includeEpisodeIds", ""):
            include_episode_ids = [
                OvercastEpisodeItemID(int(id))
                for id in include_episode_ids_str.split(",")
            ]

        playlist = ExtendedExportPlaylist(
            title=title,
            smart=smart,
            sorting=sorting,
            include_episode_ids=include_episode_ids,
        )
        playlist._validate()
        playlists.append(playlist)

    logger.debug("Found %d playlists in export", len(playlists))
    return playlists


@dataclass
class ExtendedExportFeed:
    item_id: OvercastFeedItemID
    title: str
    xml_url: HTTPURL
    html_url: HTTPURL
    added_at: datetime
    is_subscribed: bool
    episodes: list["ExtendedExportEpisode"]

    def _validate(self) -> None:
        try:
            assert self.item_id > 0
            assert len(self.title) > 3, self.title
            assert self.added_at < datetime.now(timezone.utc), self.added_at
            assert len(self.episodes) > 0
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def _opml_extended_feeds(soup: BeautifulSoup) -> list[ExtendedExportFeed]:
    feeds: list[ExtendedExportFeed] = []

    for outline in soup.select("outline[text='feeds'] > outline[type='rss']"):
        item_id = OvercastFeedItemID(int(outline.attrs["overcastId"]))
        title: str = outline.attrs["title"]
        html_url = HTTPURL(outline.attrs["htmlUrl"])
        xml_url = HTTPURL(outline.attrs["xmlUrl"])
        added_at = dateutil.parser.parse(outline.attrs["overcastAddedDate"])
        is_subscribed: bool = outline.attrs["subscribed"] == "1"

        feed = ExtendedExportFeed(
            item_id=item_id,
            title=title,
            xml_url=xml_url,
            html_url=html_url,
            added_at=added_at,
            is_subscribed=is_subscribed,
            episodes=_opml_extended_episode(outline),
        )
        feed._validate()
        feeds.append(feed)

    logger.debug("Found %d feeds in export", len(feeds))
    return feeds


@dataclass
class ExtendedExportEpisode:
    pub_date: date
    title: str
    item_id: OvercastEpisodeItemID
    url: HTTPURL
    overcast_url: OvercastEpisodeURL
    enclosure_url: HTTPURL
    user_updated_at: datetime
    user_deleted: bool
    played: bool

    def _validate(self) -> None:
        try:
            assert len(self.title) > 3, self.title
            assert self.pub_date <= datetime.now().date(), self.pub_date
            assert self.user_updated_at < datetime.now(
                timezone.utc
            ), self.user_updated_at
        except AssertionError as e:
            logger.error(e)
            if _RAISE_VALIDATION_ERRORS:
                raise e


def _opml_extended_episode(rss_outline: Tag) -> list[ExtendedExportEpisode]:
    episodes: list[ExtendedExportEpisode] = []

    for outline in rss_outline.select("outline[type='podcast-episode']"):
        overcast_url = OvercastEpisodeURL(outline.attrs["overcastUrl"])
        item_id = OvercastEpisodeItemID(int(outline.attrs["overcastId"]))
        pub_date = dateutil.parser.parse(outline.attrs["pubDate"]).date()
        title: str = outline.attrs["title"]
        url = HTTPURL(outline.attrs["url"])
        enclosure_url = HTTPURL(outline.attrs["enclosureUrl"])
        user_updated_at = dateutil.parser.parse(outline.attrs["userUpdatedDate"])
        user_deleted: bool = outline.attrs.get("userDeleted", "0") == "1"
        played: bool = outline.attrs.get("played", "0") == "1"

        episode = ExtendedExportEpisode(
            item_id=item_id,
            pub_date=pub_date,
            title=title,
            url=url,
            overcast_url=overcast_url,
            enclosure_url=enclosure_url,
            user_updated_at=user_updated_at,
            user_deleted=user_deleted,
            played=played,
        )
        episode._validate()
        episodes.append(episode)

    logger.debug("Found %d episodes in export", len(episodes))
    return episodes


def _request(
    session: Session,
    url: OvercastURL,
    accept: str | None,
    cache_expires: timedelta,
) -> requests.Response:
    path = url.removeprefix("https://overcast.fm")
    try:
        response = session.get(path=path, accept=accept, cache_expires=cache_expires)
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
