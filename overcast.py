import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Iterator, Literal, TypeVar

import dateutil.parser
import mutagen
import requests
from bs4 import BeautifulSoup, Tag

import requests_cache

logger = logging.getLogger("overcast")


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
    html_url: str
    art_url: str
    title: str
    has_unplayed_episodes: bool

    # TODO: Deprecate this
    @property
    def id(self) -> str:
        return self.html_url.removeprefix("https://overcast.fm/")

    @property
    def is_private(self) -> bool:
        return self.id.startswith("p")

    # TODO: Maybe use art_id instead of item_id

    @property
    def item_id(self) -> int | None:
        if self.id.startswith("p"):
            return int(self.id.removeprefix("p").split("-", 1)[0])
        return None

    @property
    def art_id(self) -> int:
        if m := re.search(r"(?<=/art/)\d+", self.art_url):
            return int(m.group(0))
        return 0

    def _validate(self) -> None:
        assert self.html_url.startswith("https://overcast.fm/"), self.html_url
        assert not self.id.startswith("/"), self.id
        if self.is_private:
            assert self.item_id, self.id
            assert (
                self.art_id == self.item_id
            ), f"art_url: {self.art_url}, id: {self.id}"
        assert self.art_url.startswith("https://public.overcast-cdn.com"), self.art_url
        assert self.art_id, self.art_url
        assert len(self.title) > 3


def fetch_podcasts(session: Session) -> list[HTMLPodcastsFeed]:
    r = _request(
        session=session,
        path="/podcasts",
        accept="text/html",
        cache_expires=timedelta(hours=1),
    )

    feeds: list[HTMLPodcastsFeed] = []

    soup = BeautifulSoup(r.text, "html.parser")

    for feedcell_el in soup.select("a.feedcell[href]"):
        href = feedcell_el["href"]
        assert isinstance(href, str)

        if href == "/uploads":
            continue

        art_url: str = ""
        if art_el := feedcell_el.select_one("img.art[src]"):
            art_url = art_el.attrs["src"]

        title: str = ""
        if title_el := feedcell_el.select_one(".titlestack > .title"):
            title = title_el.text.strip()

        has_unplayed_episodes = (
            True if feedcell_el.select_one(".unplayed_indicator") else False
        )

        feed = HTMLPodcastsFeed(
            html_url=f"https://overcast.fm{href}",
            art_url=art_url,
            title=title,
            has_unplayed_episodes=has_unplayed_episodes,
        )
        feed._validate()
        logger.debug("%s", feed)
        feeds.append(feed)

    if len(feeds) == 0:
        logger.error("No feeds found")

    return feeds


@dataclass
class HTMLPodcastFeed:
    title: str
    html_url: str
    overcast_uri: str
    art_url: str
    delete_url: str
    episodes: list["HTMLPodcastEpisode"]

    # TODO: Which is more reliable, item id, art id or delete id?

    @property
    def item_id(self) -> int:
        return int(self.overcast_uri.removeprefix("overcast:///F").split("-", 1)[0])

    @property
    def art_id(self) -> int:
        if m := re.search(r"(?<=/art/)\d+", self.art_url):
            return int(m.group(0))
        return 0

    @property
    def delete_action_id(self) -> int:
        if m := re.search(r"(?<=/delete/)\d+", self.delete_url):
            return int(m.group(0))
        return 0

    @property
    def is_private(self) -> bool:
        return self.html_url.startswith("https://overcast.fm/p")

    def _validate(self) -> None:
        assert self.html_url.startswith("https://overcast.fm/"), self.html_url
        assert self.overcast_uri.startswith("overcast:///"), self.overcast_uri
        assert self.item_id, self.overcast_uri
        assert self.art_url.startswith("https://public.overcast-cdn.com/"), self.art_url
        assert self.art_id, self.art_url
        assert self.delete_url.startswith("/podcasts/delete/"), self.delete_url
        assert self.delete_action_id, self.delete_url
        assert self.item_id == self.art_id
        assert self.item_id == self.delete_action_id
        assert len(self.title) > 3, self.title
        assert len(self.episodes) > 0


@dataclass
class HTMLPodcastEpisode:
    html_url: str
    title: str
    description: str
    pub_date: date
    duration: timedelta | None
    is_played: bool
    in_progress: bool
    download_state: Literal["new"] | Literal["deleted"] | None

    @property
    def id(self) -> str:
        return self.html_url.removeprefix("https://overcast.fm/")

    @property
    def is_new(self) -> bool:
        return self.download_state == "new"

    @property
    def is_deleted(self) -> bool:
        return self.download_state == "deleted"

    def _validate(self) -> None:
        assert self.html_url.startswith("https://overcast.fm/+"), self.html_url
        assert not self.id.startswith("/"), self.id
        if self.id.startswith("p"):
            assert len(self.id) == 15, self.id
            assert "-" in self.id, self.id
        assert len(self.title) > 3, self.title
        assert self.pub_date <= datetime.now().date(), self.pub_date
        assert self.is_deleted != self.is_new, "is_deleted and is_new can't be the same"
        assert self.download_state, "unknown download state"


def fetch_podcast(session: Session, feed_id: str) -> HTMLPodcastFeed:
    r = _request(
        session=session,
        path=f"/{feed_id}",
        accept="text/html",
        cache_expires=timedelta(hours=1),
    )

    soup = BeautifulSoup(r.text, "html.parser")

    overcast_uri: str = ""
    for meta_el in soup.select("meta[name=apple-itunes-app]"):
        content = meta_el["content"]
        if isinstance(content, str) and content.startswith("app-id=888422857"):
            overcast_uri = content.removeprefix("app-id=888422857, app-argument=")

    art_url: str = ""
    if img_el := soup.select_one("img.fullart[src]"):
        art_url = img_el.attrs["src"]

    delete_url: str = ""
    if delete_el := soup.select_one("form#deletepodcastform[action]"):
        delete_url = delete_el.attrs["action"]

    feed_title: str = ""
    if title_el := soup.select_one("h2.centertext"):
        feed_title = title_el.text.strip()

    episodes: list[HTMLPodcastEpisode] = []

    for episodecell_el in soup.select("a.extendedepisodecell[href]"):
        href: str = episodecell_el.attrs["href"]

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
            html_url=f"https://overcast.fm{href}",
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

    feed = HTMLPodcastFeed(
        html_url=f"https://overcast.fm/{feed_id}",
        overcast_uri=overcast_uri,
        art_url=art_url,
        delete_url=delete_url,
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
    html_url: str
    overcast_uri: str
    feed_art_url: str
    podcast_html_url: str
    title: str
    description: str
    date_published: date
    audio_url: str

    # TODO: Maybe deprecate this
    @property
    def id(self) -> str:
        return self.html_url.removeprefix("https://overcast.fm/")

    @property
    def item_id(self) -> int:
        return int(self.overcast_uri.removeprefix("overcast:///"))

    # TODO: I think this is actually podcast numeric id
    @property
    def feed_art_id(self) -> int:
        if m := re.search(r"(?<=/art/)\d+", self.feed_art_url):
            return int(m.group(0))
        return 0

    # TODO: deprecate this
    @property
    def podcast_id(self) -> str:
        return self.podcast_html_url.removeprefix("https://overcast.fm/")

    def _validate(self) -> None:
        assert self.html_url.startswith("https://overcast.fm/+"), self.html_url
        assert self.id.startswith("+"), self.id
        assert self.item_id, self.item_id
        assert self.overcast_uri.startswith("overcast:///"), self.overcast_uri
        assert self.feed_art_url.startswith(
            "https://public.overcast-cdn.com/"
        ), self.feed_art_url
        assert self.feed_art_id, self.feed_art_url
        assert len(self.title) > 3, self.title
        assert self.date_published <= datetime.now().date(), self.date_published
        assert self.audio_url.startswith("http"), self.audio_url
        assert "#" not in self.audio_url, self.audio_url


def fetch_episode(session: Session, episode_id: str) -> HTMLEpisode:
    assert episode_id.startswith("+"), episode_id

    r = _request(
        session=session,
        path=f"/{episode_id}",
        accept="text/html",
        cache_expires=timedelta(days=30),
    )

    soup = BeautifulSoup(r.text, "html.parser")

    overcast_uri: str = ""
    if meta_el := soup.select_one("meta[name=apple-itunes-app]"):
        content: str = meta_el.attrs["content"]
        if content.startswith("app-id=888422857"):
            overcast_uri = content.removeprefix("app-id=888422857, app-argument=")

    art_url: str = ""
    if img_el := soup.select_one("img.fullart[src]"):
        art_url = img_el.attrs["src"]

    audio_url: str = ""
    if meta_el := soup.select_one("meta[name='twitter:player:stream']"):
        audio_url = meta_el.attrs["content"]
        audio_url = audio_url.split("#", 1)[0]

    podcast_html_url: str = ""
    if a_el := soup.select_one(".centertext > h3 > a[href]"):
        href: str = a_el.attrs["href"]
        podcast_html_url = f"https://overcast.fm{href}"

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
        html_url=f"https://overcast.fm/{episode_id}",
        overcast_uri=overcast_uri,
        feed_art_url=art_url,
        podcast_html_url=podcast_html_url,
        title=title,
        description=description,
        date_published=date_published,
        audio_url=audio_url,
    )
    episode._validate()
    return episode


def _fetch_audio_duration(url: str, max_bytes: int | None) -> timedelta | None:
    headers: dict[str, str] = {}
    if max_bytes:
        headers["Range"] = f"bytes=0-{max_bytes}"
    response = requests.get(url, allow_redirects=True, headers=headers)
    if not response.ok:
        logger.warning("Failed to fetch audio: %s", url)
        return None
    io = BytesIO(response.content)
    try:
        f = mutagen.File(io)  # type: ignore
    except Exception:
        logger.error("Failed to parse audio: %s, max-bytes: %i", url, max_bytes)
        return None
    if not f:
        return None
    seconds = f.info.length
    if seconds < 60:
        logger.error("Duration too short: %s", url)
        return None
    return timedelta(seconds=seconds)


def fetch_audio_duration(session: Session, url: str) -> timedelta | None:
    def _inner() -> timedelta | None:
        if session._offline:
            raise requests_cache.OfflineError()
        elif duration := _fetch_audio_duration(url, max_bytes=1_000_000):
            return duration
        elif duration := _fetch_audio_duration(url, max_bytes=None):
            return duration
        else:
            return None

    key = f"fetch_audio_duration:v1:{url}"
    return session.simple_cache.get(key, _inner)


@dataclass
class ExtendedExportPlaylist:
    title: str
    smart: bool
    sorting: str

    def _validate(self) -> None:
        assert len(self.title) > 3, self.title


@dataclass
class ExtendedExportEpisode:
    id: str
    item_id: int
    pub_date: date
    title: str
    url: str
    overcast_url: str
    enclosure_url: str
    user_updated_at: datetime
    user_deleted: bool
    played: bool

    def _validate(self) -> None:
        assert not self.id.startswith("/"), self.id
        if self.id.startswith("p"):
            assert len(self.id) == 15, self.id
            assert "-" in self.id, self.id

        assert len(self.title) > 3, self.title
        assert self.pub_date <= datetime.now().date(), self.pub_date
        assert self.user_updated_at < datetime.now(timezone.utc), self.user_updated_at
        assert self.enclosure_url.startswith("http"), self.enclosure_url


@dataclass
class ExtendedExportFeed:
    item_id: int
    title: str
    xml_url: str
    html_url: str
    added_at: datetime
    is_subscribed: bool
    episodes: list[ExtendedExportEpisode]

    def _validate(self) -> None:
        assert len(self.title) > 3, self.title
        assert self.added_at < datetime.now(timezone.utc), self.added_at
        assert self.xml_url.startswith("https://"), self.xml_url
        assert self.html_url.startswith("https://"), self.html_url
        assert len(self.episodes) > 0


@dataclass
class AccountExport:
    feeds: list["ExportFeed"]


def export_account_data(session: Session) -> AccountExport:
    path = "/account/export_opml"
    r = _request(
        session,
        path=path,
        accept="application/xml",
        cache_expires=timedelta(days=7),
    )

    soup = BeautifulSoup(r.content, "xml")
    return AccountExport(
        feeds=_opml_feeds(soup),
    )


@dataclass
class ExportFeed:
    item_id: int
    title: str
    xml_url: str
    html_url: str
    added_at: datetime

    def _validate(self) -> None:
        assert not self.xml_url.startswith("/"), self.xml_url
        assert not self.html_url.startswith("/"), self.html_url
        assert len(self.title) > 3, self.title
        assert self.added_at < datetime.now(timezone.utc), self.added_at


def _opml_feeds(soup: BeautifulSoup) -> list[ExportFeed]:
    feeds: list[ExportFeed] = []

    for outline in soup.select("outline[text='feeds'] > outline[type='rss']"):
        item_id: int = int(outline.attrs["overcastId"])
        title: str = outline.attrs["title"]
        html_url: str = outline.attrs["htmlUrl"]
        xml_url: str = outline.attrs["xmlUrl"]
        added_at = dateutil.parser.parse(outline.attrs["overcastAddedDate"])

        feed = ExportFeed(
            item_id=item_id,
            title=title,
            xml_url=xml_url,
            html_url=html_url,
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
    path = "/account/export_opml/extended"
    r = _request(
        session,
        path=path,
        accept="application/xml",
        cache_expires=timedelta(days=7),
    )

    soup = BeautifulSoup(r.content, "xml")
    return AccountExtendedExport(
        playlists=_opml_extended_playlists(soup),
        feeds=_opml_extended_feeds(soup),
    )


def _opml_extended_playlists(soup: BeautifulSoup) -> list[ExtendedExportPlaylist]:
    playlists: list[ExtendedExportPlaylist] = []

    for outline in soup.select(
        "outline[text='playlists'] > outline[type='podcast-playlist']"
    ):
        title: str = outline.attrs["title"]
        smart: bool = outline.attrs["smart"] == "1"
        sorting: str = outline.attrs["sorting"]
        playlist = ExtendedExportPlaylist(title=title, smart=smart, sorting=sorting)
        playlist._validate()
        playlists.append(playlist)

    logger.debug("Found %d playlists in export", len(playlists))
    return playlists


def _opml_extended_feeds(soup: BeautifulSoup) -> list[ExtendedExportFeed]:
    feeds: list[ExtendedExportFeed] = []

    for outline in soup.select("outline[text='feeds'] > outline[type='rss']"):
        item_id: int = int(outline.attrs["overcastId"])
        title: str = outline.attrs["title"]
        html_url: str = outline.attrs["htmlUrl"]
        xml_url: str = outline.attrs["xmlUrl"]
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
        # logger.debug("%s", feed)
        feeds.append(feed)

    logger.debug("Found %d feeds in export", len(feeds))
    return feeds


def _opml_extended_episode(rss_outline: Tag) -> list[ExtendedExportEpisode]:
    episodes: list[ExtendedExportEpisode] = []

    for outline in rss_outline.select("outline[type='podcast-episode']"):
        overcast_url: str = outline.attrs["overcastUrl"]
        id = outline.attrs["overcastUrl"].removeprefix("https://overcast.fm/")
        assert id.startswith("+"), overcast_url
        item_id = int(outline.attrs["overcastId"])
        pub_date = dateutil.parser.parse(outline.attrs["pubDate"]).date()
        title: str = outline.attrs["title"]
        url: str = outline.attrs["url"]
        enclosure_url: str = outline.attrs["enclosureUrl"]
        user_updated_at = dateutil.parser.parse(outline.attrs["userUpdatedDate"])
        user_deleted: bool = outline.attrs.get("userDeleted", "0") == "1"
        played: bool = outline.attrs.get("played", "0") == "1"

        episode = ExtendedExportEpisode(
            id=id,
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
    path: str,
    accept: str | None,
    cache_expires: timedelta,
) -> requests.Response:
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


def _parse_duration(text: str) -> timedelta:
    text = text.strip()
    assert text.endswith(" min"), text
    text = text[:-4]
    minutes = int(text)
    return timedelta(minutes=minutes)


T = TypeVar("T")


def _as_list(x: T | list[T]) -> list[T]:
    if isinstance(x, list):
        return x
    return [x]


def zip_html_and_export_feeds(
    html_feeds: list[HTMLPodcastsFeed], export_feeds: list[ExtendedExportFeed]
) -> Iterator[tuple[HTMLPodcastsFeed, ExtendedExportFeed]]:
    assert len(html_feeds) == len(export_feeds)

    html_feeds_by_title = {feed.title: feed for feed in html_feeds}

    for export_feed in export_feeds:
        html_feed = html_feeds_by_title[export_feed.title]
        yield html_feed, export_feed
