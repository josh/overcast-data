import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator, TypeVar

import dateutil.parser
import requests
import xmltodict
from bs4 import BeautifulSoup

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
        min_time_between_requests=timedelta(minutes=1),
        offline=offline,
    )


@dataclass
class HTMLFeed:
    id: str
    numeric_id: int | None
    title: str
    has_unplayed_episodes: bool

    def _validate(self) -> None:
        assert not self.id.startswith("/"), self.id
        if self.id.startswith("p"):
            assert len(self.id) == 15, self.id
            assert "-" in self.id, self.id

        assert len(self.title) > 3


def fetch_podcasts(session: Session) -> list[HTMLFeed]:
    r = _request(
        session=session,
        path="/podcasts",
        cache_expires=timedelta(hours=1),
    )

    feeds: list[HTMLFeed] = []

    soup = BeautifulSoup(r.text, "html.parser")

    for feedcell_el in soup.select("a.feedcell"):
        href = feedcell_el["href"]

        if not isinstance(href, str):
            logger.error("feedcell missing href: %s", feedcell_el)
            continue

        if href == "/uploads":
            continue

        id = href.removeprefix("/")

        numeric_id: int | None = None
        if href.startswith("/p"):
            numeric_id = int(href.removeprefix("/p").split("-", 1)[0])

        title_el = feedcell_el.select_one(".titlestack > .title")

        if not title_el:
            logger.error("No title element found: %s", feedcell_el)
            continue

        has_unplayed_episodes = (
            True if feedcell_el.select_one(".unplayed_indicator") else False
        )

        feed = HTMLFeed(
            id=id,
            numeric_id=numeric_id,
            title=title_el.text.strip(),
            has_unplayed_episodes=has_unplayed_episodes,
        )
        feed._validate()
        logger.debug("%s", feed)
        feeds.append(feed)

    if len(feeds) == 0:
        logger.error("No feeds found")

    return feeds


@dataclass
class HTMLEpisodeFeed:
    overcast_uri: str
    episodes: list["HTMLEpisode"]

    def _validate(self) -> None:
        assert self.overcast_uri.startswith("overcast:///"), self.overcast_uri
        assert len(self.episodes) > 0


@dataclass
class HTMLEpisode:
    id: str
    title: str
    description: str
    pub_date: date
    duration: timedelta | None = None
    is_played: bool = False
    in_progress: bool = False
    is_new: bool = False
    is_deleted: bool = False

    def _validate(self) -> None:
        assert not self.id.startswith("/"), self.id
        if self.id.startswith("p"):
            assert len(self.id) == 15, self.id
            assert "-" in self.id, self.id

        assert len(self.title) > 3, self.title
        assert self.pub_date <= datetime.now().date(), self.pub_date
        assert self.is_deleted != self.is_new, "is_deleted and is_new can't be the same"


def fetch_podcast(session: Session, feed_id: str) -> HTMLEpisodeFeed:
    r = _request(
        session=session,
        path=f"/{feed_id}",
        cache_expires=timedelta(hours=1),
    )

    soup = BeautifulSoup(r.text, "html.parser")

    overcast_uri: str = ""
    for meta_el in soup.select("meta[name=apple-itunes-app]"):
        content = meta_el["content"]
        if isinstance(content, str) and content.startswith("app-id=888422857"):
            overcast_uri = content.removeprefix("app-id=888422857, app-argument=")

    episodes: list[HTMLEpisode] = []

    for episodecell_el in soup.select("a.extendedepisodecell"):
        href = episodecell_el["href"]
        if not isinstance(href, str):
            logger.error("episodecell missing href: %s", episodecell_el)
            continue

        id = href.removeprefix("/")

        if not isinstance(href, str):
            logger.error("episodecell missing href: %s", episodecell_el)
            continue

        title: str = ""
        title_el = episodecell_el.select_one(".title")
        if title_el:
            title = title_el.text.strip()
        else:
            logger.error("No title element found: %s", episodecell_el)

        class_name = episodecell_el.attrs["class"]
        is_deleted = "userdeletedepisode" in class_name
        is_new = "usernewepisode" in class_name

        caption2_el = episodecell_el.select_one(".caption2")
        if not caption2_el:
            logger.error("No caption2 element found: %s", episodecell_el)
            continue
        caption_result = parse_episode_caption_text(caption2_el.text)

        description: str = ""
        description_el = episodecell_el.select_one(".lighttext")
        if description_el:
            description = description_el.text.strip()
        else:
            logger.error("No description element found: %s", episodecell_el)

        episode = HTMLEpisode(
            id=id,
            title=title,
            description=description,
            pub_date=caption_result.pub_date,
            duration=caption_result.duration,
            is_played=caption_result.is_played,
            in_progress=caption_result.in_progress,
            is_deleted=is_deleted,
            is_new=is_new,
        )
        episode._validate()
        episodes.append(episode)

    feed = HTMLEpisodeFeed(overcast_uri=overcast_uri, episodes=episodes)
    feed._validate()
    return feed


@dataclass
class CaptionResult:
    pub_date: date
    duration: timedelta | None = None
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
class ExportPlaylist:
    title: str
    smart: bool
    sorting: str

    def _validate(self) -> None:
        assert len(self.title) > 3, self.title


@dataclass
class ExportEpisode:
    id: str
    numeric_id: int
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
        assert self.user_updated_at < datetime.now(), self.user_updated_at
        assert self.enclosure_url.startswith("https://"), self.enclosure_url


@dataclass
class ExportFeed:
    numeric_id: int
    title: str
    xml_url: str
    html_url: str
    added_at: datetime
    is_subscribed: bool
    episodes: list[ExportEpisode]

    def _validate(self) -> None:
        assert len(self.title) > 3, self.title
        assert self.added_at < datetime.now(), self.added_at
        assert self.xml_url.startswith("https://"), self.xml_url
        assert self.html_url.startswith("https://"), self.html_url
        assert len(self.episodes) > 0


@dataclass
class AccountExport:
    playlists: list[ExportPlaylist]
    feeds: list[ExportFeed]


def export_account_data(session: Session, extended: bool = False) -> AccountExport:
    path = "/account/export_opml"
    if extended:
        path += "/extended"

    r = _request(session, path=path, cache_expires=timedelta(days=1))
    d = xmltodict.parse(r.text)
    outline = d["opml"]["body"]["outline"]
    return AccountExport(playlists=_opml_playlists(outline), feeds=_opml_feeds(outline))


def _opml_playlists(node: dict) -> list[ExportPlaylist]:
    playlists: list[ExportPlaylist] = []

    for group in node:
        if group["@text"] == "playlists":
            for playlist_outline in _as_list(group["outline"]):
                title = playlist_outline["@title"]
                smart = playlist_outline.get("@smart", "0") == "1"
                sorting = playlist_outline.get("@sorting", "manual")
                playlist = ExportPlaylist(title=title, smart=smart, sorting=sorting)
                playlist._validate()
                playlists.append(playlist)

    logger.info("Found %d playlists in export", len(playlists))
    return playlists


def _opml_feeds(node: dict) -> list[ExportFeed]:
    feeds: list[ExportFeed] = []

    for group in node:
        if group["@text"] == "feeds":
            for feed_outline in _as_list(group["outline"]):
                assert feed_outline["@type"] == "rss"
                numeric_id = int(feed_outline["@overcastId"])
                title = feed_outline["@title"]
                html_url = feed_outline["@htmlUrl"]
                xml_url = feed_outline["@xmlUrl"]
                added_at = dateutil.parser.parse(feed_outline["@overcastAddedDate"])
                is_subscribed = feed_outline.get("@subscribed", "0") == "1"

                feed = ExportFeed(
                    numeric_id=numeric_id,
                    title=title,
                    xml_url=xml_url,
                    html_url=html_url,
                    added_at=added_at,
                    is_subscribed=is_subscribed,
                    episodes=_opml_episode(_as_list(feed_outline["outline"])),
                )
                feed._validate()
                # logger.debug("%s", feed)
                feeds.append(feed)

    logger.info("Found %d feeds in export", len(feeds))
    return feeds


def _opml_episode(nodes: list[dict]) -> list[ExportEpisode]:
    episodes: list[ExportEpisode] = []

    for node in nodes:
        assert node["@type"] == "podcast-episode"
        overcast_url = node["@overcastUrl"]
        id = node["@overcastUrl"].removeprefix("https://overcast.fm/")
        assert id.startswith("+"), overcast_url
        numeric_id = int(node["@overcastId"])
        pub_date = dateutil.parser.parse(node["@pubDate"]).date()
        title = node["@title"]
        url = node["@url"]
        enclosure_url = node["@enclosureUrl"]
        user_updated_at = dateutil.parser.parse(node["@userUpdatedDate"])
        user_deleted = node.get("@userDeleted", "0") == "1"
        played = node.get("@played", "0") == "1"

        episode = ExportEpisode(
            id=id,
            numeric_id=numeric_id,
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

    logger.info("Found %d episodes in export", len(episodes))
    return episodes


def _request(
    session: Session, path: str, cache_expires: timedelta
) -> requests.Response:
    try:
        response = session.get(path=path, cache_expires=cache_expires)
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
    html_feeds: list[HTMLFeed], export_feeds: list[ExportFeed]
) -> Iterator[tuple[HTMLFeed, ExportFeed]]:
    assert len(html_feeds) == len(export_feeds)

    html_feeds_by_title = {feed.title: feed for feed in html_feeds}

    for export_feed in export_feeds:
        html_feed = html_feeds_by_title[export_feed.title]
        if html_feed.numeric_id:
            assert html_feed.numeric_id == export_feed.numeric_id
        if html_feed.id.startswith("p"):
            assert html_feed.id.startswith(f"p{export_feed.numeric_id}-")
        yield html_feed, export_feed
