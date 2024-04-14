import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator, TypeVar

import dateutil.parser
import requests_cache
import xmltodict
from bs4 import BeautifulSoup

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


@dataclass
class HTMLFeed:
    id: str
    numeric_id: int | None
    title: str
    has_unplayed_episodes: bool


def fetch_podcasts(cache_dir: Path, cookie: str) -> list[HTMLFeed]:
    r = _request(
        url="https://overcast.fm/podcasts",
        cache_dir=cache_dir,
        cookie=cookie,
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
        logger.debug("%s", feed)
        feeds.append(feed)

    if len(feeds) == 0:
        logger.error("No feeds found")

    return feeds


@dataclass
class HTMLEpisode:
    id: str
    title: str
    description: str
    pub_date: date
    duration: timedelta | None = None
    is_played: bool = False
    in_progress: bool = False


def fetch_podcast(feed_id: str, cache_dir: Path, cookie: str) -> list[HTMLEpisode]:
    r = _request(
        url=f"https://overcast.fm/{feed_id}",
        cache_dir=cache_dir,
        cookie=cookie,
    )

    soup = BeautifulSoup(r.text, "html.parser")

    episodes: list[HTMLEpisode] = []

    for episodecell_el in soup.select("a.extendedepisodecell"):
        href = episodecell_el["href"]

        if not isinstance(href, str):
            logger.error("episodecell missing href: %s", episodecell_el)
            continue

        title: str = ""
        title_el = episodecell_el.select_one(".title")
        if title_el:
            title = title_el.text.strip()
        else:
            logger.error("No title element found: %s", episodecell_el)

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
            id=href,
            title=title,
            description=description,
            pub_date=caption_result.pub_date,
            duration=caption_result.duration,
            is_played=caption_result.is_played,
            in_progress=caption_result.in_progress,
        )
        episodes.append(episode)

    if len(episodes) == 0:
        logger.error("No episodes found")

    return episodes


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


@dataclass
class ExportFeed:
    numeric_id: int
    title: str
    xml_url: str
    html_url: str
    added_at: datetime
    is_subscribed: bool
    episodes: list[ExportEpisode]


@dataclass
class AccountExport:
    playlists: list[ExportPlaylist]
    feeds: list[ExportFeed]


def export_account_data(cache_dir: Path, cookie: str) -> AccountExport:
    r = _request(
        url="https://overcast.fm/account/export_opml/extended",
        cache_dir=cache_dir,
        cookie=cookie,
    )

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
                playlists.append(
                    ExportPlaylist(title=title, smart=smart, sorting=sorting)
                )

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

        episodes.append(
            ExportEpisode(
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
        )

    logger.info("Found %d episodes in export", len(episodes))
    return episodes


def _request(
    cache_dir: Path, cookie: str, url: str
) -> requests_cache.CachedResponse | requests_cache.OriginalResponse:
    cache_name = cache_dir / "overcast_cache"
    session = requests_cache.CachedSession(cache_name=cache_name, backend="filesystem")

    headers = _SAFARI_HEADERS.copy()
    headers["Cookie"] = f"o={cookie}; qr=-"

    assert url.startswith("https://overcast.fm/")

    _ratelimit()
    r = session.get(url, headers=headers)
    r.raise_for_status()

    if "Log In" in r.text:
        logger.error("Bad auth cookie")
        raise LoggedOutError("Bad auth cookie")

    return r


_MIN_TIME_BETWEEN_REQUESTS = timedelta(seconds=5)
_last_request_at: datetime = datetime.min


def _ratelimit() -> None:
    global _last_request_at
    seconds_to_wait = (
        _last_request_at + _MIN_TIME_BETWEEN_REQUESTS - datetime.now()
    ).total_seconds()
    if seconds_to_wait > 0:
        logger.info("Waiting %s seconds...", seconds_to_wait)
        time.sleep(seconds_to_wait)
    _last_request_at = datetime.now()


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
