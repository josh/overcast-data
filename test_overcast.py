import os
from datetime import date, timedelta
from pathlib import Path

import pytest

import overcast
from overcast import (
    Session,
    fetch_audio_duration,
    fetch_episode,
    fetch_podcast,
    fetch_podcasts,
    parse_episode_caption_text,
)

_OFFLINE = "PYTEST_OFFLINE" in os.environ


@pytest.fixture(scope="module")
def module_cache_dir() -> Path:
    cache_home = os.environ.get("XDG_CACHE_HOME") or "/tmp/pytest"
    return Path(cache_home) / "overcast"


@pytest.fixture(scope="function")
def function_cache_dir(request: pytest.FixtureRequest, module_cache_dir: Path) -> Path:
    return module_cache_dir / str(request.node.name)


@pytest.fixture(scope="module")
def overcast_cookie() -> str:
    if "OVERCAST_COOKIE" not in os.environ:
        pytest.skip("OVERCAST_COOKIE not set")
    return os.environ["OVERCAST_COOKIE"]


@pytest.fixture(scope="module")
def overcast_session(module_cache_dir: Path, overcast_cookie: str) -> Session:
    return overcast.session(
        cache_dir=module_cache_dir,
        cookie=overcast_cookie,
        offline=_OFFLINE,
    )


def test_fetch_podcasts(overcast_session: Session) -> None:
    feeds = fetch_podcasts(session=overcast_session)
    assert len(feeds) > 0


def test_fetch_podcasts_bad_cookie(
    function_cache_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    session = overcast.session(
        cache_dir=function_cache_dir,
        cookie="XXX",
        offline=_OFFLINE,
    )
    with pytest.raises(overcast.LoggedOutError):
        with caplog.at_level(100):
            fetch_podcasts(session=session)


def test_fetch_podcast(overcast_session: Session) -> None:
    episodes_feed = fetch_podcast(
        session=overcast_session,
        feed_id="itunes528458508/the-talk-show-with-john-gruber",
    )
    assert episodes_feed.title == "The Talk Show With John Gruber"
    assert (
        episodes_feed.html_url
        == "https://overcast.fm/itunes528458508/the-talk-show-with-john-gruber"
    )
    assert episodes_feed.item_id == 126160
    assert episodes_feed.art_url == "https://public.overcast-cdn.com/art/126160?v198"
    assert len(episodes_feed.episodes) > 0


def test_fetch_episode(overcast_session: Session) -> None:
    episode = fetch_episode(session=overcast_session, episode_id="+B7NAFKiP8")
    assert episode.id == "+B7NAFKiP8"
    assert episode.item_id == 135463290177791
    assert episode.overcast_uri == "overcast:///135463290177791"
    assert (
        episode.podcast_html_url
        == "https://overcast.fm/itunes528458508/the-talk-show-with-john-gruber"
    )
    assert episode.feed_art_url == "https://public.overcast-cdn.com/art/126160?v198"
    assert episode.title.startswith("83: Live From WWDC 2014")
    assert episode.date_published == date(2014, 6, 6)
    assert (
        episode.audio_url
        == "http://feeds.soundcloud.com/stream/153165973-thetalkshow-83-live-at-wwdc-2014.mp3"
    )


def test_fetch_audio_duration(overcast_session: Session) -> None:
    url = "http://feeds.soundcloud.com/stream/153165973-thetalkshow-83-live-at-wwdc-2014.mp3"
    duration = fetch_audio_duration(session=overcast_session, url=url)
    assert duration == timedelta(seconds=6538)

    url = "http://example.com/"
    duration = fetch_audio_duration(session=overcast_session, url=url)
    assert duration is None


# def test_export_account_extended_data(overcast_session: Session) -> None:
#     export_data = export_account_extended_data(session=overcast_session)
#     assert len(export_data.playlists) > 0
#     assert len(export_data.feeds) > 0


def test_parse_episode_caption_text() -> None:
    now = date.today()

    result = parse_episode_caption_text("Apr 1 • played")
    assert result.pub_date == date(now.year, 4, 1)
    assert result.duration is None
    assert result.is_played is True
    assert result.in_progress is False

    result = parse_episode_caption_text("Feb 11 • 162 min")
    assert result.pub_date == date(now.year, 2, 11)
    assert result.duration == timedelta(minutes=162)
    assert result.is_played is False
    assert result.in_progress is False

    result = parse_episode_caption_text("Feb 4, 2019 • 104 min")
    assert result.pub_date == date(2019, 2, 4)
    assert result.duration == timedelta(minutes=104)
    assert result.is_played is False
    assert result.in_progress is False

    result = parse_episode_caption_text("Jan 16 • at 99 min")
    assert result.pub_date == date(now.year, 1, 16)
    assert result.duration is None
    assert result.is_played is True
    assert result.in_progress is True

    result = parse_episode_caption_text("Nov 14, 2023 • at 82 min")
    assert result.pub_date == date(2023, 11, 14)
    assert result.duration is None
    assert result.is_played is True
    assert result.in_progress is True

    result = parse_episode_caption_text("Apr 3, 2015 • 0 min left")
    assert result.pub_date == date(2015, 4, 3)
    assert result.duration is None
    assert result.is_played
    assert result.in_progress is False

    result = parse_episode_caption_text("Dec 29, 2023")
    assert result.pub_date == date(2023, 12, 29)
    assert result.duration is None
    assert result.is_played is False
    assert result.in_progress is False

    result = parse_episode_caption_text("Apr 11")
    assert result.pub_date == date(now.year, 4, 11)
    assert result.duration is None
    assert result.is_played is False
    assert result.in_progress is False


def test_session_purge_cache(overcast_session: Session) -> None:
    overcast_session.purge_cache(older_than=timedelta(days=90))
