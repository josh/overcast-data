import os
from datetime import date, timedelta

import pytest

from overcast import (
    LoggedOutError,
    export_account_data,
    fetch_podcasts,
    parse_episode_caption_text,
)


@pytest.fixture
def overcast_cookie() -> str:
    if "OVERCAST_COOKIE" not in os.environ:
        pytest.skip("OVERCAST_COOKIE not set")
    return os.environ["OVERCAST_COOKIE"]


def test_fetch_podcasts(tmp_path, overcast_cookie):
    podcasts = fetch_podcasts(cache_dir=tmp_path, cookie=overcast_cookie)
    assert len(podcasts) > 0


def test_fetch_podcasts_bad_cookie(tmp_path):
    with pytest.raises(LoggedOutError):
        fetch_podcasts(cache_dir=tmp_path, cookie="XXX")


def test_export_account_data(tmp_path, overcast_cookie):
    export_data = export_account_data(cache_dir=tmp_path, cookie=overcast_cookie)
    assert len(export_data.playlists) > 0
    assert len(export_data.feeds) > 0


def test_parse_episode_caption_text():
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
