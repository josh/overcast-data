import os

import pytest

from overcast import fetch_podcasts


@pytest.fixture
def overcast_cookie() -> str:
    if "OVERCAST_COOKIE" not in os.environ:
        pytest.skip("OVERCAST_COOKIE not set")
    return os.environ["OVERCAST_COOKIE"]


def test_fetch_podcasts(tmp_path, overcast_cookie):
    podcasts = fetch_podcasts(cache_dir=tmp_path, cookie=overcast_cookie)
    assert len(podcasts) > 0
