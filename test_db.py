import os

import pytest

from db import Feed


def test_feed_dict_roundtrip() -> None:
    if "ENCRYPTION_KEY" not in os.environ:
        pytest.skip("ENCRYPTION_KEY not set")
    feed_dict = {
        "overcast_url": "https://overcast.fm/itunes528458508/the-talk-show-with-john-gruber",
        "id": "126160",
        "encrypted_title": "z/tWEzgSTWzWY03CGwvlbIhqfssDwOzoQvGuI4K8uDA=",
        "clean_title": "The Talk Show With John Gruber",
        "slug": "the-talk-show-with-john-gruber",
        "html_url": "https://daringfireball.net/thetalkshow/",
        "added_at": "2014-07-16T16:56:20+00:00",
        "is_added": "1",
        "is_following": "1",
    }
    assert feed_dict == Feed.from_dict(feed_dict).to_dict()
