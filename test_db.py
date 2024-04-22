from db import Feed


def test_feed_dict_roundtrip() -> None:
    feed_dict = {
        "overcast_url": "https://overcast.fm/itunes528458508/the-talk-show-with-john-gruber",
        "id": "126160",
        "title": "The Talk Show With John Gruber",
        "slug": "the-talk-show-with-john-gruber",
        "html_url": "https://daringfireball.net/thetalkshow/",
        "added_at": "2014-07-16T16:56:20+00:00",
        "is_subscribed": "1",
    }
    assert Feed.from_dict(feed_dict).to_dict() == feed_dict
