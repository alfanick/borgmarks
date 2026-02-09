from borgmarks.cli import _counted_unique_urls, _sanity_check_unique_link_counts
from borgmarks.model import Bookmark


def _bm(url: str, *, status=None, final_url=None) -> Bookmark:
    b = Bookmark(id="x", title="t", url=url)
    b.http_status = status
    b.final_url = final_url
    return b


def test_counted_unique_urls_follows_redirect_and_skips_non_200():
    b1 = _bm("https://old.example/?utm_source=mail", status=200, final_url="https://new.example/path")
    b2 = _bm("https://drop.example/", status=404, final_url="https://drop.example/")
    b3 = _bm("https://raw.example/?fbclid=abc", status=None, final_url=None)
    b4 = _bm("https://raw.example/", status=200, final_url=None)

    got = _counted_unique_urls([b1, b2, b3, b4])

    assert got == {"https://new.example/path", "https://raw.example/"}


def test_sanity_check_passes_when_output_preserves_unique_urls():
    inp = [
        _bm("https://a.example/", status=200, final_url="https://a.example/home"),
        _bm("https://b.example/?utm_campaign=test", status=200, final_url=None),
        _bm("https://dead.example/", status=500, final_url="https://dead.example/"),
    ]
    out = [
        _bm("https://a.example/home", status=200, final_url=None),
        _bm("https://b.example/", status=200, final_url=None),
    ]

    assert _sanity_check_unique_link_counts(inp, out) is True


def test_sanity_check_fails_when_unique_link_is_missing():
    inp = [
        _bm("https://a.example/", status=200, final_url=None),
        _bm("https://b.example/", status=200, final_url=None),
    ]
    out = [_bm("https://a.example/", status=200, final_url=None)]

    assert _sanity_check_unique_link_counts(inp, out) is False
