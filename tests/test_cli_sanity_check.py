from borgmarks.cli import _counted_unique_urls, _fallback_assign, _normalize_category_paths, _sanity_check_unique_link_counts
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

    assert got == {"new.example/path", "raw.example/"}


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


def test_normalize_category_paths_collapses_emoji_and_plain_folder_names():
    b1 = Bookmark(id="1", title="a", url="https://a")
    b2 = Bookmark(id="2", title="b", url="https://b")
    b1.assigned_path = ["Shopping", "👕 Clothing"]
    b2.assigned_path = ["Shopping", "Clothing"]
    _normalize_category_paths([b1, b2])
    assert b1.assigned_path == b2.assigned_path


def test_fallback_assign_only_touches_uncategorized_bookmarks():
    b1 = Bookmark(id="1", title="A", url="https://github.com/")
    b2 = Bookmark(id="2", title="B", url="https://x.example/")
    b2.assigned_path = ["Reading", "Stable"]
    touched = _fallback_assign([b1, b2])
    assert touched == {"1"}
    assert b2.assigned_path == ["Reading", "Stable"]
