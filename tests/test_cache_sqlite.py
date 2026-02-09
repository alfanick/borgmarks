from pathlib import Path

from borgmarks.cache_sqlite import CacheEntry, init_cache, load_entries, upsert_entries


def test_sqlite_cache_roundtrip(tmp_path: Path):
    db = tmp_path / "cache.sqlite"
    init_cache(db, recreate=True)
    row = CacheEntry(
        cache_key="example.com/path",
        url="https://example.com/path",
        final_url="https://example.com/path",
        title="Example",
        tags=["tag-a", "tag-b"],
        categories=["Reading", "Inbox"],
        status_code=200,
        visited_at="2026-02-09T10:00:00+00:00",
        summary="summary",
        html="<html></html>",
        page_title="Page Title",
        page_description="Desc",
        content_snippet="Snippet",
        icon_url="https://example.com/favicon.ico",
    )
    upsert_entries(db, [row])

    got = load_entries(db, ["example.com/path"])
    assert "example.com/path" in got
    r = got["example.com/path"]
    assert r.url == "https://example.com/path"
    assert r.status_code == 200
    assert r.tags == ["tag-a", "tag-b"]
    assert r.categories == ["Reading", "Inbox"]
    assert r.summary == "summary"
    assert r.html == "<html></html>"
    assert r.icon_url == "https://example.com/favicon.ico"
