from pathlib import Path
import sqlite3

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


def test_sqlite_cache_migrates_old_schema_and_keeps_data(tmp_path: Path):
    db = tmp_path / "cache-v1.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE bookmark_cache (
                cache_key TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                final_url TEXT,
                title TEXT,
                tags_json TEXT NOT NULL DEFAULT '[]',
                categories_json TEXT NOT NULL DEFAULT '[]',
                status_code INTEGER,
                visited_at TEXT,
                summary TEXT,
                html TEXT,
                page_title TEXT,
                page_description TEXT,
                content_snippet TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO bookmark_cache (
                cache_key, url, final_url, title, tags_json, categories_json, status_code, visited_at,
                summary, html, page_title, page_description, content_snippet, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ex",
                "https://example.com/",
                "https://example.com/",
                "Example",
                '["news"]',
                '["Reading"]',
                200,
                "2026-02-09T12:00:00+00:00",
                "sum",
                "<html/>",
                "pt",
                "pd",
                "snip",
                "2026-02-09T12:00:00+00:00",
            ),
        )
        conn.execute("PRAGMA user_version = 1")

    init_cache(db, recreate=False)
    got = load_entries(db, ["ex"])
    assert got["ex"].url == "https://example.com/"

    with sqlite3.connect(db) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(bookmark_cache)")}
        assert "icon_url" in cols
        ver = int(conn.execute("PRAGMA user_version").fetchone()[0] or 0)
        assert ver >= 2


def test_sqlite_cache_migrates_minimal_schema_and_enforces_unique_key(tmp_path: Path):
    db = tmp_path / "cache-legacy.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE bookmark_cache (
                cache_key TEXT,
                url TEXT
            )
            """
        )
        # Simulate bad legacy duplicates.
        conn.execute("INSERT INTO bookmark_cache(cache_key, url) VALUES(?, ?)", ("dup", "https://x/1"))
        conn.execute("INSERT INTO bookmark_cache(cache_key, url) VALUES(?, ?)", ("dup", "https://x/2"))
        conn.execute("PRAGMA user_version = 0")

    init_cache(db, recreate=False)
    row = CacheEntry(
        cache_key="dup",
        url="https://x/3",
        final_url="https://x/3",
        title="X",
        tags=["news"],
        categories=["Reading"],
        status_code=200,
        visited_at="2026-02-09T12:00:00+00:00",
        summary="s",
        html="<h/>",
        page_title="pt",
        page_description="pd",
        content_snippet="sn",
        icon_url="https://x/favicon.ico",
    )
    upsert_entries(db, [row])
    got = load_entries(db, ["dup"])
    assert got["dup"].url == "https://x/3"

    with sqlite3.connect(db) as conn:
        indexes = {r[1] for r in conn.execute("PRAGMA index_list(bookmark_cache)").fetchall()}
        assert "uq_bookmark_cache_cache_key" in indexes
