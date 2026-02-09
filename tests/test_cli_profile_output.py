import sqlite3
from pathlib import Path
import json

from borgmarks.cache_sqlite import CacheEntry, init_cache, upsert_entries
from borgmarks.cli import _url_identity, main


def _mk_profile(profile: Path) -> None:
    profile.mkdir(parents=True, exist_ok=True)
    db = profile / "places.sqlite"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE moz_places (
              id INTEGER PRIMARY KEY,
              url TEXT,
              title TEXT,
              hidden INTEGER DEFAULT 0,
              guid TEXT,
              foreign_count INTEGER DEFAULT 0
            );
            CREATE TABLE moz_bookmarks (
              id INTEGER PRIMARY KEY,
              type INTEGER,
              fk INTEGER DEFAULT NULL,
              parent INTEGER,
              position INTEGER,
              title TEXT,
              keyword_id INTEGER,
              folder_type TEXT,
              dateAdded INTEGER,
              lastModified INTEGER,
              guid TEXT,
              syncStatus INTEGER NOT NULL DEFAULT 0,
              syncChangeCounter INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE moz_bookmarks_roots (root_name TEXT PRIMARY KEY, folder_id INTEGER);
            """
        )
        conn.executemany(
            "INSERT INTO moz_bookmarks_roots(root_name, folder_id) VALUES(?, ?)",
            [
                ("toolbar", 3),
                ("menu", 2),
                ("tags", 4),
                ("unfiled", 5),
                ("mobile", 6),
            ],
        )
        conn.executemany(
            "INSERT INTO moz_bookmarks(id,type,fk,parent,position,title,dateAdded,lastModified,guid) VALUES(?,?,?,?,?,?,?,?,?)",
            [
                (1, 2, None, 0, 0, "root", 0, 0, "root________"),
                (2, 2, None, 1, 0, "menu", 0, 0, "menu________"),
                (3, 2, None, 1, 1, "toolbar", 0, 0, "toolbar_____"),
                (4, 2, None, 1, 2, "tags", 0, 0, "tags________"),
                (5, 2, None, 1, 3, "unfiled", 0, 0, "unfiled_____"),
                (6, 2, None, 1, 4, "mobile", 0, 0, "mobile______"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _meta_semantic_index(meta_path: Path):
    rows = [json.loads(x) for x in meta_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    return {
        r["url"]: (
            tuple(r.get("path") or []),
            tuple(r.get("tags") or []),
            r.get("http_status"),
            r.get("summary"),
        )
        for r in rows
    }


def test_cli_writes_output_to_firefox_profile_and_ignores_out(tmp_path: Path):
    profile = tmp_path / "profile"
    _mk_profile(profile)

    ios = Path(__file__).parent / "fixtures" / "sample_bookmarks.html"
    ignored_out = tmp_path / "ignored.html"

    rc = main(
        [
            "organize",
            "--ios-html",
            str(ios),
            "--firefox-profile",
            str(profile),
            "--out",
            str(ignored_out),
            "--no-openai",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc == 0
    assert (profile / "bookmarks.organized.html").exists()
    assert (profile / "bookmarks.organized.meta.jsonl").exists()
    assert not ignored_out.exists()


def test_cli_supports_firefox_only_mode_without_ios_html(tmp_path: Path):
    profile = tmp_path / "profile"
    _mk_profile(profile)

    rc = main(
        [
            "organize",
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc == 0
    assert (profile / "bookmarks.organized.html").exists()


def test_cli_skips_openai_when_cache_has_summary_and_categories(tmp_path: Path, monkeypatch):
    profile = tmp_path / "profile"
    _mk_profile(profile)
    cache_db = profile / "borg_cache.sqlite"
    init_cache(cache_db, recreate=True)
    urls = [
        "https://github.com/",
        "https://onet.pl/",
        "https://en.wikipedia.org/wiki/Fujifilm",
    ]
    rows = [
        CacheEntry(
            cache_key=_url_identity(url),
            url=url,
            final_url=url,
            title=None,
            tags=["cached"],
            categories=["Computers", "Dev"],
            status_code=200,
            visited_at="2026-02-09T00:00:00+00:00",
            summary="cached openai summary",
            html=None,
            page_title=None,
            page_description=None,
            content_snippet=None,
            icon_url=None,
        )
        for url in urls
    ]
    upsert_entries(cache_db, rows)

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_KEY", raising=False)

    ios = Path(__file__).parent / "fixtures" / "sample_bookmarks.html"
    rc = main(
        [
            "organize",
            "--ios-html",
            str(ios),
            "--firefox-profile",
            str(profile),
            "--no-fetch",
        ]
    )
    assert rc == 0
    assert (profile / "bookmarks.organized.html").exists()


def test_cli_apply_firefox_persists_links_even_if_folder_emoji_fails(tmp_path: Path, monkeypatch):
    profile = tmp_path / "profile"
    _mk_profile(profile)
    ios = Path(__file__).parent / "fixtures" / "sample_bookmarks.html"

    def _fake_classify(bookmarks, _cfg):
        for b in bookmarks:
            if not b.assigned_path:
                b.assigned_path = ["Bookmarks Menu", "Reading"]
        return {b.id for b in bookmarks}

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr("borgmarks.cli.classify_bookmarks", _fake_classify)
    monkeypatch.setattr("borgmarks.cli.enrich_folder_emojis", lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("boom")))

    rc = main(
        [
            "organize",
            "--ios-html",
            str(ios),
            "--firefox-profile",
            str(profile),
            "--apply-firefox",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc == 0

    with sqlite3.connect(profile / "places.sqlite") as conn:
        count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM moz_bookmarks b
                JOIN moz_places p ON p.id = b.fk
                WHERE b.type = 1 AND p.url LIKE 'https://%'
                """
            ).fetchone()[0]
        )
    assert count >= 3


def test_cli_no_openai_no_fetch_second_run_is_stable_with_cache(tmp_path: Path):
    profile = tmp_path / "profile"
    _mk_profile(profile)
    ios = Path(__file__).parent / "fixtures" / "sample_bookmarks.html"

    rc1 = main(
        [
            "organize",
            "--ios-html",
            str(ios),
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc1 == 0

    out_html = profile / "bookmarks.organized.html"
    out_meta = profile / "bookmarks.organized.meta.jsonl"
    cache_db = profile / "borg_cache.sqlite"
    assert out_html.exists()
    assert out_meta.exists()
    assert cache_db.exists()
    first_html = out_html.read_text(encoding="utf-8")
    first_meta = out_meta.read_text(encoding="utf-8")

    rc2 = main(
        [
            "organize",
            "--ios-html",
            str(ios),
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
        ]
    )
    assert rc2 == 0

    second_html = out_html.read_text(encoding="utf-8")
    second_meta = out_meta.read_text(encoding="utf-8")
    assert second_html == first_html
    assert second_meta == first_meta


def test_cli_no_openai_no_fetch_golden_reingest_is_stable(tmp_path: Path):
    profile = tmp_path / "profile"
    _mk_profile(profile)
    ios = Path(__file__).parent / "fixtures" / "sample_bookmarks.html"

    # A -> B
    rc1 = main(
        [
            "organize",
            "--ios-html",
            str(ios),
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc1 == 0
    out_html = profile / "bookmarks.organized.html"
    out_meta = profile / "bookmarks.organized.meta.jsonl"
    assert out_html.exists()
    b_html = out_html.read_text(encoding="utf-8")
    assert out_meta.exists()
    b_meta_sem = _meta_semantic_index(out_meta)

    # B -> C (re-ingest previous golden output)
    rc2 = main(
        [
            "organize",
            "--ios-html",
            str(out_html),
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
        ]
    )
    assert rc2 == 0
    c_html = out_html.read_text(encoding="utf-8")
    c_meta_sem = _meta_semantic_index(out_meta)
    assert c_html == b_html
    # IDs/order may differ on re-ingest; semantic data must stay stable.
    assert c_meta_sem == b_meta_sem
