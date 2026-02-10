import sqlite3
from pathlib import Path
import json

from borgmarks.cache_sqlite import CacheEntry, init_cache, load_entries, upsert_entries
from borgmarks.cli import _url_identity, main
from borgmarks.places_db import PlacesDB


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


def _write_bookmarks_html(path: Path, urls: list[str]) -> None:
    lines = [
        "<!DOCTYPE NETSCAPE-Bookmark-file-1>",
        '<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">',
        "<TITLE>Bookmarks</TITLE>",
        "<H1>Bookmarks</H1>",
        "<DL><p>",
    ]
    for i, url in enumerate(urls, start=1):
        lines.append(f'<DT><A HREF="{url}">Link {i}</A>')
    lines.append("</DL><p>")
    path.write_text("\n".join(lines), encoding="utf-8")


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


def test_cli_apply_firefox_fails_fast_when_places_locked(tmp_path: Path, monkeypatch):
    profile = tmp_path / "profile"
    _mk_profile(profile)
    ios = Path(__file__).parent / "fixtures" / "sample_bookmarks.html"

    called = {"classify": 0}

    def _classify_guard(*_a, **_kw):
        called["classify"] += 1
        raise AssertionError("classify_bookmarks should not run when firefox DB is locked")

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr("borgmarks.cli.classify_bookmarks", _classify_guard)

    lock_conn = sqlite3.connect(profile / "places.sqlite")
    lock_conn.execute("BEGIN EXCLUSIVE")
    try:
        rc = main(
            [
                "organize",
                "--ios-html",
                str(ios),
                "--firefox-profile",
                str(profile),
                "--apply-firefox",
                "--no-fetch",
            ]
        )
    finally:
        lock_conn.rollback()
        lock_conn.close()

    assert rc == 2
    assert called["classify"] == 0


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
    called = {"classify": 0, "folder_emoji": 0, "tag_openai": 0}

    def _classify_guard(*_a, **_kw):
        called["classify"] += 1
        raise AssertionError("classify_bookmarks should not run when cache has full OpenAI enrichment")

    def _folder_emoji_guard(*_a, **_kw):
        called["folder_emoji"] += 1
        raise AssertionError("folder emoji OpenAI path should be skipped with full cache")

    def _tag_openai_guard(*_a, **_kw):
        called["tag_openai"] += 1
        raise AssertionError("tag OpenAI path should be skipped with full cache")

    monkeypatch.setattr("borgmarks.cli.classify_bookmarks", _classify_guard)
    monkeypatch.setattr("borgmarks.cli.enrich_folder_emojis", _folder_emoji_guard)
    monkeypatch.setattr("borgmarks.tagging.suggest_tags_for_tree", _tag_openai_guard)

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
    assert called == {"classify": 0, "folder_emoji": 0, "tag_openai": 0}


def test_cli_skips_folder_emoji_when_no_newly_classified_links(tmp_path: Path, monkeypatch):
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
            categories=["💻 Computers", "Dev"],
            status_code=200,
            visited_at="2026-02-09T00:00:00+00:00",
            # one missing summary keeps openai_enabled=True
            summary=None if i == 0 else "cached summary",
            html=None,
            page_title=None,
            page_description=None,
            content_snippet=None,
            icon_url=None,
        )
        for i, url in enumerate(urls)
    ]
    upsert_entries(cache_db, rows)

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("OPENAI_KEY", raising=False)
    monkeypatch.setattr("borgmarks.cli.classify_bookmarks", lambda *_a, **_kw: set())
    monkeypatch.setattr("borgmarks.cli.enrich_bookmark_tags", lambda *_a, **_kw: None)

    called = {"emoji": 0}

    def _emoji_guard(*_a, **_kw):
        called["emoji"] += 1
        raise AssertionError("enrich_folder_emojis should be skipped when no newly assigned ids")

    monkeypatch.setattr("borgmarks.cli.enrich_folder_emojis", _emoji_guard)

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
    assert called["emoji"] == 0


def test_cli_removed_input_link_is_not_restored_from_cache(tmp_path: Path):
    profile = tmp_path / "profile"
    _mk_profile(profile)
    cache_db = profile / "borg_cache.sqlite"

    urls_a = [
        "https://alpha.example.com/",
        "https://beta.example.com/",
        "https://gamma.example.com/",
    ]
    urls_b = [
        "https://alpha.example.com/",
        "https://gamma.example.com/",
    ]
    removed = "https://beta.example.com/"

    ios_a = tmp_path / "ios-a.html"
    ios_b = tmp_path / "ios-b.html"
    _write_bookmarks_html(ios_a, urls_a)
    _write_bookmarks_html(ios_b, urls_b)

    rc1 = main(
        [
            "organize",
            "--ios-html",
            str(ios_a),
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc1 == 0

    removed_key = _url_identity(removed)
    assert removed_key in load_entries(cache_db, [removed_key])

    rc2 = main(
        [
            "organize",
            "--ios-html",
            str(ios_b),
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
        ]
    )
    assert rc2 == 0

    out_html = (profile / "bookmarks.organized.html").read_text(encoding="utf-8")
    out_meta = (profile / "bookmarks.organized.meta.jsonl").read_text(encoding="utf-8")
    assert removed not in out_html
    assert removed not in out_meta
    for u in urls_b:
        assert u in out_html
        assert u in out_meta

    # Cache keeps old entries; output is derived from current input set.
    assert removed_key in load_entries(cache_db, [removed_key])


def test_cli_skip_cache_rebuild_uses_only_current_input_links(tmp_path: Path):
    profile = tmp_path / "profile"
    _mk_profile(profile)

    # Seed a stale cache first.
    ios_old = tmp_path / "ios-old.html"
    old_urls = [
        "https://old-one.example.com/",
        "https://old-two.example.com/",
    ]
    _write_bookmarks_html(ios_old, old_urls)
    rc_seed = main(
        [
            "organize",
            "--ios-html",
            str(ios_old),
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc_seed == 0

    # Re-run with different input and force cache recreation.
    ios_new = tmp_path / "ios-new.html"
    new_urls = [
        "https://new-one.example.com/",
        "https://new-two.example.com/",
        "https://new-three.example.com/",
    ]
    _write_bookmarks_html(ios_new, new_urls)
    rc = main(
        [
            "organize",
            "--ios-html",
            str(ios_new),
            "--firefox-profile",
            str(profile),
            "--no-openai",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc == 0

    out_meta = profile / "bookmarks.organized.meta.jsonl"
    meta_rows = [json.loads(x) for x in out_meta.read_text(encoding="utf-8").splitlines() if x.strip()]
    out_urls = {row.get("url") for row in meta_rows}
    assert out_urls == set(new_urls)


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


def test_cli_apply_firefox_second_run_does_not_grow_bookmark_count(tmp_path: Path):
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
            "--apply-firefox",
            "--no-openai",
            "--no-fetch",
            "--skip-cache",
        ]
    )
    assert rc1 == 0
    with PlacesDB(profile / "places.sqlite", readonly=True) as db:
        c1 = len(db.read_all(include_tag_links=False))

    rc2 = main(
        [
            "organize",
            "--ios-html",
            str(ios),
            "--firefox-profile",
            str(profile),
            "--apply-firefox",
            "--no-openai",
            "--no-fetch",
        ]
    )
    assert rc2 == 0
    with PlacesDB(profile / "places.sqlite", readonly=True) as db:
        c2 = len(db.read_all(include_tag_links=False))

    assert c2 == c1


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
