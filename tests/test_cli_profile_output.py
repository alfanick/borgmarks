import sqlite3
from pathlib import Path

from borgmarks.cli import main


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
