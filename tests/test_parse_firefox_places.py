import sqlite3
from pathlib import Path

from borgmarks.parse_firefox_places import parse_firefox_places


def _mk_min_places_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE moz_places (
              id INTEGER PRIMARY KEY,
              url TEXT,
              title TEXT,
              hidden INTEGER DEFAULT 0
            );
            CREATE TABLE moz_bookmarks (
              id INTEGER PRIMARY KEY,
              type INTEGER,
              fk INTEGER,
              parent INTEGER,
              position INTEGER,
              title TEXT,
              dateAdded INTEGER,
              lastModified INTEGER
            );
            CREATE TABLE moz_bookmarks_roots (
              root_name TEXT PRIMARY KEY,
              folder_id INTEGER
            );
            """
        )
        conn.executemany(
            "INSERT INTO moz_bookmarks_roots(root_name, folder_id) VALUES(?, ?)",
            [
                ("toolbar", 2),
                ("menu", 3),
                ("tags", 4),
                ("unfiled", 5),
                ("mobile", 6),
            ],
        )
        conn.executemany(
            "INSERT INTO moz_bookmarks(id,type,fk,parent,position,title,dateAdded,lastModified) VALUES(?,?,?,?,?,?,?,?)",
            [
                (1, 2, None, 0, 0, "root", 0, 0),
                (2, 2, None, 1, 0, "toolbar", 0, 0),
                (3, 2, None, 1, 1, "menu", 0, 0),
                (4, 2, None, 1, 2, "tags", 0, 0),
                (5, 2, None, 1, 3, "unfiled", 0, 0),
                (6, 2, None, 1, 4, "mobile", 0, 0),
                (10, 2, None, 2, 0, "Camera", 0, 0),
                (20, 1, 100, 10, 0, "Fstoppers Camera", 1700000000000000, 1700000000000000),
                (21, 1, 101, 3, 0, "Mozilla", 1700000000000000, 1700000000000000),
                (22, 1, 102, 3, 1, "Smart Query", 1700000000000000, 1700000000000000),
                (30, 2, None, 4, 0, "video", 0, 0),
                (31, 1, 100, 30, 0, "Fstoppers Camera [tag copy]", 1700000000000000, 1700000000000000)
            ],
        )
        conn.executemany(
            "INSERT INTO moz_places(id,url,title,hidden) VALUES(?,?,?,?)",
            [
                (100, "https://fstoppers.com/cameras", "fstoppers", 0),
                (101, "https://www.mozilla.org/", "mozilla", 0),
                (102, "place:type=7&sort=8", "smart", 0),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _mk_places_db_with_root_guids(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE moz_places (
              id INTEGER PRIMARY KEY,
              url TEXT,
              title TEXT,
              hidden INTEGER DEFAULT 0
            );
            CREATE TABLE moz_bookmarks (
              id INTEGER PRIMARY KEY,
              type INTEGER,
              fk INTEGER,
              parent INTEGER,
              position INTEGER,
              title TEXT,
              guid TEXT,
              dateAdded INTEGER,
              lastModified INTEGER
            );
            """
        )
        conn.executemany(
            "INSERT INTO moz_bookmarks(id,type,fk,parent,position,title,guid,dateAdded,lastModified) VALUES(?,?,?,?,?,?,?,?,?)",
            [
                (1, 2, None, 0, 0, "root", "root________", 0, 0),
                (2, 2, None, 1, 0, "menu", "menu________", 0, 0),
                (3, 2, None, 1, 1, "toolbar", "toolbar_____", 0, 0),
                (4, 2, None, 1, 2, "tags", "tags________", 0, 0),
                (5, 2, None, 1, 3, "unfiled", "unfiled_____", 0, 0),
                (6, 2, None, 1, 4, "mobile", "mobile______", 0, 0),
                (10, 2, None, 3, 0, "Video", "foldervideo01", 0, 0),
                (20, 1, 100, 10, 0, "Fstoppers Video", "bookmarkvideo", 1700000000000000, 1700000000000000),
            ],
        )
        conn.execute(
            "INSERT INTO moz_places(id,url,title,hidden) VALUES(?,?,?,?)",
            (100, "https://fstoppers.com/video", "fstoppers", 0),
        )
        conn.commit()
    finally:
        conn.close()


def test_parse_firefox_places_merges_paths_and_tags(tmp_path: Path):
    profile = tmp_path / "fx"
    profile.mkdir()
    db = profile / "places.sqlite"
    _mk_min_places_db(db)

    bookmarks = parse_firefox_places(profile)
    urls = {b.url for b in bookmarks}
    assert "https://fstoppers.com/cameras" in urls
    assert "https://www.mozilla.org/" in urls
    assert all(not b.url.startswith("place:") for b in bookmarks)

    fst = next(b for b in bookmarks if b.url == "https://fstoppers.com/cameras")
    assert fst.folder_path == ["Bookmarks Toolbar", "Camera"]
    assert fst.tags == ["video"]

    moz = next(b for b in bookmarks if b.url == "https://www.mozilla.org/")
    assert moz.folder_path == ["Bookmarks Menu"]


def test_parse_firefox_places_supports_root_guid_fallback_without_roots_table(tmp_path: Path):
    profile = tmp_path / "fx-guid"
    profile.mkdir()
    db = profile / "places.sqlite"
    _mk_places_db_with_root_guids(db)

    bookmarks = parse_firefox_places(profile)
    assert len(bookmarks) == 1
    b = bookmarks[0]
    assert b.url == "https://fstoppers.com/video"
    assert b.folder_path == ["Bookmarks Toolbar", "Video"]
