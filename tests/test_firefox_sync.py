import sqlite3
from pathlib import Path

from borgmarks.firefox_sync import apply_bookmarks_to_firefox
from borgmarks.model import Bookmark
from borgmarks.places_db import PlacesDB


def _mk_db(path: Path) -> None:
    conn = sqlite3.connect(path)
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
                (10, 2, None, 2, 0, "Old", 0, 0, "oldfolder"),
            ],
        )
        conn.executemany(
            "INSERT INTO moz_places(id,url,title,hidden,guid,foreign_count) VALUES(?,?,?,?,?,?)",
            [(100, "https://example.com/a", "A", 0, "p100", 1)],
        )
        conn.executemany(
            "INSERT INTO moz_bookmarks(id,type,fk,parent,position,title,dateAdded,lastModified,guid) VALUES(?,?,?,?,?,?,?,?,?)",
            [(20, 1, 100, 10, 0, "A-old", 0, 0, "l20")],
        )
        conn.commit()
    finally:
        conn.close()


def _mk_favicons_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE moz_pages_w_icons (
              id INTEGER PRIMARY KEY,
              page_url TEXT UNIQUE,
              page_url_hash INTEGER
            );
            CREATE TABLE moz_icons (
              id INTEGER PRIMARY KEY,
              icon_url TEXT UNIQUE,
              fixed_icon_url_hash INTEGER,
              width INTEGER,
              root INTEGER,
              expire_ms INTEGER,
              flags INTEGER,
              data BLOB
            );
            CREATE TABLE moz_icons_to_pages (
              page_id INTEGER NOT NULL,
              icon_id INTEGER NOT NULL,
              expire_ms INTEGER,
              PRIMARY KEY(page_id, icon_id),
              FOREIGN KEY(page_id) REFERENCES moz_pages_w_icons(id) ON DELETE CASCADE,
              FOREIGN KEY(icon_id) REFERENCES moz_icons(id) ON DELETE CASCADE
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _mk_favicons_db_with_duplicates(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE moz_pages_w_icons (
              id INTEGER PRIMARY KEY,
              page_url TEXT,
              page_url_hash INTEGER
            );
            CREATE TABLE moz_icons (
              id INTEGER PRIMARY KEY,
              icon_url TEXT,
              fixed_icon_url_hash INTEGER,
              width INTEGER,
              root INTEGER,
              expire_ms INTEGER,
              flags INTEGER,
              data BLOB
            );
            CREATE TABLE moz_icons_to_pages (
              page_id INTEGER NOT NULL,
              icon_id INTEGER NOT NULL,
              expire_ms INTEGER
            );
            """
        )
        conn.executemany(
            "INSERT INTO moz_pages_w_icons(id,page_url,page_url_hash) VALUES(?,?,?)",
            [
                (1, "https://example.com/a", 111),
                (2, "https://example.com/a", 111),
            ],
        )
        conn.executemany(
            "INSERT INTO moz_icons(id,icon_url,fixed_icon_url_hash,width,root,expire_ms,flags,data) VALUES(?,?,?,?,?,?,?,?)",
            [
                (10, "https://example.com/favicon.ico", 222, 16, 1, 123, 0, None),
                (11, "https://example.com/favicon.ico", 222, 16, 1, 456, 0, None),
            ],
        )
        conn.executemany(
            "INSERT INTO moz_icons_to_pages(page_id,icon_id,expire_ms) VALUES(?,?,?)",
            [
                (1, 10, 123),
                (2, 11, 456),
                (1, 10, 789),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_apply_bookmarks_to_firefox_is_idempotent_and_handles_toolbar_menu(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    fav_path = tmp_path / "favicons.sqlite"
    _mk_db(db_path)
    _mk_favicons_db(fav_path)

    b1 = Bookmark(id="b1", title="A", url="https://example.com/a")
    b1.assigned_path = ["Bookmarks Toolbar", "Shopping", "👕 Clothing"]
    b1.tags = ["video"]
    b1.meta["icon_uri"] = "https://example.com/favicon.ico"

    b2 = Bookmark(id="b2", title="B", url="https://example.com/b")
    b2.assigned_path = ["Shopping", "Clothing"]  # Defaults to Bookmarks Menu root.
    b2.tags = ["camera"]
    b2.meta["icon_uri"] = "https://example.com/favicon.ico"

    s1 = apply_bookmarks_to_firefox(db_path, [b1, b2], favicons_db_path=fav_path)
    assert s1.touched_links == 2
    assert s1.added_links == 1
    assert s1.moved_links == 1
    assert s1.icon_links == 2

    s2 = apply_bookmarks_to_firefox(db_path, [b1, b2], favicons_db_path=fav_path)
    assert s2.added_links == 0
    assert s2.moved_links == 0
    assert s2.icon_links == 0

    with PlacesDB(db_path, readonly=True) as db:
        links = {x.url: x for x in db.read_all(include_tag_links=False)}
        assert links["https://example.com/a"].path == ["Bookmarks Toolbar", "Shopping", "👕 Clothing"]
        assert links["https://example.com/b"].path == ["Bookmarks Menu", "Shopping", "Clothing"]
        assert "video" in links["https://example.com/a"].tags
        assert "camera" in links["https://example.com/b"].tags

    with sqlite3.connect(db_path) as conn:
        # keep sqlite references coherent after sync
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        # existing A + one tag-ref for A + one link for B + one tag-ref for B
        fc_a = int(conn.execute("SELECT foreign_count FROM moz_places WHERE url = 'https://example.com/a'").fetchone()[0])
        fc_b = int(conn.execute("SELECT foreign_count FROM moz_places WHERE url = 'https://example.com/b'").fetchone()[0])
        assert fc_a >= 2
        assert fc_b >= 2

    with sqlite3.connect(fav_path) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        rows = conn.execute("SELECT COUNT(*) FROM moz_icons_to_pages").fetchone()[0]
        assert int(rows) == 2


def test_apply_bookmarks_dedupes_existing_duplicates_in_places_and_favicons(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    fav_path = tmp_path / "favicons.sqlite"
    _mk_db(db_path)
    _mk_favicons_db_with_duplicates(fav_path)

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO moz_places(id,url,title,hidden,guid,foreign_count) VALUES(?,?,?,?,?,?)",
            [
                (101, "https://example.com/a", "A-dup", 0, "p101", 1),
            ],
        )
        conn.executemany(
            "INSERT INTO moz_bookmarks(id,type,fk,parent,position,title,dateAdded,lastModified,guid) VALUES(?,?,?,?,?,?,?,?,?)",
            [
                (21, 1, 101, 2, 1, "A-menu-dup", 0, 0, "l21"),
                (22, 1, 100, 10, 1, "A-same-folder-dup", 0, 0, "l22"),
            ],
        )
        conn.commit()

    b = Bookmark(id="b1", title="A", url="https://example.com/a")
    b.assigned_path = ["Bookmarks Toolbar", "Shopping", "👕 Clothing"]
    b.tags = ["video"]
    b.meta["icon_uri"] = "https://example.com/favicon.ico"

    s = apply_bookmarks_to_firefox(db_path, [b], favicons_db_path=fav_path)
    assert s.deduped_bookmark_rows > 0
    assert s.deduped_favicon_rows > 0

    with PlacesDB(db_path, readonly=True) as db:
        links = [x for x in db.read_all(include_tag_links=False) if x.url == "https://example.com/a"]
        assert len(links) == 1

    with sqlite3.connect(fav_path) as conn:
        page_cnt = int(
            conn.execute("SELECT COUNT(*) FROM moz_pages_w_icons WHERE page_url = 'https://example.com/a'").fetchone()[0]
        )
        icon_cnt = int(
            conn.execute("SELECT COUNT(*) FROM moz_icons WHERE icon_url = 'https://example.com/favicon.ico'").fetchone()[0]
        )
        map_cnt = int(conn.execute("SELECT COUNT(*) FROM moz_icons_to_pages").fetchone()[0])
        assert page_cnt == 1
        assert icon_cnt == 1
        assert map_cnt == 1


def test_apply_bookmarks_keeps_links_when_favicon_phase_fails(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "places.sqlite"
    fav_path = tmp_path / "favicons.sqlite"
    _mk_db(db_path)
    _mk_favicons_db(fav_path)

    def _boom(*_args, **_kwargs):
        raise RuntimeError("favicon write failed")

    monkeypatch.setattr("borgmarks.favicons_db.FaviconsDB.set_page_icon", _boom)

    b = Bookmark(id="b1", title="A", url="https://example.com/new-link")
    b.assigned_path = ["Bookmarks Menu", "Tools"]
    b.meta["icon_uri"] = "https://example.com/favicon.ico"

    s = apply_bookmarks_to_firefox(db_path, [b], favicons_db_path=fav_path, apply_icons=True)
    assert s.added_links == 1
    assert s.icon_links == 0
    assert s.icon_errors == 1

    with PlacesDB(db_path, readonly=True) as db:
        urls = {x.url for x in db.read_all(include_tag_links=False)}
        assert "https://example.com/new-link" in urls
