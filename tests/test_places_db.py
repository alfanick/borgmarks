import sqlite3
from pathlib import Path

import pytest

from borgmarks.places_db import PlacesDB


def _mk_places_db(path: Path, *, with_roots_table: bool = True) -> None:
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
            """
        )
        if with_roots_table:
            conn.execute(
                "CREATE TABLE moz_bookmarks_roots (root_name TEXT PRIMARY KEY, folder_id INTEGER)"
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
                (10, 2, None, 3, 0, "Shopping", 0, 0, "folder-shop"),
                (11, 2, None, 10, 0, "Camera", 0, 0, "folder-cam"),
                (30, 2, None, 4, 0, "video", 0, 0, "tag-video"),
            ],
        )
        conn.executemany(
            "INSERT INTO moz_places(id,url,title,hidden,guid,foreign_count) VALUES(?,?,?,?,?,?)",
            [
                (100, "https://fstoppers.com/camera", "fstoppers", 0, "p100", 2),
                (101, "https://www.mozilla.org/", "mozilla", 0, "p101", 1),
            ],
        )
        conn.executemany(
            "INSERT INTO moz_bookmarks(id,type,fk,parent,position,title,dateAdded,lastModified,guid) VALUES(?,?,?,?,?,?,?,?,?)",
            [
                (20, 1, 100, 11, 0, "Fstoppers Camera", 0, 0, "l20"),
                (21, 1, 101, 2, 0, "Mozilla", 0, 0, "l21"),
                (31, 1, 100, 30, 0, "Fstoppers Camera [tag]", 0, 0, "l31"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_read_all_folders_folder_and_tags(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    _mk_places_db(db_path)
    with PlacesDB(db_path, readonly=True) as db:
        links = db.read_all(include_tag_links=False)
        assert len(links) == 2
        fst = next(x for x in links if x.url == "https://fstoppers.com/camera")
        assert fst.path == ["Bookmarks Toolbar", "Shopping", "Camera"]
        assert fst.tags == ["video"]

        folders = db.read_folders()
        assert any(f.path == ["Bookmarks Toolbar", "Shopping"] for f in folders)
        assert len(db.read_foloders()) == len(folders)

        menu_id = db.get_root_folder_id("menu")
        fv = db.read_folder(menu_id)
        assert fv.folder.path == ["Bookmarks Menu"]
        assert any(link.url == "https://www.mozilla.org/" for link in fv.links)


def test_add_folder_dedupes_emoji_and_plain_names(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    _mk_places_db(db_path)
    with PlacesDB(db_path, readonly=False) as db:
        shopping = db.ensure_folder_path(db.get_root_folder_id("toolbar"), ["Shopping"])
        f1 = db.add_folder(shopping, "👕 Clothing")
        f2 = db.add_folder(shopping, "Clothing")
        assert f1 == f2

        f3 = db.ensure_folder_path(
            db.get_root_folder_id("toolbar"),
            ["Shopping", "👕 Clothing"],
        )
        f4 = db.ensure_folder_path(
            db.get_root_folder_id("toolbar"),
            ["Shopping", "Clothing"],
        )
        assert f3 == f4


def test_add_folder_upgrades_plain_name_to_emoji_variant(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    _mk_places_db(db_path)
    with PlacesDB(db_path, readonly=False) as db:
        shopping = db.ensure_folder_path(db.get_root_folder_id("toolbar"), ["Shopping"])
        fid_plain = db.add_folder(shopping, "Clothing")
        fid_emoji = db.add_folder(shopping, "👕 Clothing")
        assert fid_plain == fid_emoji
        row = db.conn.execute("SELECT title FROM moz_bookmarks WHERE id = ?", (fid_plain,)).fetchone()
        assert row is not None
        assert (row[0] or "").strip() == "👕 Clothing"


def test_ensure_folder_path_handles_toolbar_and_menu_roots(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    _mk_places_db(db_path)
    with PlacesDB(db_path, readonly=False) as db:
        p1 = db.ensure_folder_path(1, ["Bookmarks Toolbar", "Photography"])
        p2 = db.ensure_folder_path(1, ["toolbar", "Photography"])
        assert p1 == p2

        m1 = db.ensure_folder_path(1, ["Bookmarks Menu", "Reading"])
        m2 = db.ensure_folder_path(1, ["menu", "Reading"])
        assert m1 == m2


def test_add_link_move_link_and_tag_are_idempotent(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    _mk_places_db(db_path)
    with PlacesDB(db_path, readonly=False) as db:
        menu = db.get_root_folder_id("menu")
        shopping = db.ensure_folder_path(menu, ["Shopping", "Camera"])

        link_id = db.add_link(shopping, "https://example.com/x", "Example X", tags=["video"])
        link_id_again = db.add_link(shopping, "https://example.com/x", "Example X", tags=["video"])
        assert link_id == link_id_again

        toolbar = db.get_root_folder_id("toolbar")
        dst = db.ensure_folder_path(toolbar, ["Shopping", "Camera"])
        db.move_link(link_id, dst)
        db.move_link(link_id, dst)

        before = len(db.read_tag("video"))
        db.add_link_tag(link_id, "video")
        db.add_link_tag(link_id, "video")
        after = len(db.read_tag("video"))
        assert after == before or after == before + 1


def test_negative_cases(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    _mk_places_db(db_path)
    with PlacesDB(db_path, readonly=False) as db:
        with pytest.raises(ValueError):
            db.add_folder(db.get_root_folder_id("menu"), "")
        with pytest.raises(ValueError):
            db.add_link(db.get_root_folder_id("menu"), "", "x")
        with pytest.raises(ValueError):
            db.read_folder(999999)
        with pytest.raises(ValueError):
            db.move_folder(db.get_root_folder_id("menu"), db.get_root_folder_id("toolbar"))

        parent = db.ensure_folder_path(db.get_root_folder_id("menu"), ["X"])
        child = db.ensure_folder_path(parent, ["Y"])
        with pytest.raises(ValueError):
            db.move_folder(parent, child)
        with pytest.raises(ValueError):
            db.move_link(999999, parent)
        with pytest.raises(ValueError):
            db.add_tag("")


def test_root_guid_fallback_without_roots_table(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    _mk_places_db(db_path, with_roots_table=False)
    with PlacesDB(db_path, readonly=True) as db:
        assert db.get_root_folder_id("toolbar") == 3
        assert db.get_root_folder_id("menu") == 2


def test_recompute_foreign_count_and_integrity_check(tmp_path: Path):
    db_path = tmp_path / "places.sqlite"
    _mk_places_db(db_path)
    with PlacesDB(db_path, readonly=False) as db:
        # Corrupt a counter on purpose, then recompute.
        db.conn.execute("UPDATE moz_places SET foreign_count = 999 WHERE id = 100")
        db.conn.commit()
        db.recompute_foreign_count()
        row = db.conn.execute("SELECT foreign_count FROM moz_places WHERE id = 100").fetchone()
        # place 100 has 2 references in fixture: one normal link + one tag link
        assert int(row[0]) == 2
        db.validate_integrity()
