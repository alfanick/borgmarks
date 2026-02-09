from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .url_norm import normalize_url

_ROOT_LABELS = {
    "toolbar": "Bookmarks Toolbar",
    "menu": "Bookmarks Menu",
    "unfiled": "Other Bookmarks",
    "mobile": "Mobile Bookmarks",
    "tags": "Tags",
}

_ROOT_GUID_TO_NAME = {
    "menu________": "menu",
    "toolbar_____": "toolbar",
    "tags________": "tags",
    "unfiled_____": "unfiled",
    "mobile______": "mobile",
}

_ROOT_ALIASES = {
    "bookmarkstoolbar": "toolbar",
    "toolbar": "toolbar",
    "bookmarksmenu": "menu",
    "menu": "menu",
    "otherbookmarks": "unfiled",
    "unfiled": "unfiled",
    "mobilebookmarks": "mobile",
    "mobile": "mobile",
    "tags": "tags",
}


@dataclass
class FolderEntry:
    id: int
    parent_id: int
    title: str
    path: List[str]
    is_root: bool


@dataclass
class LinkEntry:
    id: int
    parent_id: int
    place_id: int
    title: str
    url: str
    path: List[str]
    tags: List[str]


@dataclass
class FolderView:
    folder: FolderEntry
    folders: List[FolderEntry]
    links: List[LinkEntry]


class PlacesDB:
    def __init__(self, db_path: Path | str, *, readonly: bool = False, busy_timeout_ms: int = 5000):
        self.db_path = Path(db_path)
        self.readonly = readonly
        self.busy_timeout_ms = max(0, int(busy_timeout_ms))
        self.conn: sqlite3.Connection | None = None
        self._has_guid = False
        self._has_foreign_count = False
        self._has_url_hash = False
        self.root_ids: Dict[str, int] = {}

    def __enter__(self) -> "PlacesDB":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def open(self) -> None:
        mode = "ro" if self.readonly else "rw"
        uri = f"file:{self.db_path.as_posix()}?mode={mode}"
        timeout_s = max(0.1, self.busy_timeout_ms / 1000.0) if self.busy_timeout_ms > 0 else 0.1
        self.conn = sqlite3.connect(uri, uri=True, timeout=timeout_s)
        self.conn.row_factory = sqlite3.Row
        if self.busy_timeout_ms > 0:
            self.conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._has_guid = self._has_column("moz_bookmarks", "guid")
        self._has_foreign_count = self._has_column("moz_places", "foreign_count")
        self._has_url_hash = self._has_column("moz_places", "url_hash")
        self.root_ids = self._discover_root_ids()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def get_root_folder_id(self, name: str) -> Optional[int]:
        return self.root_ids.get(name)

    def get_place_url_hash(self, url: str) -> Optional[int]:
        norm = normalize_url(url or "")
        if not norm or not self._has_url_hash:
            return None
        c = self._cursor()
        row = c.execute(
            "SELECT url_hash FROM moz_places WHERE url = ? LIMIT 1",
            (norm,),
        ).fetchone()
        if not row:
            return None
        value = row["url_hash"]
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    def read_all(self, *, include_tag_links: bool = False) -> List[LinkEntry]:
        c = self._cursor()
        rows = c.execute(
            """
            SELECT b.id, b.fk, b.parent, b.type, b.title, p.url, p.hidden
            FROM moz_bookmarks b
            LEFT JOIN moz_places p ON p.id = b.fk
            WHERE b.type = 1
            ORDER BY b.id
            """
        ).fetchall()
        parent_map, title_map, type_map = self._bookmark_tree_maps()
        tags_by_fk = self._tag_names_by_fk(rows, parent_map)

        tags_root_id = self.root_ids.get("tags")
        out: List[LinkEntry] = []
        for r in rows:
            url = (r["url"] or "").strip()
            if not url or url.startswith("place:") or int(r["hidden"] or 0) != 0:
                continue
            row_id = int(r["id"])
            if not include_tag_links and tags_root_id is not None and self._descends_from(row_id, tags_root_id, parent_map):
                continue
            fk = int(r["fk"] or 0)
            path = self._folder_path(int(r["parent"] or 0), parent_map, title_map, type_map)
            out.append(
                LinkEntry(
                    id=row_id,
                    parent_id=int(r["parent"] or 0),
                    place_id=fk,
                    title=(r["title"] or "").strip() or url,
                    url=url,
                    path=path,
                    tags=sorted(tags_by_fk.get(fk, set())),
                )
            )
        return out

    # User requested spelling support.
    def read_foloders(self) -> List[FolderEntry]:
        return self.read_folders()

    def read_folders(self) -> List[FolderEntry]:
        c = self._cursor()
        rows = c.execute(
            "SELECT id, parent, title, type FROM moz_bookmarks WHERE type = 2 ORDER BY id"
        ).fetchall()
        parent_map, title_map, type_map = self._bookmark_tree_maps()
        out: List[FolderEntry] = []
        for r in rows:
            fid = int(r["id"])
            out.append(
                FolderEntry(
                    id=fid,
                    parent_id=int(r["parent"] or 0),
                    title=(r["title"] or "").strip(),
                    path=self._folder_path(fid, parent_map, title_map, type_map),
                    is_root=fid in self.root_ids.values(),
                )
            )
        return out

    def read_folder(self, folder_id: int) -> FolderView:
        self._require_folder(folder_id)
        parent_map, title_map, type_map = self._bookmark_tree_maps()
        folder = FolderEntry(
            id=folder_id,
            parent_id=int(parent_map.get(folder_id, 0)),
            title=(title_map.get(folder_id, "") or "").strip(),
            path=self._folder_path(folder_id, parent_map, title_map, type_map),
            is_root=folder_id in self.root_ids.values(),
        )
        c = self._cursor()
        child_folders = c.execute(
            "SELECT id, parent, title FROM moz_bookmarks WHERE type = 2 AND parent = ? ORDER BY position, id",
            (folder_id,),
        ).fetchall()
        child_links = c.execute(
            """
            SELECT b.id, b.parent, b.fk, b.title, p.url, p.hidden
            FROM moz_bookmarks b
            LEFT JOIN moz_places p ON p.id = b.fk
            WHERE b.type = 1 AND b.parent = ?
            ORDER BY b.position, b.id
            """,
            (folder_id,),
        ).fetchall()

        tag_map = self.read_tags()
        tags_by_fk = self._tag_fks_to_names(tag_map)

        folders: List[FolderEntry] = []
        for r in child_folders:
            fid = int(r["id"])
            folders.append(
                FolderEntry(
                    id=fid,
                    parent_id=int(r["parent"] or 0),
                    title=(r["title"] or "").strip(),
                    path=self._folder_path(fid, parent_map, title_map, type_map),
                    is_root=fid in self.root_ids.values(),
                )
            )
        links: List[LinkEntry] = []
        for r in child_links:
            url = (r["url"] or "").strip()
            if not url or url.startswith("place:") or int(r["hidden"] or 0) != 0:
                continue
            fk = int(r["fk"] or 0)
            links.append(
                LinkEntry(
                    id=int(r["id"]),
                    parent_id=int(r["parent"] or 0),
                    place_id=fk,
                    title=(r["title"] or "").strip() or url,
                    url=url,
                    path=self._folder_path(folder_id, parent_map, title_map, type_map),
                    tags=sorted(tags_by_fk.get(fk, set())),
                )
            )
        return FolderView(folder=folder, folders=folders, links=links)

    def add_folder(self, parent_id: int, title: str, position: Optional[int] = None) -> int:
        self._assert_writable()
        self._require_folder(parent_id)
        name = (title or "").strip()
        if not name:
            raise ValueError("folder title cannot be empty")
        c = self._cursor()
        siblings = c.execute(
            "SELECT id, title FROM moz_bookmarks WHERE type = 2 AND parent = ? ORDER BY id",
            (parent_id,),
        ).fetchall()
        target_key = _folder_component_key(name)
        row = None
        for s in siblings:
            if _folder_component_key((s["title"] or "").strip()) == target_key:
                row = s
                break
        if row:
            existing_title = (row["title"] or "").strip()
            # Prefer emoji-prefixed folder names when caller explicitly asks for one.
            # This keeps "Clothing" and "👕 Clothing" as a single folder while still
            # allowing gradual emoji enrichment on existing trees.
            if _has_leading_emoji(name) and not _has_leading_emoji(existing_title):
                c.execute(
                    "UPDATE moz_bookmarks SET title = ?, lastModified = ? WHERE id = ?",
                    (name, self._now_us(), int(row["id"])),
                )
                self._touch_folder(parent_id)
                self.conn.commit()
            return int(row["id"])

        pos = self._resolve_position(parent_id, position)
        now = self._now_us()
        cols = ["type", "fk", "parent", "position", "title", "dateAdded", "lastModified"]
        vals: List[object] = [2, None, parent_id, pos, name, now, now]
        if self._has_guid:
            cols.append("guid")
            vals.append(self._new_guid())
        placeholders = ", ".join(["?"] * len(vals))
        c.execute(
            f"INSERT INTO moz_bookmarks ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        new_id = int(c.lastrowid)
        self._touch_folder(parent_id)
        self.conn.commit()
        return new_id

    def ensure_folder_path(self, parent_id: int, folder_path: Iterable[str]) -> int:
        comps = [str(x).strip() for x in folder_path if str(x).strip()]
        cur = parent_id
        if comps:
            maybe_root_id = self._resolve_root_alias(comps[0])
            if maybe_root_id is not None:
                cur = maybe_root_id
                comps = comps[1:]
        for comp in comps:
            name = (comp or "").strip()
            if not name:
                continue
            cur = self.add_folder(cur, name)
        return cur

    def move_folder(self, folder_id: int, new_parent_id: int, position: Optional[int] = None) -> None:
        self._assert_writable()
        self._require_folder(folder_id)
        self._require_folder(new_parent_id)
        if folder_id in self.root_ids.values():
            raise ValueError("cannot move Firefox root folders")

        parent_map, _, _ = self._bookmark_tree_maps()
        old_parent = int(parent_map.get(folder_id, 0))
        if self._descends_from(new_parent_id, folder_id, parent_map):
            raise ValueError("cannot move folder into itself/descendant")
        pos = self._resolve_position(new_parent_id, position)

        c = self._cursor()
        c.execute(
            "UPDATE moz_bookmarks SET parent = ?, position = ?, lastModified = ? WHERE id = ?",
            (new_parent_id, pos, self._now_us(), folder_id),
        )
        self._touch_folder(old_parent)
        self._touch_folder(new_parent_id)
        self.conn.commit()

    def add_link(
        self,
        parent_id: int,
        url: str,
        title: str,
        *,
        tags: Optional[Iterable[str]] = None,
        position: Optional[int] = None,
    ) -> int:
        self._assert_writable()
        self._require_folder(parent_id)
        norm_url = normalize_url(url or "")
        if not norm_url:
            raise ValueError("link URL cannot be empty")
        display_title = (title or "").strip() or norm_url
        place_id = self._ensure_place(norm_url, display_title)

        c = self._cursor()
        row = c.execute(
            "SELECT id, title FROM moz_bookmarks WHERE type = 1 AND parent = ? AND fk = ? ORDER BY id LIMIT 1",
            (parent_id, place_id),
        ).fetchone()
        if row:
            link_id = int(row["id"])
            if display_title and (row["title"] or "") != display_title:
                c.execute(
                    "UPDATE moz_bookmarks SET title = ?, lastModified = ? WHERE id = ?",
                    (display_title, self._now_us(), link_id),
                )
        else:
            pos = self._resolve_position(parent_id, position)
            link_id = self._insert_bookmark(
                btype=1,
                fk=place_id,
                parent_id=parent_id,
                position=pos,
                title=display_title,
            )
        if tags:
            for t in tags:
                tag = (t or "").strip()
                if tag:
                    self.add_link_tag(link_id, tag)
        self.conn.commit()
        return link_id

    def move_link(self, link_id: int, new_parent_id: int, position: Optional[int] = None) -> None:
        self._assert_writable()
        self._require_link(link_id)
        self._require_folder(new_parent_id)
        c = self._cursor()
        row = c.execute("SELECT parent FROM moz_bookmarks WHERE id = ?", (link_id,)).fetchone()
        old_parent = int(row["parent"] or 0)
        pos = self._resolve_position(new_parent_id, position)
        c.execute(
            "UPDATE moz_bookmarks SET parent = ?, position = ?, lastModified = ? WHERE id = ?",
            (new_parent_id, pos, self._now_us(), link_id),
        )
        self._touch_folder(old_parent)
        self._touch_folder(new_parent_id)
        self.conn.commit()

    def read_tags(self) -> Dict[str, List[int]]:
        tags_root = self.root_ids.get("tags")
        if tags_root is None:
            return {}
        c = self._cursor()
        tag_folders = c.execute(
            "SELECT id, title FROM moz_bookmarks WHERE type = 2 AND parent = ? ORDER BY title, id",
            (tags_root,),
        ).fetchall()
        out: Dict[str, List[int]] = {}
        for tf in tag_folders:
            tag_name = (tf["title"] or "").strip()
            if not tag_name:
                continue
            refs = c.execute(
                "SELECT fk FROM moz_bookmarks WHERE type = 1 AND parent = ? AND fk IS NOT NULL ORDER BY id",
                (int(tf["id"]),),
            ).fetchall()
            out[tag_name] = [int(x["fk"]) for x in refs]
        return out

    def read_tag(self, tag_name: str) -> List[int]:
        return self.read_tags().get((tag_name or "").strip(), [])

    def add_tag(self, tag_name: str) -> int:
        self._assert_writable()
        tags_root = self.root_ids.get("tags")
        if tags_root is None:
            raise ValueError("tags root folder not found")
        name = (tag_name or "").strip()
        if not name:
            raise ValueError("tag name cannot be empty")
        return self.add_folder(tags_root, name)

    def add_link_tag(self, link_id: int, tag_name: str, *, return_created: bool = False):
        self._assert_writable()
        self._require_link(link_id)
        tag_folder_id = self.add_tag(tag_name)
        c = self._cursor()
        row = c.execute(
            "SELECT fk, title FROM moz_bookmarks WHERE id = ?",
            (link_id,),
        ).fetchone()
        fk = int(row["fk"] or 0)
        if fk <= 0:
            raise ValueError("link has no moz_places foreign key")
        existing = c.execute(
            "SELECT id FROM moz_bookmarks WHERE type = 1 AND parent = ? AND fk = ? ORDER BY id LIMIT 1",
            (tag_folder_id, fk),
        ).fetchone()
        if existing:
            out = int(existing["id"])
            if return_created:
                return out, False
            return out
        tag_ref_id = self._insert_bookmark(
            btype=1,
            fk=fk,
            parent_id=tag_folder_id,
            position=self._resolve_position(tag_folder_id, None),
            title=(row["title"] or "").strip() or None,
        )
        self.conn.commit()
        if return_created:
            return tag_ref_id, True
        return tag_ref_id

    def dedupe_bookmark_links_by_url(self) -> int:
        """Remove duplicate bookmark links and merge duplicate moz_places rows by URL.

        Returns number of removed rows (bookmarks + merged duplicate places).
        """
        self._assert_writable()
        c = self._cursor()
        removed = 0

        # 1) Merge duplicate moz_places rows that share the same URL.
        place_dupes = c.execute(
            """
            SELECT url, GROUP_CONCAT(id) AS ids
            FROM moz_places
            WHERE url IS NOT NULL AND TRIM(url) != ''
            GROUP BY url
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        for row in place_dupes:
            ids = sorted(int(x) for x in str(row["ids"]).split(",") if str(x).strip())
            if len(ids) <= 1:
                continue
            keep = ids[0]
            for dup in ids[1:]:
                c.execute("UPDATE moz_bookmarks SET fk = ? WHERE fk = ?", (keep, dup))
                c.execute("DELETE FROM moz_places WHERE id = ?", (dup,))
                removed += 1

        # 2) Remove duplicate link entries in the same folder with the same fk.
        same_parent_dupe_ids = c.execute(
            """
            SELECT b1.id
            FROM moz_bookmarks b1
            JOIN moz_bookmarks b2
              ON b1.type = 1
             AND b2.type = 1
             AND b1.parent = b2.parent
             AND COALESCE(b1.fk, -1) = COALESCE(b2.fk, -1)
             AND b1.id > b2.id
            """
        ).fetchall()
        for row in same_parent_dupe_ids:
            c.execute("DELETE FROM moz_bookmarks WHERE id = ?", (int(row["id"]),))
            removed += 1

        # 3) Enforce global uniqueness for regular bookmarks (exclude tag copies).
        links = self.read_all(include_tag_links=False)
        by_url: Dict[str, List[LinkEntry]] = {}
        for e in links:
            key = normalize_url(e.url or "")
            if not key:
                continue
            by_url.setdefault(key, []).append(e)
        for items in by_url.values():
            if len(items) <= 1:
                continue
            items.sort(key=lambda x: x.id)
            for dup in items[1:]:
                c.execute("DELETE FROM moz_bookmarks WHERE id = ?", (dup.id,))
                removed += 1

        if removed:
            self.conn.commit()
        return removed

    def recompute_foreign_count(self) -> None:
        self._assert_writable()
        if not self._has_foreign_count:
            return
        c = self._cursor()
        c.execute(
            """
            UPDATE moz_places
            SET foreign_count = (
                SELECT COUNT(*)
                FROM moz_bookmarks b
                WHERE b.type = 1 AND b.fk = moz_places.id
            )
            """
        )
        self.conn.commit()

    def validate_integrity(self) -> None:
        c = self._cursor()
        row = c.execute("PRAGMA integrity_check").fetchone()
        status = str(row[0]) if row is not None else ""
        if status.lower() != "ok":
            raise RuntimeError(f"sqlite integrity_check failed: {status or '<empty>'}")

        fk_rows = c.execute("PRAGMA foreign_key_check").fetchall()
        if fk_rows:
            raise RuntimeError(f"sqlite foreign_key_check failed with {len(fk_rows)} row(s)")

    def _bookmark_tree_maps(self) -> tuple[Dict[int, int], Dict[int, str], Dict[int, int]]:
        c = self._cursor()
        rows = c.execute("SELECT id, parent, title, type FROM moz_bookmarks ORDER BY id").fetchall()
        parent_map: Dict[int, int] = {}
        title_map: Dict[int, str] = {}
        type_map: Dict[int, int] = {}
        for r in rows:
            bid = int(r["id"])
            parent_map[bid] = int(r["parent"] or 0)
            title_map[bid] = (r["title"] or "").strip()
            type_map[bid] = int(r["type"] or 0)
        return parent_map, title_map, type_map

    def _folder_path(
        self,
        folder_id: int,
        parent_map: Dict[int, int],
        title_map: Dict[int, str],
        type_map: Dict[int, int],
    ) -> List[str]:
        out: List[str] = []
        current = folder_id
        seen = set()
        inv_roots = {v: k for k, v in self.root_ids.items()}
        while current and current not in seen:
            seen.add(current)
            root_name = inv_roots.get(current)
            if root_name:
                label = _ROOT_LABELS.get(root_name, root_name.title())
                if label:
                    out.append(label)
                break
            if type_map.get(current) == 2:
                name = (title_map.get(current) or "").strip()
                if name:
                    out.append(name)
            current = parent_map.get(current, 0)
        out.reverse()
        return out

    def _tag_names_by_fk(self, link_rows, parent_map: Dict[int, int]) -> Dict[int, set[str]]:
        tags_root = self.root_ids.get("tags")
        if tags_root is None:
            return {}
        c = self._cursor()
        by_id = c.execute("SELECT id, parent, title FROM moz_bookmarks WHERE type = 2").fetchall()
        folder_parent = {int(x["id"]): int(x["parent"] or 0) for x in by_id}
        folder_title = {int(x["id"]): (x["title"] or "").strip() for x in by_id}
        out: Dict[int, set[str]] = {}
        for r in link_rows:
            row_id = int(r["id"])
            fk = int(r["fk"] or 0)
            if fk <= 0:
                continue
            if not self._descends_from(row_id, tags_root, parent_map):
                continue
            tag_name = self._nearest_tag_folder_name(int(r["parent"] or 0), tags_root, folder_parent, folder_title)
            if not tag_name:
                continue
            out.setdefault(fk, set()).add(tag_name.lower())
        return out

    def _tag_fks_to_names(self, tags: Dict[str, List[int]]) -> Dict[int, set[str]]:
        out: Dict[int, set[str]] = {}
        for tag, fks in tags.items():
            for fk in fks:
                out.setdefault(int(fk), set()).add(tag.lower())
        return out

    def _nearest_tag_folder_name(
        self,
        folder_id: int,
        tags_root_id: int,
        folder_parent: Dict[int, int],
        folder_title: Dict[int, str],
    ) -> str:
        current = folder_id
        seen = set()
        while current and current not in seen:
            seen.add(current)
            parent = folder_parent.get(current, 0)
            if parent == tags_root_id:
                return (folder_title.get(current) or "").strip()
            current = parent
        return ""

    def _descends_from(self, node_id: int, ancestor_id: int, parent_map: Dict[int, int]) -> bool:
        current = node_id
        seen = set()
        while current and current not in seen:
            if current == ancestor_id:
                return True
            seen.add(current)
            current = parent_map.get(current, 0)
        return False

    def _discover_root_ids(self) -> Dict[str, int]:
        c = self._cursor()
        out: Dict[str, int] = {}
        if self._has_table("moz_bookmarks_roots"):
            rows = c.execute("SELECT root_name, folder_id FROM moz_bookmarks_roots").fetchall()
            for r in rows:
                out[str(r["root_name"])] = int(r["folder_id"])
        if not out and self._has_guid:
            rows = c.execute(
                "SELECT id, guid FROM moz_bookmarks WHERE guid IN (?, ?, ?, ?, ?)",
                tuple(_ROOT_GUID_TO_NAME.keys()),
            ).fetchall()
            for r in rows:
                name = _ROOT_GUID_TO_NAME.get(str(r["guid"]))
                if name:
                    out[name] = int(r["id"])
        return out

    def _resolve_root_alias(self, component: str) -> Optional[int]:
        key = _root_alias_key(component)
        root_name = _ROOT_ALIASES.get(key)
        if not root_name:
            return None
        return self.root_ids.get(root_name)

    def _ensure_place(self, url: str, title: str) -> int:
        c = self._cursor()
        row = c.execute("SELECT id, title FROM moz_places WHERE url = ? LIMIT 1", (url,)).fetchone()
        if row:
            pid = int(row["id"])
            if title and not (row["title"] or ""):
                c.execute("UPDATE moz_places SET title = ? WHERE id = ?", (title, pid))
            return pid
        cols = ["url", "title"]
        vals: List[object] = [url, title]
        if self._has_column("moz_places", "guid"):
            cols.append("guid")
            vals.append(self._new_guid())
        placeholders = ", ".join(["?"] * len(vals))
        c.execute(
            f"INSERT INTO moz_places ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        return int(c.lastrowid)

    def _insert_bookmark(
        self,
        *,
        btype: int,
        fk: Optional[int],
        parent_id: int,
        position: int,
        title: Optional[str],
    ) -> int:
        c = self._cursor()
        now = self._now_us()
        cols = ["type", "fk", "parent", "position", "title", "dateAdded", "lastModified"]
        vals: List[object] = [btype, fk, parent_id, position, title, now, now]
        if self._has_guid:
            cols.append("guid")
            vals.append(self._new_guid())
        placeholders = ", ".join(["?"] * len(vals))
        c.execute(
            f"INSERT INTO moz_bookmarks ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        row_id = int(c.lastrowid)
        if fk is not None and fk > 0 and self._has_foreign_count:
            c.execute("UPDATE moz_places SET foreign_count = foreign_count + 1 WHERE id = ?", (fk,))
        self._touch_folder(parent_id)
        return row_id

    def _resolve_position(self, parent_id: int, position: Optional[int]) -> int:
        if position is not None and position >= 0:
            return int(position)
        c = self._cursor()
        row = c.execute("SELECT COALESCE(MAX(position), -1) AS p FROM moz_bookmarks WHERE parent = ?", (parent_id,)).fetchone()
        return int(row["p"]) + 1

    def _touch_folder(self, folder_id: int) -> None:
        if not folder_id:
            return
        c = self._cursor()
        c.execute("UPDATE moz_bookmarks SET lastModified = ? WHERE id = ?", (self._now_us(), folder_id))

    def _require_folder(self, folder_id: int) -> None:
        c = self._cursor()
        row = c.execute("SELECT type FROM moz_bookmarks WHERE id = ?", (folder_id,)).fetchone()
        if not row:
            raise ValueError(f"folder id not found: {folder_id}")
        if int(row["type"] or 0) != 2:
            raise ValueError(f"id is not a folder: {folder_id}")

    def _require_link(self, link_id: int) -> None:
        c = self._cursor()
        row = c.execute("SELECT type FROM moz_bookmarks WHERE id = ?", (link_id,)).fetchone()
        if not row:
            raise ValueError(f"link id not found: {link_id}")
        if int(row["type"] or 0) != 1:
            raise ValueError(f"id is not a link: {link_id}")

    def _assert_writable(self) -> None:
        if self.readonly:
            raise RuntimeError("database opened in readonly mode")

    def _has_table(self, name: str) -> bool:
        c = self._cursor()
        row = c.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        ).fetchone()
        return row is not None

    def _has_column(self, table_name: str, column_name: str) -> bool:
        c = self._cursor()
        rows = c.execute(f"PRAGMA table_info({table_name})").fetchall()
        for r in rows:
            if str(r[1]) == column_name:
                return True
        return False

    def _cursor(self) -> sqlite3.Cursor:
        if self.conn is None:
            raise RuntimeError("database is not open")
        return self.conn.cursor()

    def _now_us(self) -> int:
        return int(time.time() * 1_000_000)

    def _new_guid(self) -> str:
        # Firefox GUIDs are commonly 12-char URL-safe strings.
        import base64
        import os

        raw = os.urandom(9)
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _folder_component_key(name: str) -> str:
    # Treat emoji-prefixed and plain names as equivalent:
    # "👕 Clothing" == "Clothing".
    s = (name or "").strip()
    while s and not s[0].isalnum():
        s = s[1:].lstrip()
    out = []
    prev_space = False
    for ch in s:
        if ch.isspace():
            if not prev_space:
                out.append(" ")
            prev_space = True
            continue
        prev_space = False
        out.append(ch.lower())
    return "".join(out).strip()


def _root_alias_key(name: str) -> str:
    s = (name or "").strip()
    while s and not s[0].isalnum():
        s = s[1:].lstrip()
    return "".join(ch.lower() for ch in s if ch.isalnum())


def _has_leading_emoji(name: str) -> bool:
    s = (name or "").strip()
    return bool(s and not s[0].isalnum())
