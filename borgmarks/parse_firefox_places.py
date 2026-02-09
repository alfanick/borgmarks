from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Set

from .log import get_logger
from .model import Bookmark

log = get_logger(__name__)

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


def parse_firefox_places(profile_or_db_path: Path) -> List[Bookmark]:
    db_path = _resolve_places_path(profile_or_db_path)
    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        guid_expr = "b.guid" if _has_column(conn, "moz_bookmarks", "guid") else "NULL AS guid"
        rows = conn.execute(
            f"""
            SELECT
              b.id, b.fk, b.parent, b.type, b.title, {guid_expr}, b.dateAdded, b.lastModified,
              p.url, p.hidden
            FROM moz_bookmarks b
            LEFT JOIN moz_places p ON p.id = b.fk
            ORDER BY b.id
            """
        ).fetchall()
        root_rows = []
        if _has_table(conn, "moz_bookmarks_roots"):
            root_rows = conn.execute(
                "SELECT root_name, folder_id FROM moz_bookmarks_roots"
            ).fetchall()
    finally:
        conn.close()

    roots_by_id = {int(r["folder_id"]): str(r["root_name"]) for r in root_rows}
    if not roots_by_id:
        # Newer desktop profiles can lack moz_bookmarks_roots; derive roots by stable GUIDs.
        for r in rows:
            root_name = _ROOT_GUID_TO_NAME.get(str(r["guid"] or ""))
            if root_name:
                roots_by_id[int(r["id"])] = root_name
    tags_root_id = next((rid for rid, name in roots_by_id.items() if name == "tags"), None)

    by_id = {int(r["id"]): r for r in rows}
    parent_by_id = {int(r["id"]): int(r["parent"] or 0) for r in rows}
    tag_names_by_fk = _build_tag_index(rows, by_id, parent_by_id, tags_root_id)

    out: List[Bookmark] = []
    for r in rows:
        if int(r["type"] or 0) != 1:
            continue
        fk = r["fk"]
        if fk is None:
            continue
        url = (r["url"] or "").strip()
        if not url or url.startswith("place:"):
            continue
        if int(r["hidden"] or 0) != 0:
            continue
        row_id = int(r["id"])
        if tags_root_id is not None and _descends_from(row_id, parent_by_id, tags_root_id):
            # Tag containers duplicate bookmark references; skip them as standalone bookmarks.
            continue

        title = (r["title"] or "").strip() or url
        folder_path = _build_folder_path(int(r["parent"] or 0), by_id, roots_by_id)
        b = Bookmark(
            id="",
            title=title,
            url=url,
            add_date=_moz_time_to_unix_s(r["dateAdded"]),
            last_modified=_moz_time_to_unix_s(r["lastModified"]),
            folder_path=folder_path,
        )
        b.tags = sorted(tag_names_by_fk.get(int(fk), set()))
        b.meta["source"] = "firefox"
        out.append(b)

    return out


def _resolve_places_path(profile_or_db_path: Path) -> Path:
    p = Path(profile_or_db_path)
    if p.is_file():
        return p
    db = p / "places.sqlite"
    if db.exists():
        return db
    raise FileNotFoundError(f"places.sqlite not found in {p}")


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(r[1]) == column_name for r in rows)


def _build_tag_index(
    rows,
    by_id: Dict[int, sqlite3.Row],
    parent_by_id: Dict[int, int],
    tags_root_id: Optional[int],
) -> Dict[int, Set[str]]:
    if tags_root_id is None:
        return {}
    out: Dict[int, Set[str]] = {}
    for r in rows:
        if int(r["type"] or 0) != 1 or r["fk"] is None:
            continue
        row_id = int(r["id"])
        if not _descends_from(row_id, parent_by_id, tags_root_id):
            continue
        tag_name = _nearest_child_of_root_title(int(r["parent"] or 0), by_id, tags_root_id)
        if not tag_name:
            continue
        out.setdefault(int(r["fk"]), set()).add(tag_name.lower())
    return out


def _nearest_child_of_root_title(start_id: int, by_id: Dict[int, sqlite3.Row], root_id: int) -> str:
    current = start_id
    while current in by_id:
        row = by_id[current]
        parent = int(row["parent"] or 0)
        if parent == root_id:
            return (row["title"] or "").strip()
        current = parent
    return ""


def _build_folder_path(parent_id: int, by_id: Dict[int, sqlite3.Row], roots_by_id: Dict[int, str]) -> List[str]:
    parts: List[str] = []
    current = parent_id
    seen = set()
    while current and current not in seen:
        seen.add(current)
        root_name = roots_by_id.get(current)
        if root_name:
            label = _ROOT_LABELS.get(root_name, root_name.title())
            if root_name != "tags":
                parts.append(label)
            break
        row = by_id.get(current)
        if row is None:
            break
        if int(row["type"] or 0) == 2:
            t = (row["title"] or "").strip()
            if t:
                parts.append(t)
        current = int(row["parent"] or 0)
    parts.reverse()
    return parts


def _descends_from(node_id: int, parent_by_id: Dict[int, int], ancestor_id: int) -> bool:
    current = node_id
    seen = set()
    while current and current not in seen:
        if current == ancestor_id:
            return True
        seen.add(current)
        current = parent_by_id.get(current, 0)
    return False


def _moz_time_to_unix_s(value) -> Optional[int]:
    if value is None:
        return None
    try:
        iv = int(value)
    except Exception:
        return None
    # Firefox PRTime is microseconds since Unix epoch.
    if iv > 10_000_000_000:
        return iv // 1_000_000
    return iv
