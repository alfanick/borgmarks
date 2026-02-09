from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from .model import Bookmark
from .places_db import PlacesDB
from .url_norm import normalize_url

_ROOT_ALIAS = {
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
class SyncStats:
    added_links: int = 0
    moved_links: int = 0
    tagged_links: int = 0
    touched_links: int = 0


def apply_bookmarks_to_firefox(places_db_path: Path, bookmarks: Iterable[Bookmark]) -> SyncStats:
    stats = SyncStats()
    with PlacesDB(places_db_path, readonly=False) as db:
        existing = db.read_all(include_tag_links=False)
        existing_by_url: Dict[str, int] = {}
        existing_parent_by_url: Dict[str, int] = {}
        for e in existing:
            key = normalize_url(e.url)
            if key and key not in existing_by_url:
                existing_by_url[key] = e.id
                existing_parent_by_url[key] = e.parent_id

        for b in bookmarks:
            url = normalize_url(b.final_url or b.url)
            if not url:
                continue
            title = (b.assigned_title or b.title or url).strip() or url
            tags = [t for t in (b.tags or []) if str(t).strip()]

            root_id, rel_path = _resolve_target_root_and_relpath(db, b.assigned_path or b.folder_path or [])
            target_parent_id = db.ensure_folder_path(root_id, rel_path)

            existing_link_id = existing_by_url.get(url)
            if existing_link_id is None:
                link_id = db.add_link(target_parent_id, url, title, tags=tags)
                existing_by_url[url] = link_id
                existing_parent_by_url[url] = target_parent_id
                stats.added_links += 1
            else:
                if existing_parent_by_url.get(url) != target_parent_id:
                    db.move_link(existing_link_id, target_parent_id)
                    existing_parent_by_url[url] = target_parent_id
                    stats.moved_links += 1
                # Ensure title and tags converge in-place, idempotently.
                link_id = db.add_link(target_parent_id, url, title, tags=[])
                for tag in tags:
                    before = len(db.read_tag(tag))
                    db.add_link_tag(link_id, tag)
                    after = len(db.read_tag(tag))
                    if after > before:
                        stats.tagged_links += 1
            stats.touched_links += 1
    return stats


def _resolve_target_root_and_relpath(db: PlacesDB, folder_path: List[str]) -> Tuple[int, List[str]]:
    comps = [str(x).strip() for x in folder_path if str(x).strip()]
    if comps:
        alias = _ROOT_ALIAS.get(_folder_key(comps[0]))
        if alias:
            rid = db.get_root_folder_id(alias)
            if rid is not None:
                return rid, comps[1:]
    menu_root = db.get_root_folder_id("menu")
    if menu_root is not None:
        return menu_root, comps
    toolbar_root = db.get_root_folder_id("toolbar")
    if toolbar_root is not None:
        return toolbar_root, comps
    unfiled_root = db.get_root_folder_id("unfiled")
    if unfiled_root is not None:
        return unfiled_root, comps
    raise ValueError("no known Firefox bookmark roots found (menu/toolbar/unfiled)")


def _folder_key(name: str) -> str:
    s = (name or "").strip()
    while s and not s[0].isalnum():
        s = s[1:].lstrip()
    return "".join(ch.lower() for ch in s if ch.isalnum())
