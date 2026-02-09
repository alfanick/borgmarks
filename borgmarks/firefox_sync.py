from __future__ import annotations

import sqlite3
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .favicons_db import FaviconsDB
from .log import get_logger
from .model import Bookmark
from .places_db import PlacesDB
from .url_norm import normalize_url

log = get_logger(__name__)

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
    icon_links: int = 0
    icon_errors: int = 0
    deduped_bookmark_rows: int = 0
    deduped_favicon_rows: int = 0


def apply_bookmarks_to_firefox(
    places_db_path: Path,
    bookmarks: Iterable[Bookmark],
    *,
    favicons_db_path: Optional[Path] = None,
    apply_icons: bool = True,
    dedupe: bool = True,
) -> SyncStats:
    stats = SyncStats()
    rows = list(bookmarks)
    total_links = len(rows)
    try:
        with ExitStack() as stack:
            db = stack.enter_context(PlacesDB(places_db_path, readonly=False))
            favicon_db = None
            if favicons_db_path and favicons_db_path.exists():
                candidate = stack.enter_context(FaviconsDB(favicons_db_path))
                if candidate.supports_schema():
                    favicon_db = candidate

            if dedupe:
                stats.deduped_bookmark_rows = db.dedupe_bookmark_links_by_url()
                if favicon_db is not None:
                    stats.deduped_favicon_rows = favicon_db.dedupe()

            existing = db.read_all(include_tag_links=False)
            existing_by_url: Dict[str, int] = {}
            existing_parent_by_url: Dict[str, int] = {}
            for e in existing:
                key = normalize_url(e.url)
                if key and key not in existing_by_url:
                    existing_by_url[key] = e.id
                    existing_parent_by_url[key] = e.parent_id

            for idx, b in enumerate(rows, start=1):
                url = normalize_url(b.final_url or b.url)
                if not url:
                    continue
                title = (b.assigned_title or b.title or url).strip() or url
                tags = [t for t in (b.tags or []) if str(t).strip()]
                category = "/".join(b.assigned_path or b.folder_path or ["Uncategorized"])
                domain = (b.domain or "").strip() or "unknown-domain"
                log.info("Link [%d/%d] - %s - %s (phase=apply-links)", idx, total_links, domain, category)

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
                        _tag_ref_id, created = db.add_link_tag(link_id, tag, return_created=True)
                        if created:
                            stats.tagged_links += 1
                stats.touched_links += 1

            if apply_icons and favicon_db is not None:
                icon_rows = [b for b in rows if normalize_url(b.final_url or b.url) and (b.meta.get("icon_uri") or "").strip()]
                total_icons = len(icon_rows)
                for idx, b in enumerate(icon_rows, start=1):
                    url = normalize_url(b.final_url or b.url)
                    if not url:
                        continue
                    icon_uri = (b.meta.get("icon_uri") or "").strip()
                    if not icon_uri:
                        continue
                    domain = (b.domain or "").strip() or "unknown-domain"
                    log.info("Icon [%d/%d] - %s (phase=apply-icons)", idx, total_icons, domain)
                    try:
                        page_hash = db.get_place_url_hash(url)
                        if favicon_db.set_page_icon(page_url=url, icon_url=icon_uri, page_url_hash=page_hash):
                            stats.icon_links += 1
                    except Exception as e:
                        stats.icon_errors += 1
                        log.warning("Failed to set favicon for %s: %s", url, e)

            # Keep references consistent and fail fast if DB is not coherent.
            db.recompute_foreign_count()
            db.validate_integrity()
            if favicon_db is not None:
                favicon_db.validate_integrity()
    except sqlite3.OperationalError as e:
        msg = str(e).strip()
        if "locked" in msg.lower() or "busy" in msg.lower():
            raise RuntimeError(
                f"Firefox database is locked ({places_db_path}). Close Firefox and rerun."
            ) from e
        raise
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
