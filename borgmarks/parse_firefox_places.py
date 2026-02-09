from __future__ import annotations

from pathlib import Path
from typing import List

from .model import Bookmark
from .places_db import PlacesDB


def parse_firefox_places(profile_or_db_path: Path) -> List[Bookmark]:
    with PlacesDB(_resolve_places_path(profile_or_db_path), readonly=True) as db:
        links = db.read_all(include_tag_links=False)

    out: List[Bookmark] = []
    for entry in links:
        b = Bookmark(
            id="",
            title=entry.title,
            url=entry.url,
            add_date=None,
            last_modified=None,
            folder_path=list(entry.path),
        )
        b.tags = list(entry.tags)
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
