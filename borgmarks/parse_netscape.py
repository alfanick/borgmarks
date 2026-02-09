from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple

from bs4 import BeautifulSoup  # type: ignore

from .model import Bookmark
from .log import get_logger

log = get_logger(__name__)
_WS_RE = re.compile(r"\s+")


def parse_bookmarks_html(path: Path) -> Tuple[List[Bookmark], str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(text, "lxml")

    h1 = soup.find("h1")
    root_title = h1.get_text(strip=True) if h1 else "Bookmarks"

    dl = soup.find("dl")
    if dl is None:
        raise ValueError("Could not find <DL> root in bookmarks file")

    bookmarks: List[Bookmark] = []
    _walk_dl(dl, current_path=[], out=bookmarks)

    for i, b in enumerate(bookmarks):
        b.id = f"b{i+1}"
    return bookmarks, root_title


def _walk_dl(dl, current_path: List[str], out: List[Bookmark]) -> None:
    for dt in dl.find_all("dt", recursive=False):
        h3 = dt.find("h3", recursive=False)
        if h3 is not None:
            name = _WS_RE.sub(" ", h3.get_text(strip=True))
            sub_dl = dt.find_next_sibling("dl")
            if sub_dl is None:
                sub_dl = dt.find("dl")
            if sub_dl is None:
                log.warning("Folder without DL: %s", name)
                continue
            _walk_dl(sub_dl, current_path + [name], out)
            continue

        a = dt.find("a", recursive=False)
        if a is not None and a.get("href"):
            title = _WS_RE.sub(" ", a.get_text(strip=True))
            url = a.get("href")
            add_date = _maybe_int(a.get("add_date"))
            last_modified = _maybe_int(a.get("last_modified"))
            b = Bookmark(
                id="",
                title=title,
                url=url,
                add_date=add_date,
                last_modified=last_modified,
                folder_path=list(current_path),
            )
            tags = a.get("tags")
            if tags:
                b.tags = [t for t in _WS_RE.sub(" ", tags).split(" ") if t]
            out.append(b)


def _maybe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None
