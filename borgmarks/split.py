from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Dict, List, Tuple

from .log import get_logger
from .model import Bookmark

log = get_logger(__name__)


def enforce_leaf_limits(bookmarks: List[Bookmark], leaf_max_links: int, max_depth: int) -> None:
    groups: Dict[Tuple[str, ...], List[Bookmark]] = defaultdict(list)
    for b in bookmarks:
        if not b.assigned_path:
            b.assigned_path = ["Archive", "Unclassified"]
        groups[tuple(b.assigned_path)].append(b)

    changed = 0
    for path, items in list(groups.items()):
        if len(items) <= leaf_max_links:
            continue

        base = list(path)
        if len(base) >= max_depth:
            base = base[: max_depth - 1]

        by_domain: Dict[str, List[Bookmark]] = defaultdict(list)
        for b in items:
            d = (b.domain or "").lower()
            if d.startswith("www."):
                d = d[4:]
            by_domain[d or "unknown"].append(b)

        if 1 < len(by_domain) <= 20:
            for d, bms in by_domain.items():
                sub = _safe_folder_name(d)
                for b in bms:
                    b.assigned_path = base + [sub]
                    changed += 1
            continue

        for b in items:
            bucket = _bucket_for_url(b.url)
            b.assigned_path = base + [bucket]
            changed += 1

    if changed:
        log.info("Leaf-size enforcement adjusted %d bookmarks to keep folders <= %d items.", changed, leaf_max_links)


def _bucket_for_url(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8", errors="ignore")).hexdigest()
    c = h[0]
    if c.isdigit():
        return "0-9"
    if c in "abcdef":
        return "A-F"
    if c in "ghijkl":
        return "G-L"
    if c in "mnopqr":
        return "M-R"
    return "S-Z"


def _safe_folder_name(s: str) -> str:
    s = s.strip().replace("/", "_").replace("\\", "_")
    return s[:60] if len(s) > 60 else (s or "unknown")
