from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Dict, List, Tuple

from .log import get_logger
from .model import Bookmark

log = get_logger(__name__)


def enforce_leaf_limits(bookmarks: List[Bookmark], leaf_max_links: int, max_depth: int) -> None:
    changed = 0
    # Apply repeatedly so deep overfull leaves are handled in one run.
    for _pass in range(max(1, max_depth * 3)):
        groups: Dict[Tuple[str, ...], List[Bookmark]] = defaultdict(list)
        for b in bookmarks:
            if not b.assigned_path:
                b.assigned_path = ["Archive", "Unclassified"]
            groups[tuple(b.assigned_path)].append(b)

        changed_pass = 0
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
                    new_path = base + [sub]
                    for b in bms:
                        if b.assigned_path != new_path:
                            b.assigned_path = new_path
                            changed_pass += 1
                continue

            for b in items:
                bucket = _bucket_for_url(b.url)
                # Keep repeated runs stable: if this leaf is already bucketed by the
                # same key, do not append another identical suffix (A-F/A-F).
                if base and _norm_token(base[-1]) == _norm_token(bucket):
                    continue
                new_path = base + [bucket]
                if b.assigned_path != new_path:
                    b.assigned_path = new_path
                    changed_pass += 1

        changed += changed_pass
        if changed_pass == 0:
            break

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


def _norm_token(s: str) -> str:
    return "".join(ch.lower() for ch in (s or "") if ch.isalnum())
