from __future__ import annotations

import html
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

from .log import get_logger
from .model import Bookmark, FolderNode

log = get_logger(__name__)


def build_tree(bookmarks: List[Bookmark]) -> FolderNode:
    root = FolderNode(name="root", sort_key="root", path_tokens=[])
    for b in bookmarks:
        node = root
        for comp in b.assigned_path:
            child = node.get_or_create(comp)
            child.sort_key = _folder_sort_key(comp)
            child.path_tokens = node.path_tokens + [child.sort_key]
            node = child
        node.bookmarks.append(b)
    return root


def write_firefox_html(
    *,
    out_path: Path,
    bookmarks_tree: FolderNode,
    toolbar_spec: Dict,
    embed_metadata: bool,
    title_root: str,
) -> None:
    """Write a Firefox-importable Netscape Bookmark HTML.

    Key details:
    - A folder with H3 attribute PERSONAL_TOOLBAR_FOLDER="true" becomes the Firefox toolbar folder.
    - Firefox bookmark tags can be expressed using TAGS="tag1 tag2" on <A>.
    """
    lines: List[str] = []
    lines.append("<!DOCTYPE NETSCAPE-Bookmark-file-1>")
    lines.append("<!-- This is an automatically generated file. DO NOT EDIT! -->")
    lines.append('<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">')
    lines.append(f"<TITLE>{html.escape(title_root)}</TITLE>")
    lines.append(f"<H1>{html.escape(title_root)}</H1>")
    lines.append("<DL><p>")

    # Toolbar folder
    lines.append('    <DT><H3 PERSONAL_TOOLBAR_FOLDER="true">Bookmarks Toolbar</H3>')
    lines.append("    <DL><p>")
    for fname in toolbar_spec.get("folders", []):
        lines.append(f"        <DT><H3>{html.escape(str(fname))}</H3>")
        lines.append("        <DL><p></DL><p>")
    for link in toolbar_spec.get("links", []):
        title = str(link.get("title", link.get("url", "")))
        url = str(link.get("url", ""))
        tags = link.get("tags") or []
        attrs = [f'HREF="{html.escape(url, quote=True)}"']
        if tags:
            attrs.append(f'TAGS="{html.escape(" ".join(map(str, tags)), quote=True)}"')
        lines.append(f"        <DT><A {' '.join(attrs)}>{html.escape(title)}</A>")
    lines.append("    </DL><p>")

    # Bookmarks Menu wrapper
    lines.append("    <DT><H3>Bookmarks Menu</H3>")
    lines.append("    <DL><p>")
    _write_folder(lines, bookmarks_tree, indent="        ", embed_metadata=embed_metadata)
    lines.append("    </DL><p>")

    lines.append("</DL><p>")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("Wrote Firefox bookmarks HTML: %s", out_path)


def _write_folder(lines: List[str], node: FolderNode, indent: str, embed_metadata: bool) -> None:
    # Folders first, grouped by semantic key to keep similar groups together.
    ordered_children = sorted(
        node.children.items(),
        key=lambda kv: (
            _emoji_sort_group(kv[0]),
            kv[1].sort_key.lower(),
            kv[0].lower(),
        ),
    )
    for name, child in ordered_children:
        lines.append(f"{indent}<DT><H3>{html.escape(name)}</H3>")
        lines.append(f"{indent}<DL><p>")
        _write_folder(lines, child, indent + "    ", embed_metadata)
        lines.append(f"{indent}</DL><p>")

    # Then bookmarks, freshest first in each leaf.
    ordered_bookmarks = sorted(
        node.bookmarks,
        key=lambda b: (
            -_freshness_ts(b),
            (b.assigned_title or b.title or "").lower(),
            (b.final_url or b.url or "").lower(),
        ),
    )
    for b in ordered_bookmarks:
        title = b.assigned_title or b.title
        attrs = [f'HREF="{html.escape(b.final_url or b.url, quote=True)}"']
        attrs.extend(_bookmark_icon_attrs(b))
        if b.add_date:
            attrs.append(f'ADD_DATE="{b.add_date}"')
        if b.last_modified:
            attrs.append(f'LAST_MODIFIED="{b.last_modified}"')
        if b.tags:
            attrs.append(f'TAGS="{html.escape(" ".join(b.tags), quote=True)}"')

        if embed_metadata:
            # Keep values compact: browsers will import unknown attributes but very large HTML files are annoying.
            if b.summary:
                attrs.append(f'data-borg-summary="{html.escape(b.summary[:220], quote=True)}"')
            if b.lang and b.lang != "EN":
                attrs.append(f'data-borg-lang="{html.escape(b.lang, quote=True)}"')
            if b.http_status is not None:
                attrs.append(f'data-borg-http="{b.http_status}"')
            if "fetch_ms" in b.meta:
                attrs.append(f'data-borg-fetch-ms="{html.escape(b.meta.get("fetch_ms",""), quote=True)}"')
            if "openai_ms" in b.meta:
                attrs.append(f'data-borg-openai-ms="{html.escape(b.meta.get("openai_ms",""), quote=True)}"')

        lines.append(f"{indent}<DT><A {' '.join(attrs)}>{html.escape(title)}</A>")


def _freshness_ts(b: Bookmark) -> int:
    # Prefer explicit visit/fetch metadata, then bookmark timestamps.
    for k in ("visited_at", "fetch_ts"):
        v = b.meta.get(k)
        if not v:
            continue
        try:
            from datetime import datetime

            return int(datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp())
        except Exception:
            pass
    candidates = [x for x in (b.last_modified, b.add_date) if isinstance(x, int)]
    if candidates:
        return max(candidates)
    return 0


def _folder_sort_key(name: str) -> str:
    # Strip emoji/punctuation prefix so "👕 Clothing" and "Clothing"
    # are adjacent in ordering.
    s = (name or "").strip()
    while s and not s[0].isalnum():
        s = s[1:].lstrip()
    return s.lower()


def _emoji_sort_group(name: str) -> str:
    # Cluster folders with the same leading emoji together.
    s = (name or "").strip()
    lead = []
    for ch in s:
        if ch.isalnum():
            break
        if ch.isspace():
            if lead:
                break
            continue
        lead.append(ch)
        if len(lead) >= 2:
            break
    # Folders without emoji come first for readability.
    if not lead:
        return ""
    return "".join(lead)


def _bookmark_icon_attrs(b: Bookmark) -> List[str]:
    icon_uri = (b.meta.get("icon_uri") or "").strip()
    if icon_uri:
        return [f'ICON_URI="{html.escape(icon_uri, quote=True)}"']
    emoji = _bookmark_fallback_emoji(b)
    return [f'ICON="{html.escape(_emoji_svg_data_uri(emoji), quote=True)}"']


def _bookmark_fallback_emoji(b: Bookmark) -> str:
    for comp in reversed(b.assigned_path or []):
        lead = _leading_emoji(comp)
        if lead:
            return lead
    top = (b.assigned_path[0] if b.assigned_path else "").lower()
    if "comput" in top:
        return "💻"
    if "shop" in top:
        return "🛒"
    if "photo" in top:
        return "📷"
    if "video" in top:
        return "🎬"
    if "news" in top:
        return "📰"
    return "🔗"


def _leading_emoji(name: str) -> str:
    s = (name or "").strip()
    out = []
    for ch in s:
        if ch.isalnum():
            break
        if ch.isspace():
            if out:
                break
            continue
        out.append(ch)
        if len(out) >= 2:
            break
    return "".join(out).strip()


def _emoji_svg_data_uri(emoji: str) -> str:
    # Firefox import accepts ICON data URLs. SVG keeps this tiny.
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64">'
        '<rect width="100%" height="100%" fill="white"/>'
        f'<text x="32" y="42" text-anchor="middle" font-size="40">{html.escape(emoji)}</text>'
        "</svg>"
    )
    return "data:image/svg+xml;utf8," + quote(svg, safe="")
