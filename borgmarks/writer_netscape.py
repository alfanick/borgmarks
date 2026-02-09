from __future__ import annotations

import html
from pathlib import Path
from typing import Dict, List

from .log import get_logger
from .model import Bookmark, FolderNode

log = get_logger(__name__)


def build_tree(bookmarks: List[Bookmark]) -> FolderNode:
    root = FolderNode(name="root")
    for b in bookmarks:
        node = root
        for comp in b.assigned_path:
            node = node.get_or_create(comp)
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
    # Folders first (stable sort)
    for name in sorted(node.children.keys(), key=lambda s: s.lower()):
        child = node.children[name]
        lines.append(f"{indent}<DT><H3>{html.escape(name)}</H3>")
        lines.append(f"{indent}<DL><p>")
        _write_folder(lines, child, indent + "    ", embed_metadata)
        lines.append(f"{indent}</DL><p>")

    # Then bookmarks
    for b in node.bookmarks:
        title = b.assigned_title or b.title
        attrs = [f'HREF="{html.escape(b.final_url or b.url, quote=True)}"']
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
