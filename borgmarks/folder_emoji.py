from __future__ import annotations

import json
from collections import Counter
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from .config import Settings
from .log import get_logger
from .model import Bookmark
from .openai_client import suggest_folder_emojis

log = get_logger(__name__)


SYSTEM_PROMPT_FOLDER_EMOJI = """You assign a single best emoji to folder nodes in a bookmark taxonomy.

Rules:
- Input contains folder paths and usage counts.
- Return suggestions for as many folders as possible.
- One emoji per folder (no text labels, no multi-emoji strings).
- Preserve meaning and avoid noisy swaps between similar technical folders.
- Prefer clear technical emojis when relevant (computers/dev/photo/video/network/security/etc.).
- If no useful emoji exists, return null or empty.
- Do not modify path names.

Output must be strict JSON for the schema (no extra text).
"""


def enrich_folder_emojis(
    bookmarks: List[Bookmark],
    cfg: Settings,
    *,
    target_ids: Optional[Set[str]] = None,
) -> None:
    if not bookmarks:
        return
    nodes = _folder_nodes(bookmarks)
    if not nodes:
        return
    if cfg.openai_folder_emoji_max_nodes > 0 and len(nodes) > cfg.openai_folder_emoji_max_nodes:
        nodes = nodes[: cfg.openai_folder_emoji_max_nodes]
        log.warning(
            "Folder emoji enrichment capped to first %d nodes (BORG_OPENAI_FOLDER_EMOJI_MAX_NODES).",
            len(nodes),
        )

    payload = {"folders": [{"path": list(path), "count": count} for path, count in nodes]}
    try:
        res = suggest_folder_emojis(
            model=cfg.openai_model,
            timeout_s=cfg.openai_timeout_s,
            max_output_tokens=cfg.openai_max_output_tokens,
            system_prompt=SYSTEM_PROMPT_FOLDER_EMOJI,
            user_payload=json.dumps(payload, ensure_ascii=False),
            batch_label=f"nodes-{len(nodes)}",
            use_browser_tool=cfg.openai_agent_browser,
            reasoning_effort=cfg.openai_reasoning_effort,
        )
    except Exception as e:
        log.warning("OpenAI folder emoji enrichment failed: %s", e)
        return

    mapping: Dict[Tuple[str, ...], str] = {}
    for s in res.parsed.suggestions:
        key = tuple(_base_component(x) for x in (s.path or []) if str(x).strip())
        if not key:
            continue
        emoji = _sanitize_emoji(s.emoji or "")
        if emoji:
            mapping[key] = emoji
    if not mapping:
        return
    changed = _apply_emoji_mapping(bookmarks, mapping, target_ids=target_ids)
    if changed:
        log.info("Folder emoji enrichment applied to %d bookmarks.", changed)


def _folder_nodes(bookmarks: Iterable[Bookmark]) -> List[Tuple[Tuple[str, ...], int]]:
    counts = Counter()
    for b in bookmarks:
        comps = [c for c in (b.assigned_path or []) if str(c).strip()]
        path: List[str] = []
        for comp in comps:
            path.append(_base_component(comp))
            counts[tuple(path)] += 1
    rows = list(counts.items())
    rows.sort(key=lambda x: (-x[1], "/".join(x[0]).lower()))
    return rows


def _apply_emoji_mapping(
    bookmarks: Iterable[Bookmark],
    mapping: Dict[Tuple[str, ...], str],
    *,
    target_ids: Optional[Set[str]] = None,
) -> int:
    changed = 0
    for b in bookmarks:
        if target_ids is not None and b.id not in target_ids:
            continue
        if not b.assigned_path:
            continue
        out: List[str] = []
        base_path: List[str] = []
        modified = False
        for comp in b.assigned_path:
            base = _base_component(comp)
            base_path.append(base)
            emoji = mapping.get(tuple(base_path))
            if emoji and not _has_leading_emoji(comp):
                out_comp = f"{emoji} {base}"
                modified = True
            else:
                out_comp = comp
            out.append(out_comp)
        if modified:
            b.assigned_path = out
            changed += 1
    return changed


def _base_component(name: str) -> str:
    s = (name or "").strip()
    while s and not s[0].isalnum():
        s = s[1:].lstrip()
    return s.strip()


def _has_leading_emoji(name: str) -> bool:
    s = (name or "").strip()
    return bool(s and not s[0].isalnum())


def _sanitize_emoji(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return ""
    # Keep only first contiguous emoji-ish symbol group at the beginning.
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
