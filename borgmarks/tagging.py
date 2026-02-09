from __future__ import annotations

import json
import re
from collections import Counter
from typing import Dict, Iterable, List, Sequence

from .config import Settings
from .log import get_logger
from .model import Bookmark
from .openai_client import suggest_tags_for_tree

log = get_logger(__name__)


SYSTEM_PROMPT_TAGGER = """You are a bookmark tagger for a single user's browser library.

Goal:
- Build one global tag catalog (max 50 tags total), then assign 1-4 tags per bookmark.

Rules:
- Use the whole bookmark tree context (folder paths + summaries + domains) to keep tags consistent.
- Reuse existing tags whenever they are still valid.
- Prefer one-word tags; at most two words.
- Tags must be plain text only: lowercase letters with optional underscore separator for 2-word tags.
- Never output spaces in tags; for two words use underscore (example: machine_learning).
- Use sensible domain abbreviations when appropriate (for example: AI, ML, LLM), but only when they are clear.
- Abbreviations must be UPPERCASE.
- Suggested abbreviations include examples like AI, ML, LLM, NLP, API, SDK, SQL, DB, UX, UI, but you may use other well-known abbreviations when they are clearer.
- No emojis, punctuation, symbols, or numbers.
- Keep tags short and practical (examples: amazon, news, blog, camera, python).
- Avoid near-duplicate synonyms (pick one canonical tag).
- Do not use folder names verbatim when they are too broad (e.g., archive, reading, misc).
- Keep assignments stable; do not churn tags unless there is strong reason.

Output must be strict JSON for the schema (no extra text).
"""

_STOP_TAGS = {
    "archive",
    "inbox",
    "misc",
    "other",
    "reading",
    "bookmarks",
    "bookmark",
    "folder",
    "uncategorized",
    "unclassified",
    "dead_links",
}


def enrich_bookmark_tags(bookmarks: List[Bookmark], cfg: Settings) -> int:
    if not bookmarks:
        return 0
    if not cfg.openai_tags_enrich:
        return _normalize_and_cap_tags(bookmarks, cfg)

    payload = _build_payload(bookmarks)
    try:
        res = suggest_tags_for_tree(
            model=cfg.openai_model,
            timeout_s=cfg.openai_timeout_s,
            max_output_tokens=cfg.openai_max_output_tokens,
            system_prompt=SYSTEM_PROMPT_TAGGER,
            user_payload=json.dumps(payload, ensure_ascii=False),
            batch_label=f"links-{len(bookmarks)}",
            use_browser_tool=cfg.openai_agent_browser,
            reasoning_effort=cfg.openai_reasoning_effort,
        )
    except Exception as e:
        log.warning("OpenAI tag enrichment failed: %s", e)
        return _normalize_and_cap_tags(bookmarks, cfg)

    by_id: Dict[str, Bookmark] = {b.id: b for b in bookmarks}
    changed = 0
    for a in res.parsed.assignments:
        b = by_id.get(a.id)
        if not b:
            continue
        before = list(b.tags)
        b.tags = list(a.tags or [])
        if before != b.tags:
            changed += 1

    normalized = _normalize_and_cap_tags(bookmarks, cfg)
    if changed or normalized:
        log.info(
            "Tag enrichment updated %d bookmarks (normalized=%d, global_cap=%d).",
            changed,
            normalized,
            max(1, cfg.openai_tags_max_global),
        )
    return changed + normalized


def _build_payload(bookmarks: Sequence[Bookmark]) -> dict:
    folder_catalog = Counter()
    for b in bookmarks:
        p = [str(x).strip() for x in (b.assigned_path or []) if str(x).strip()]
        if p:
            folder_catalog[tuple(p)] += 1

    rows = []
    for b in bookmarks:
        rows.append(
            {
                "id": b.id,
                "url": b.final_url or b.url,
                "domain": b.domain,
                "title": b.assigned_title or b.title,
                "path": b.assigned_path or b.folder_path or [],
                "summary": (b.summary or b.page_description or b.content_snippet or "")[:500],
                "current_tags": list(b.tags or []),
            }
        )
    folders = [{"path": list(k), "count": v} for k, v in folder_catalog.items()]
    folders.sort(key=lambda x: (-x["count"], "/".join(x["path"]).lower()))
    return {"bookmarks": rows, "folder_catalog": folders}


def _normalize_and_cap_tags(bookmarks: Sequence[Bookmark], cfg: Settings) -> int:
    changed = 0
    max_per_link = max(1, int(cfg.openai_tags_max_per_link))
    for b in bookmarks:
        cleaned = _normalize_tag_list(b.tags, max_per_link=max_per_link)
        if not cleaned:
            cleaned = [_fallback_tag_for_bookmark(b)]
        if cleaned != (b.tags or []):
            b.tags = cleaned
            changed += 1

    # Enforce global tag cap deterministically by frequency.
    max_global = max(1, int(cfg.openai_tags_max_global))
    counts = Counter()
    first_seen: Dict[str, int] = {}
    for i, b in enumerate(bookmarks):
        for t in b.tags:
            counts[t] += 1
            first_seen.setdefault(t, i)
    allowed = {
        t
        for t, _ in sorted(
            counts.items(),
            key=lambda kv: (-kv[1], first_seen.get(kv[0], 10**9), kv[0]),
        )[:max_global]
    }

    for b in bookmarks:
        filtered = [t for t in b.tags if t in allowed]
        if not filtered:
            fallback = _fallback_tag_for_bookmark(b)
            filtered = [fallback if fallback in allowed else next(iter(allowed))]
        filtered = filtered[:max_per_link]
        if filtered != b.tags:
            b.tags = filtered
            changed += 1
    return changed


def _normalize_tag_list(tags: Iterable[str], *, max_per_link: int) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in tags or []:
        tag = _normalize_tag(raw)
        if not tag or tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
        if len(out) >= max_per_link:
            break
    return out


def _normalize_tag(raw: str) -> str:
    raw_s = (raw or "").strip()
    if not raw_s:
        return ""
    abbr = _normalize_abbreviation(raw_s)
    if abbr:
        return abbr

    s = raw_s.lower()
    # Keep plain lowercase letters only and normalize separators.
    s = s.replace("-", " ").replace("_", " ")
    s = re.sub(r"[^a-z\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    words = [w for w in s.split(" ") if w]
    if not words:
        return ""
    if len(words) > 2:
        words = words[:2]
    tag = "_".join(words)
    if tag.lower() in _STOP_TAGS:
        return ""
    return tag


def _normalize_abbreviation(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = s.replace("-", " ").replace("_", " ")
    s = re.sub(r"[^A-Za-z\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    words = [w for w in s.split(" ") if w]
    if len(words) != 1:
        return ""
    token = words[0]
    # Prompt-driven policy: we preserve uppercase abbreviations produced by the model.
    if token.isupper() and token.isalpha() and 2 <= len(token) <= 3:
        return token
    return ""


def _fallback_tag_for_bookmark(b: Bookmark) -> str:
    if b.domain:
        host = b.domain.lower().strip()
        host = host.split(".")[0]
        t = _normalize_tag(host)
        if t:
            return t
    if b.assigned_path:
        for comp in reversed(b.assigned_path):
            t = _normalize_tag(comp)
            if t:
                return t
    return "link"
