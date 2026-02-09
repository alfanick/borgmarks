from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Sequence, Tuple

from .config import Settings
from .log import get_logger
from .model import Bookmark
from .openai_client import classify_batch

log = get_logger(__name__)


SYSTEM_PROMPT_CLASSIFY = """You are organizing browser bookmarks for one technical user.

Rules:
- Create a folder taxonomy derived from the inputs. Not all folders are predefined.
- Folder depth <= 4 (path array length <= 4).
- Folder names: short; may use 1-2 emojis as prefix. Avoid long sentences.
- Tags: lowercase, ASCII, no spaces (use '-' if needed). Max 5 tags.
- Don't invent URLs. Don't drop bookmarks.
- Prefer stable top-level buckets, but invent new ones if it improves clarity:
  Computers, Admin, Shopping, Travelling, News, Sport, Photography, Art, Utilities, Fun, Archive.
- Use the provided page summary/snippet and existing folder path as strong hints.
- Prefer reusing existing_path when it is reasonable; do not reshuffle aggressively.
- Reuse existing folder names and paths whenever possible; only introduce new folders when clearly better.
- Context: input URLs come from an exported iOS/iPadOS Safari Netscape Bookmark HTML file.
- Context: output will be a Firefox-importable Netscape Bookmark HTML file.

Output must be strict JSON for the schema (no extra text).
"""


SYSTEM_PROMPT_RECLASSIFY = """You are reclassifying already-classified browser bookmarks.

Rules:
- Use only: prior classification, prior tags, and summary text.
- Keep folder depth <= 4.
- Reuse folder paths from folder_catalog whenever possible.
- Be conservative: keep current_path unless there is clear improvement.
- Do not move bookmarks between folders unless there is a strong semantic reason.
- If current_path is acceptable, keep it unchanged.
- Avoid moving between closely related sibling folders (e.g. Camera <-> Video) unless strongly justified.
- Avoid creating singleton folders; prefer placing multiple related links in the same folder.
- Keep tags concise and lowercase.
- Don't drop bookmarks.
- Context: input URLs come from an exported iOS/iPadOS Safari Netscape Bookmark HTML file.
- Context: output will be a Firefox-importable Netscape Bookmark HTML file.

Output must be strict JSON for the schema (no extra text).
"""


def classify_bookmarks(bookmarks: List[Bookmark], cfg: Settings) -> set[str]:
    preassigned = [b for b in bookmarks if b.assigned_path]
    if preassigned:
        log.info("Reusing cached classification for %d bookmarks.", len(preassigned))
    bookmarks = [b for b in bookmarks if not b.assigned_path]

    n_total = len(bookmarks)
    if n_total == 0:
        return set()
    if cfg.openai_max_bookmarks > 0 and n_total > cfg.openai_max_bookmarks:
        log.warning(
            "OpenAI classification capped to first %d/%d bookmarks (BORG_OPENAI_MAX_BOOKMARKS).",
            cfg.openai_max_bookmarks,
            n_total,
        )
        target = bookmarks[: cfg.openai_max_bookmarks]
        rest = bookmarks[cfg.openai_max_bookmarks :]
    else:
        target = bookmarks
        rest = []

    for b in rest:
        b.assigned_path = ["Archive", "Unclassified (overflow)"]

    _classify_phase(
        phase_name="classify",
        target=target,
        cfg=cfg,
        system_prompt=SYSTEM_PROMPT_CLASSIFY,
        payload_kind="initial",
        folder_catalog=[],
    )

    if cfg.openai_reclassify and target:
        folder_catalog = _folder_catalog(target)
        _classify_phase(
            phase_name="reclassify",
            target=target,
            cfg=cfg,
            system_prompt=SYSTEM_PROMPT_RECLASSIFY,
            payload_kind="reclassify",
            folder_catalog=folder_catalog,
        )
    touched = {b.id for b in target}
    touched.update(b.id for b in rest)
    return touched


def _classify_phase(
    *,
    phase_name: str,
    target: List[Bookmark],
    cfg: Settings,
    system_prompt: str,
    payload_kind: str,
    folder_catalog: List[dict],
) -> None:
    if not target:
        return
    batch_size = 40
    batches = [target[i:i + batch_size] for i in range(0, len(target), batch_size)]
    log.info(
        "OpenAI %s: %d bookmarks in %d batches (batch_size=%d, jobs=%d, model=%s, timeout_s=%d)",
        phase_name,
        len(target),
        len(batches),
        batch_size,
        cfg.openai_jobs,
        cfg.openai_model,
        cfg.openai_timeout_s,
    )

    id_to_bm: Dict[str, Bookmark] = {b.id: b for b in target}
    allowed_paths = {tuple(x["path"]) for x in folder_catalog if x.get("path")}
    folder_sizes = {tuple(x["path"]): int(x.get("count", 0) or 0) for x in folder_catalog if x.get("path")}
    errors = 0

    def _run_batch(batch_idx: int, batch: List[Bookmark]):
        if payload_kind == "reclassify":
            payload = _payload_for_reclassify(batch, folder_catalog)
        else:
            payload = _payload_for_initial(batch)
        return batch, classify_batch(
            model=cfg.openai_model,
            timeout_s=cfg.openai_timeout_s,
            max_output_tokens=cfg.openai_max_output_tokens,
            system_prompt=system_prompt,
            user_payload=json.dumps(payload, ensure_ascii=False),
            phase_label=phase_name,
            batch_label=f"batch-{batch_idx + 1}/{len(batches)}",
            use_browser_tool=cfg.openai_agent_browser,
            reasoning_effort=cfg.openai_reasoning_effort,
        )

    with ThreadPoolExecutor(max_workers=max(1, cfg.openai_jobs)) as ex:
        futs = [ex.submit(_run_batch, i, batch) for i, batch in enumerate(batches)]
        for fut in as_completed(futs):
            try:
                batch, res = fut.result()
                _apply_assignments(
                    batch=batch,
                    id_to_bm=id_to_bm,
                    cfg=cfg,
                    assignments=res.parsed.assignments,
                    allowed_paths=allowed_paths,
                    folder_sizes=folder_sizes,
                    phase_name=phase_name,
                    openai_ms=res.ms,
                )
            except Exception as e:
                errors += 1
                log.exception("OpenAI %s batch failed: %s", phase_name, e)

    if errors:
        log.warning(
            "OpenAI %s had %d batch errors; missing assignments fall back to Archive/Unclassified.",
            phase_name,
            errors,
        )
    for b in target:
        if not b.assigned_path:
            b.assigned_path = ["Archive", "Unclassified (errors)"]


def _payload_for_initial(batch: Sequence[Bookmark]) -> dict:
    payload = []
    for b in batch:
        payload.append(
            {
                "id": b.id,
                "title": b.title,
                "url": b.final_url or b.url,
                "domain": b.domain,
                "existing_path": b.folder_path,
                "summary": (b.summary or b.page_description or b.content_snippet or "")[:1200],
            }
        )
    return {"bookmarks": payload}


def _payload_for_reclassify(batch: Sequence[Bookmark], folder_catalog: List[dict]) -> dict:
    payload = []
    for b in batch:
        payload.append(
            {
                "id": b.id,
                "current_path": b.assigned_path,
                "current_title": b.assigned_title or b.title,
                "current_tags": b.tags,
                "summary": (b.summary or b.page_description or b.content_snippet or "")[:1200],
            }
        )
    return {"bookmarks": payload, "folder_catalog": folder_catalog}


def _apply_assignments(
    *,
    batch: Sequence[Bookmark],
    id_to_bm: Dict[str, Bookmark],
    cfg: Settings,
    assignments,
    allowed_paths: set[Tuple[str, ...]],
    folder_sizes: Dict[Tuple[str, ...], int],
    phase_name: str,
    openai_ms: int,
) -> None:
    expected_ids = {b.id for b in batch}
    seen_ids = set()
    for a in assignments:
        b = id_to_bm.get(a.id)
        if not b:
            log.warning("OpenAI %s returned unknown id: %s", phase_name, a.id)
            continue
        seen_ids.add(a.id)

        prev_path = list(b.assigned_path) if b.assigned_path else ["Archive", "Unclassified"]
        new_path = (a.path or [])[: cfg.max_depth]
        if not new_path:
            new_path = prev_path
        if phase_name == "reclassify" and allowed_paths and tuple(new_path) not in allowed_paths:
            # Enforce folder reuse in pass-2; if a new folder appears, keep previous folder.
            new_path = prev_path
        elif phase_name == "reclassify" and cfg.reclassify_conservative:
            allowed, reason = _allow_conservative_reclass_move(
                prev_path=prev_path,
                new_path=new_path,
                folder_sizes=folder_sizes,
                min_folder_gain=cfg.reclassify_min_folder_gain,
            )
            if not allowed:
                target = b.domain or (b.final_url or b.url)
                log.debug(
                    "Keeping %s in %s (reclass move to %s blocked: %s)",
                    target,
                    "/".join(prev_path),
                    "/".join(new_path),
                    reason,
                )
                new_path = prev_path

        if phase_name == "reclassify" and new_path != prev_path:
            target = b.domain or (b.final_url or b.url)
            log.info(
                "Moving %s from %s to %s",
                target,
                "/".join(prev_path),
                "/".join(new_path),
            )

        b.assigned_path = new_path
        if a.title:
            b.assigned_title = a.title
        b.tags = (a.tags or [])[:10]
        b.meta["openai_ms"] = str(openai_ms)

    missing = sorted(expected_ids - seen_ids)
    if missing:
        log.warning(
            "OpenAI %s returned incomplete assignments: missing=%d/%d",
            phase_name,
            len(missing),
            len(expected_ids),
        )
        for mid in missing:
            b = id_to_bm[mid]
            if not b.assigned_path:
                b.assigned_path = ["Archive", "Unclassified (missing)"]


def _folder_catalog(bookmarks: Sequence[Bookmark]) -> List[dict]:
    counts: Dict[Tuple[str, ...], int] = {}
    for b in bookmarks:
        if not b.assigned_path:
            continue
        key = tuple(b.assigned_path)
        counts[key] = counts.get(key, 0) + 1
    rows = [{"path": list(k), "count": v} for k, v in counts.items()]
    rows.sort(key=lambda x: (-x["count"], "/".join(x["path"]).lower()))
    return rows


def _allow_conservative_reclass_move(
    *,
    prev_path: List[str],
    new_path: List[str],
    folder_sizes: Dict[Tuple[str, ...], int],
    min_folder_gain: int,
) -> Tuple[bool, str]:
    if new_path == prev_path:
        return True, "same-path"
    if _is_generic_path(prev_path):
        return True, "from-generic-path"

    prev_size = folder_sizes.get(tuple(prev_path), 0)
    new_size = folder_sizes.get(tuple(new_path), 0)
    gain = new_size - prev_size
    min_gain = max(0, int(min_folder_gain))
    shared = _shared_prefix_len(prev_path, new_path)

    same_top = bool(prev_path and new_path and _norm_token(prev_path[0]) == _norm_token(new_path[0]))
    near_sibling = shared >= max(0, min(len(prev_path), len(new_path)) - 1)
    strong_cluster = new_size >= max(3, prev_size + min_gain)

    if near_sibling and not strong_cluster:
        return False, f"near-sibling move without strong cluster (prev={prev_size}, new={new_size}, gain={gain})"
    if same_top and gain < min_gain:
        return False, f"same-top move below gain threshold (prev={prev_size}, new={new_size}, gain={gain})"
    return True, "accepted"


def _is_generic_path(path: List[str]) -> bool:
    if not path:
        return True
    text = "/".join(path).lower()
    generic_tokens = (
        "unclassified",
        "inbox",
        "misc",
        "other",
        "archive",
        "reading",
        "overflow",
    )
    return any(tok in text for tok in generic_tokens)


def _shared_prefix_len(a: List[str], b: List[str]) -> int:
    n = min(len(a), len(b))
    i = 0
    while i < n and _norm_token(a[i]) == _norm_token(b[i]):
        i += 1
    return i


def _norm_token(s: str) -> str:
    return "".join(ch.lower() for ch in s if ch.isalnum())
