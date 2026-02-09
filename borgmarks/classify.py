from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from .config import Settings
from .log import get_logger
from .model import Bookmark
from .openai_client import classify_batch

log = get_logger(__name__)


SYSTEM_PROMPT = """    You are organizing browser bookmarks for one technical user.

Rules:
- Create a folder taxonomy derived from the inputs. Not all folders are predefined.
- Folder depth <= 4 (path array length <= 4).
- Folder names: short; may use 1-2 emojis as prefix. Avoid long sentences.
- Tags: lowercase, ASCII, no spaces (use '-' if needed). Max 5 tags.
- Don't invent URLs. Don't drop bookmarks.
- Prefer stable top-level buckets, but invent new ones if it improves clarity:
  Computers, Admin, Shopping, Travelling, News, Sport, Photography, Art, Utilities, Fun, Archive.
- Use the provided page summary/snippet and existing folder path as hints.

Output must be strict JSON for the schema (no extra text).
"""


def classify_bookmarks(bookmarks: List[Bookmark], cfg: Settings) -> None:
    n_total = len(bookmarks)
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

    batch_size = 40
    batches = [target[i:i + batch_size] for i in range(0, len(target), batch_size)]
    log.info(
        "Classifying %d bookmarks in %d batches (batch_size=%d, jobs=%d, model=%s)",
        len(target),
        len(batches),
        batch_size,
        cfg.openai_jobs,
        cfg.openai_model,
    )

    def _run_batch(batch: List[Bookmark]):
        payload = []
        for b in batch:
            payload.append(
                {
                    "id": b.id,
                    "title": b.title,
                    "url": b.final_url or b.url,
                    "domain": b.domain,
                    "existing_path": b.folder_path,
                    "summary": (b.summary or b.page_description or b.content_snippet or "")[:600],
                }
            )
        return classify_batch(
            model=cfg.openai_model,
            timeout_s=cfg.openai_timeout_s,
            system_prompt=SYSTEM_PROMPT,
            user_payload=json.dumps({"bookmarks": payload}, ensure_ascii=False),
        )

    id_to_bm: Dict[str, Bookmark] = {b.id: b for b in target}
    errors = 0

    with ThreadPoolExecutor(max_workers=max(1, cfg.openai_jobs)) as ex:
        futs = [ex.submit(_run_batch, batch) for batch in batches]
        for fut in as_completed(futs):
            try:
                res = fut.result()
                for a in res.parsed.assignments:
                    b = id_to_bm.get(a.id)
                    if not b:
                        continue
                    b.assigned_path = (a.path or [])[: cfg.max_depth] or ["Archive", "Unclassified"]
                    if a.title:
                        b.assigned_title = a.title
                    b.tags = (a.tags or [])[:10]
                    b.meta["openai_ms"] = str(res.ms)
            except Exception as e:
                errors += 1
                log.error("OpenAI batch failed: %s", e)

    if errors:
        log.warning(
            "OpenAI classification had %d batch errors; affected bookmarks remain in Archive/Unclassified.",
            errors,
        )
        for b in target:
            if not b.assigned_path:
                b.assigned_path = ["Archive", "Unclassified (errors)"]
