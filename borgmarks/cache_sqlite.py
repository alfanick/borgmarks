from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


@dataclass
class CacheEntry:
    cache_key: str
    url: str
    final_url: Optional[str]
    title: Optional[str]
    tags: List[str]
    categories: List[str]
    status_code: Optional[int]
    visited_at: Optional[str]
    summary: Optional[str]
    html: Optional[str]
    page_title: Optional[str]
    page_description: Optional[str]
    content_snippet: Optional[str]


def init_cache(db_path: Path, *, recreate: bool = False) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if recreate and db_path.exists():
        db_path.unlink()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bookmark_cache (
                cache_key TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                final_url TEXT,
                title TEXT,
                tags_json TEXT NOT NULL DEFAULT '[]',
                categories_json TEXT NOT NULL DEFAULT '[]',
                status_code INTEGER,
                visited_at TEXT,
                summary TEXT,
                html TEXT,
                page_title TEXT,
                page_description TEXT,
                content_snippet TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bookmark_cache_url ON bookmark_cache(url)")


def load_entries(db_path: Path, cache_keys: Iterable[str]) -> Dict[str, CacheEntry]:
    keys = [k for k in cache_keys if k]
    if not keys or not db_path.exists():
        return {}

    placeholders = ",".join(["?"] * len(keys))
    query = (
        "SELECT cache_key, url, final_url, title, tags_json, categories_json, status_code, visited_at, "
        "summary, html, page_title, page_description, content_snippet "
        f"FROM bookmark_cache WHERE cache_key IN ({placeholders})"
    )

    out: Dict[str, CacheEntry] = {}
    with sqlite3.connect(db_path) as conn:
        for row in conn.execute(query, keys):
            out[row[0]] = CacheEntry(
                cache_key=row[0],
                url=row[1],
                final_url=row[2],
                title=row[3],
                tags=_safe_json_array(row[4]),
                categories=_safe_json_array(row[5]),
                status_code=row[6],
                visited_at=row[7],
                summary=row[8],
                html=row[9],
                page_title=row[10],
                page_description=row[11],
                content_snippet=row[12],
            )
    return out


def upsert_entries(db_path: Path, entries: Iterable[CacheEntry]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for e in entries:
        rows.append(
            (
                e.cache_key,
                e.url,
                e.final_url,
                e.title,
                json.dumps(e.tags or [], ensure_ascii=False),
                json.dumps(e.categories or [], ensure_ascii=False),
                e.status_code,
                e.visited_at,
                e.summary,
                e.html,
                e.page_title,
                e.page_description,
                e.content_snippet,
                now,
            )
        )
    if not rows:
        return

    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO bookmark_cache (
                cache_key, url, final_url, title, tags_json, categories_json, status_code, visited_at,
                summary, html, page_title, page_description, content_snippet, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                url=excluded.url,
                final_url=excluded.final_url,
                title=excluded.title,
                tags_json=excluded.tags_json,
                categories_json=excluded.categories_json,
                status_code=excluded.status_code,
                visited_at=excluded.visited_at,
                summary=excluded.summary,
                html=excluded.html,
                page_title=excluded.page_title,
                page_description=excluded.page_description,
                content_snippet=excluded.content_snippet,
                updated_at=excluded.updated_at
            """,
            rows,
        )


def _safe_json_array(value: Optional[str]) -> List[str]:
    if not value:
        return []
    try:
        data = json.loads(value)
        if isinstance(data, list):
            return [str(x) for x in data]
        return []
    except Exception:
        return []
