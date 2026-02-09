from __future__ import annotations

import hashlib
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlparse

from .url_norm import normalize_url


class FaviconsDB:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.conn: sqlite3.Connection | None = None

    def __enter__(self) -> "FaviconsDB":
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def open(self) -> None:
        uri = f"file:{self.db_path.as_posix()}?mode=rw"
        self.conn = sqlite3.connect(uri, uri=True)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def supports_schema(self) -> bool:
        c = self._cursor()
        required = {"moz_pages_w_icons", "moz_icons", "moz_icons_to_pages"}
        rows = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = {str(r[0]) for r in rows}
        return required.issubset(tables)

    def set_page_icon(
        self,
        *,
        page_url: str,
        icon_url: str,
        page_url_hash: int | None = None,
        expire_ms: int | None = None,
    ) -> bool:
        """Associate an icon URL to a page URL using Firefox favicons.sqlite schema.

        Returns True when an insertion/update was made.
        """
        c = self._cursor()
        p = normalize_url(page_url or "")
        i = normalize_url(icon_url or "")
        if not p or not i:
            return False
        if not self.supports_schema():
            return False

        if expire_ms is None:
            expire_ms = int(time.time() * 1000) + 180 * 24 * 3600 * 1000

        page_id = self._get_or_create_page_id(c, p, page_url_hash=page_url_hash)
        page_already_has_icon = c.execute(
            "SELECT 1 FROM moz_icons_to_pages WHERE page_id = ? LIMIT 1",
            (page_id,),
        ).fetchone()
        if page_already_has_icon:
            return False
        icon_id = self._get_or_create_icon_id(c, i, expire_ms=expire_ms)
        existing = c.execute(
            "SELECT 1 FROM moz_icons_to_pages WHERE page_id = ? AND icon_id = ? LIMIT 1",
            (page_id, icon_id),
        ).fetchone()
        if existing:
            return False

        c.execute(
            """
            INSERT INTO moz_icons_to_pages(page_id, icon_id, expire_ms)
            VALUES (?, ?, ?)
            ON CONFLICT(page_id, icon_id) DO UPDATE SET expire_ms=excluded.expire_ms
            """,
            (page_id, icon_id, int(expire_ms)),
        )
        self.conn.commit()
        return True

    def validate_integrity(self) -> None:
        c = self._cursor()
        row = c.execute("PRAGMA integrity_check").fetchone()
        status = str(row[0]) if row is not None else ""
        if status.lower() != "ok":
            raise RuntimeError(f"favicons sqlite integrity_check failed: {status or '<empty>'}")

        fk_rows = c.execute("PRAGMA foreign_key_check").fetchall()
        if fk_rows:
            raise RuntimeError(f"favicons sqlite foreign_key_check failed with {len(fk_rows)} row(s)")

    def dedupe(self) -> int:
        """Deduplicate pages/icons/mappings and enforce one icon mapping per page."""
        if not self.supports_schema():
            return 0
        c = self._cursor()
        removed = 0

        # 1) Merge duplicate page rows by page_url.
        page_dupes = c.execute(
            """
            SELECT page_url, GROUP_CONCAT(id) AS ids
            FROM moz_pages_w_icons
            WHERE page_url IS NOT NULL AND TRIM(page_url) != ''
            GROUP BY page_url
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        for row in page_dupes:
            ids = sorted(int(x) for x in str(row["ids"]).split(",") if str(x).strip())
            keep = ids[0]
            for dup in ids[1:]:
                c.execute("UPDATE moz_icons_to_pages SET page_id = ? WHERE page_id = ?", (keep, dup))
                c.execute("DELETE FROM moz_pages_w_icons WHERE id = ?", (dup,))
                removed += 1

        # 2) Merge duplicate icon rows by icon_url.
        icon_dupes = c.execute(
            """
            SELECT icon_url, GROUP_CONCAT(id) AS ids
            FROM moz_icons
            WHERE icon_url IS NOT NULL AND TRIM(icon_url) != ''
            GROUP BY icon_url
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        for row in icon_dupes:
            ids = sorted(int(x) for x in str(row["ids"]).split(",") if str(x).strip())
            keep = ids[0]
            for dup in ids[1:]:
                c.execute("UPDATE moz_icons_to_pages SET icon_id = ? WHERE icon_id = ?", (keep, dup))
                c.execute("DELETE FROM moz_icons WHERE id = ?", (dup,))
                removed += 1

        # 3) Rebuild mapping table to one icon per page (first icon_id, max expiry).
        canon = c.execute(
            """
            SELECT page_id, MIN(icon_id) AS icon_id, MAX(COALESCE(expire_ms, 0)) AS expire_ms
            FROM moz_icons_to_pages
            GROUP BY page_id
            """
        ).fetchall()
        before = int(c.execute("SELECT COUNT(*) FROM moz_icons_to_pages").fetchone()[0])
        c.execute("DELETE FROM moz_icons_to_pages")
        if canon:
            c.executemany(
                "INSERT INTO moz_icons_to_pages(page_id, icon_id, expire_ms) VALUES (?, ?, ?)",
                [(int(r["page_id"]), int(r["icon_id"]), int(r["expire_ms"])) for r in canon],
            )
        after = int(c.execute("SELECT COUNT(*) FROM moz_icons_to_pages").fetchone()[0])
        removed += max(0, before - after)

        if removed:
            self.conn.commit()
        return removed

    def _get_or_create_page_id(
        self,
        c: sqlite3.Cursor,
        page_url: str,
        *,
        page_url_hash: int | None = None,
    ) -> int:
        row = c.execute(
            "SELECT id FROM moz_pages_w_icons WHERE page_url = ? LIMIT 1",
            (page_url,),
        ).fetchone()
        if row:
            return int(row["id"])
        page_hash = int(page_url_hash) if page_url_hash is not None else _stable_i64_hash(page_url)
        c.execute(
            "INSERT INTO moz_pages_w_icons(page_url, page_url_hash) VALUES (?, ?)",
            (page_url, page_hash),
        )
        return int(c.lastrowid)

    def _get_or_create_icon_id(self, c: sqlite3.Cursor, icon_url: str, *, expire_ms: int) -> int:
        row = c.execute(
            "SELECT id FROM moz_icons WHERE icon_url = ? LIMIT 1",
            (icon_url,),
        ).fetchone()
        if row:
            icon_id = int(row["id"])
            c.execute(
                "UPDATE moz_icons SET expire_ms = ? WHERE id = ?",
                (int(expire_ms), icon_id),
            )
            return icon_id

        c.execute(
            """
            INSERT INTO moz_icons(icon_url, fixed_icon_url_hash, width, root, expire_ms, flags, data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                icon_url,
                _stable_i64_hash(_fixed_icon_url(icon_url)),
                16,
                1 if _looks_like_root_favicon(icon_url) else 0,
                int(expire_ms),
                0,
                None,
            ),
        )
        return int(c.lastrowid)

    def _cursor(self) -> sqlite3.Cursor:
        if self.conn is None:
            raise RuntimeError("database is not open")
        return self.conn.cursor()


def _looks_like_root_favicon(url: str) -> bool:
    p = urlparse(url)
    return (p.path or "").rstrip("/") == "/favicon.ico"


def _fixed_icon_url(url: str) -> str:
    # Firefox uses fixup_url() before hashing. We approximate with a stable
    # normalization that strips scheme and common subdomain prefixes.
    p = urlparse(url)
    host = (p.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = p.path or "/"
    return f"{host}{path}"


def _stable_i64_hash(value: str) -> int:
    # Deterministic 64-bit positive integer for moz_*_hash columns.
    if not value:
        return 0
    d = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(d[:8], "big", signed=False) & 0x7FFF_FFFF_FFFF_FFFF
