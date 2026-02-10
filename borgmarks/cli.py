
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from . import __version__
from .cache_sqlite import CacheEntry, init_cache, load_entries, upsert_entries
from .classify import classify_bookmarks
from .config import load_settings
from .domain_lang import domain_of, guess_lang
from .firefox_sync import SyncStats, apply_bookmarks_to_firefox
from .fetch import fetch_many
from .folder_emoji import enrich_folder_emojis
from .log import LogConfig, get_logger, setup_logging
from .parse_firefox_places import parse_firefox_places
from .parse_netscape import parse_bookmarks_html
from .split import enforce_leaf_limits
from .tagging import enrich_bookmark_tags
from .url_norm import normalize_url
from .writer_netscape import build_tree, write_firefox_html

log = get_logger(__name__)

DEFAULT_TOOLBAR = {
    "folders": ["Now / Inbox", "Computers", "Admin"],
    "links": [
        {"title": "GitHub", "url": "https://github.com/", "tags": ["dev"]},
        {"title": "Hacker News", "url": "https://news.ycombinator.com/", "tags": ["news", "tech"]},
        {"title": "Wikipedia", "url": "https://en.wikipedia.org/", "tags": ["reference"]},
        {"title": "Maps", "url": "https://www.google.com/maps", "tags": ["maps"]},
        {"title": "Xhalf", "url": "https://xhalf.nakarmamana.ch/", "tags": ["photos"]},
        {"title": "Strava Heatmap", "url": "https://www.strava.com/heatmap", "tags": ["sport", "running"]},
        {"title": "MeteoSwiss", "url": "https://www.meteoswiss.admin.ch/", "tags": ["weather", "ch"]},
    ],
}


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="borgmarks",
        description="AI-assisted bookmark organizer (iOS/Safari export -> Firefox import HTML).",
    )
    p.add_argument("-V", "--version", action="version", version=f"borgmarks {__version__}")
    p.add_argument("--config", default=None, help="YAML config file (optional). Env vars override defaults.")
    sub = p.add_subparsers(dest="cmd", required=True)

    org = sub.add_parser("organize", help="Organize an iOS/Safari bookmarks HTML export for Firefox import.")
    org.add_argument("--ios-html", required=False, help="Optional iOS/Safari bookmarks HTML export (Netscape format).")
    org.add_argument("--firefox-profile", required=True, help="Firefox profile dir (used for input, cache, output, and optional apply).")
    org.add_argument("--out", required=False, help="Deprecated: output path is now fixed to <firefox-profile>/bookmarks.organized.html.")
    org.add_argument("--state-dir", required=False, help="Deprecated: state sidecars are written under firefox profile.")
    org.add_argument("--log-level", default=None, help="DEBUG/INFO/WARN/ERROR (overrides env/config).")
    org.add_argument("--no-color", action="store_true", help="Disable colored logging.")
    org.add_argument("--no-fetch", action="store_true", help="Skip website fetching (faster, less accurate).")
    org.add_argument("--no-openai", action="store_true", help="Skip OpenAI classification (fallback bucketing).")
    org.add_argument(
        "--no-folder-emoji",
        action="store_true",
        help="Disable OpenAI folder emoji enrichment for this run (default: enabled).",
    )
    org.add_argument("--skip-cache", action="store_true", help="Recreate SQLite cache and skip reading existing cache data.")
    org.add_argument(
        "--apply-firefox",
        action="store_true",
        help="Apply organized bookmarks/tags/folders back into Firefox places.sqlite (bookmarks only, no history).",
    )
    org.add_argument("--dry-run", action="store_true", help="Run pipeline but do not write output file.")
    org.add_argument("--backup-firefox", action="store_true", help="If firefox-profile is given, back up places.sqlite to state-dir.")

    args = p.parse_args(argv)
    cfg = load_settings(args.config)
    if args.log_level:
        cfg.log_level = args.log_level
    if args.no_color:
        cfg.no_color = True
    if args.no_folder_emoji:
        cfg.openai_folder_emoji_enrich = False
    setup_logging(LogConfig(level=cfg.log_level, no_color=cfg.no_color))

    if args.cmd == "organize":
        return _cmd_organize(args, cfg)
    return 2


def _cmd_organize(args, cfg) -> int:
    t0 = time.time()
    ios_html = Path(args.ios_html) if args.ios_html else None
    if ios_html is not None and not ios_html.exists():
        log.error("Input file not found: %s", ios_html)
        return 2

    profile_dir = Path(args.firefox_profile)
    if not profile_dir.exists():
        log.error("Firefox profile not found: %s", profile_dir)
        return 2
    if args.out:
        log.warning("--out is deprecated and ignored. Using <firefox-profile>/bookmarks.organized.html.")
    if args.state_dir:
        log.warning("--state-dir is deprecated and ignored. Using <firefox-profile> for sidecars/state.")

    out_path = profile_dir / "bookmarks.organized.html"
    state_dir = profile_dir
    state_dir.mkdir(parents=True, exist_ok=True)
    firefox_places = _resolve_places_db_path(profile_dir)
    favicons_db = _resolve_favicons_db_path(profile_dir, firefox_places)
    cache_db = profile_dir / "borg_cache.sqlite"
    cache_db.parent.mkdir(parents=True, exist_ok=True)
    init_cache(cache_db, recreate=args.skip_cache)

    if firefox_places and firefox_places.exists():
        begin_backup = _backup_firefox_to_tmp(firefox_places, phase="begin", label="places")
        log.info("Firefox places backup (begin): %s", begin_backup)
    if favicons_db and favicons_db.exists():
        begin_backup = _backup_firefox_to_tmp(favicons_db, phase="begin", label="favicons")
        log.info("Firefox favicons backup (begin): %s", begin_backup)

    def _finish(code: int) -> int:
        if firefox_places and firefox_places.exists():
            try:
                end_backup = _backup_firefox_to_tmp(firefox_places, phase="end", label="places")
                log.info("Firefox places backup (end): %s", end_backup)
            except Exception as e:
                log.warning("Firefox places end-backup failed: %s", e)
        if favicons_db and favicons_db.exists():
            try:
                end_backup = _backup_firefox_to_tmp(favicons_db, phase="end", label="favicons")
                log.info("Firefox favicons backup (end): %s", end_backup)
            except Exception as e:
                log.warning("Firefox favicons end-backup failed: %s", e)
        if code == 0:
            log.info("Done in %d ms.", int((time.time() - t0) * 1000))
        return code

    if args.apply_firefox:
        lock_msg = _preflight_firefox_write_locks(firefox_places, favicons_db)
        if lock_msg:
            log.error("%s", lock_msg)
            return _finish(2)

    if args.backup_firefox:
        _backup_firefox_profile(profile_dir, state_dir)

    ios_bookmarks = []
    if ios_html is not None:
        try:
            ios_bookmarks, _root_title = parse_bookmarks_html(ios_html)
        except Exception as e:
            log.error("Failed to parse bookmarks HTML: %s", e)
            return _finish(2)
        log.info("Parsed %d bookmarks from iOS export: %s", len(ios_bookmarks), ios_html)
    else:
        log.info("No --ios-html provided; running with Firefox bookmarks only.")

    firefox_bookmarks = []
    if firefox_places:
        try:
            firefox_bookmarks = parse_firefox_places(firefox_places)
            log.info(
                "Parsed %d bookmarks from Firefox profile: %s",
                len(firefox_bookmarks),
                args.firefox_profile,
            )
        except Exception as e:
            log.warning("Failed to parse Firefox places.sqlite (%s): %s", args.firefox_profile, e)

    # Source merge happens before any normalization/dedupe so iOS and Firefox have equal priority.
    bookmarks = list(ios_bookmarks) + list(firefox_bookmarks)
    _assign_sequential_ids(bookmarks)
    log.info("Merged %d total bookmarks from iOS + Firefox sources.", len(bookmarks))

    # Normalize + derive domain/lang + dedupe
    seen = set()
    deduped = []
    exact_dupes = 0
    for b in bookmarks:
        b.url = normalize_url(b.url)
        b.domain = domain_of(b.url)
        b.lang = guess_lang(b.url, b.title)

        if b.url in seen and not cfg.keep_duplicates:
            exact_dupes += 1
            continue
        seen.add(b.url)
        deduped.append(b)
    bookmarks = deduped
    if exact_dupes:
        log.info("Deduped %d duplicates (set BORG_KEEP_DUPLICATES=1 to keep).", exact_dupes)

    # Cache prefill (unless explicitly skipped)
    if not args.skip_cache:
        cache_keys = [_url_identity(b.url) for b in bookmarks]
        cached = load_entries(cache_db, cache_keys)
        hits = 0
        for b in bookmarks:
            c = cached.get(_url_identity(b.url))
            if not c:
                continue
            _apply_cache_entry(b, c)
            hits += 1
        if hits:
            log.info("Loaded %d bookmark entries from SQLite cache: %s", hits, cache_db)

    # Fetch subset (visit websites)
    if not args.no_fetch:
        fetch_scope = bookmarks[: cfg.fetch_max_urls]
        fetch_targets = [b for b in fetch_scope if b.http_status is None]
        urls = [b.url for b in fetch_targets]
        log.info("Fetching %d/%d URLs (backend=%s, jobs=%d)...", len(urls), len(bookmarks), cfg.fetch_backend, cfg.fetch_jobs)
        try:
            results = fetch_many(
                urls,
                backend=cfg.fetch_backend,
                jobs=cfg.fetch_jobs,
                timeout_s=cfg.fetch_timeout_s,
                user_agent=cfg.fetch_user_agent,
                max_bytes=cfg.fetch_max_bytes,
            )
            fetched_cache_rows: List[CacheEntry] = []
            for b in fetch_targets:
                original_url = b.url
                r = results.get(b.url)
                if not r:
                    continue
                b.fetched_ok = r.ok
                b.http_status = r.status
                b.final_url = r.final_url
                b.page_title = r.title
                b.page_description = r.description
                b.content_snippet = r.snippet
                if r.favicon_url:
                    b.meta["icon_uri"] = r.favicon_url
                b.page_html = r.html
                b.meta["fetch_ms"] = str(r.fetch_ms)
                b.meta["visited_at"] = _utc_now_iso()
                fetched_cache_rows.extend(_cache_entries_for_bookmark(b, original_url=original_url))
            if fetched_cache_rows:
                upsert_entries(cache_db, fetched_cache_rows)
                log.info("Stored %d fetched bookmark entries in SQLite cache.", len(fetched_cache_rows))
        except Exception as e:
            log.warning("Fetch phase failed: %s", e)
    else:
        log.info("Skipping fetch phase (--no-fetch).")

    # Replace original URLs with redirected finals for downstream processing.
    for b in bookmarks:
        if b.final_url:
            b.url = normalize_url(b.final_url)
            b.domain = domain_of(b.url)
            b.lang = guess_lang(b.url, b.title)

    # Near-duplicate cleanup (after redirects are known).
    near_dupes = 0
    if not cfg.keep_duplicates:
        before = len(bookmarks)
        bookmarks = _dedupe_near_duplicates(bookmarks)
        near_dupes = before - len(bookmarks)
        if near_dupes:
            log.info("Removed %d near-duplicates after redirect normalization.", near_dupes)

    sanity_input = list(bookmarks)

    # Pre-summary from meta/snippet
    for b in bookmarks:
        if b.page_description:
            b.summary = b.page_description
        elif b.content_snippet:
            b.summary = b.content_snippet[: cfg.summary_max_chars]

    skip_openai_via_cache = _all_bookmarks_have_cached_openai_enrichment(bookmarks)
    openai_enabled = not args.no_openai and not skip_openai_via_cache

    # OpenAI categorization
    newly_assigned_ids: set[str] = set()
    if args.no_openai:
        log.warning("Skipping OpenAI classification (--no-openai). Using fallback bucketing.")
        newly_assigned_ids = _fallback_assign(bookmarks)
    elif skip_openai_via_cache:
        log.info(
            "Skipping OpenAI classification/tagging/emoji: cache already has summary+category for all %d bookmarks.",
            len(bookmarks),
        )
    else:
        if not (os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")):
            log.error("OPENAI_API_KEY not set. Set it or use --no-openai.")
            return _finish(2)
        newly_assigned_ids = classify_bookmarks(bookmarks, cfg)

    # Dead links handling
    if not cfg.drop_dead:
        for b in bookmarks:
            if _is_strictly_inaccessible(b.http_status):
                b.assigned_path = ["Archive", "🪦 Dead links"]
    else:
        before = len(bookmarks)
        bookmarks = [b for b in bookmarks if not _is_strictly_inaccessible(b.http_status)]
        dropped = before - len(bookmarks)
        if dropped:
            log.warning("Dropped %d strictly inaccessible links (BORG_DROP_DEAD=0 to keep in Archive).", dropped)

    # Language prefix for non-English (English = no prefix)
    if cfg.prefix_non_english:
        for b in bookmarks:
            if b.lang != "EN":
                t = b.assigned_title or b.title
                if not t.startswith(f"[{b.lang}]"):
                    b.assigned_title = f"[{b.lang}] {t}"

    _normalize_category_paths(bookmarks)

    # Leaf folder cap
    enforce_leaf_limits(bookmarks, leaf_max_links=cfg.leaf_max_links, max_depth=cfg.max_depth)
    if not openai_enabled:
        # Keep tags normalized/capped even without OpenAI calls.
        prev = cfg.openai_tags_enrich
        cfg.openai_tags_enrich = False
        try:
            enrich_bookmark_tags(bookmarks, cfg)
        finally:
            cfg.openai_tags_enrich = prev
    elif cfg.openai_tags_enrich:
        enrich_bookmark_tags(bookmarks, cfg)

    primary_sync: SyncStats | None = None
    secondary_sync: SyncStats | None = None
    if args.apply_firefox:
        try:
            primary_sync = apply_bookmarks_to_firefox(
                firefox_places,
                bookmarks,
                favicons_db_path=favicons_db,
                apply_icons=False,
                dedupe=True,
            )
            _log_firefox_sync_stats("phase-1 links/tags", primary_sync)
        except Exception as e:
            log.error("Failed to apply phase-1 links/tags to Firefox places.sqlite: %s", e)
            return _finish(2)

    if openai_enabled and cfg.openai_folder_emoji_enrich:
        # Apply missing folder emojis across the whole tree (existing + new),
        # but never replace already present emoji prefixes.
        if newly_assigned_ids:
            try:
                enrich_folder_emojis(bookmarks, cfg, target_ids=newly_assigned_ids)
                _normalize_category_paths(bookmarks)
            except Exception as e:
                log.warning(
                    "Folder emoji enrichment failed after links were applied; keeping existing links unchanged: %s",
                    e,
                )
        else:
            log.info("Skipping folder emoji enrichment: no newly classified links in this run.")

    _log_link_progress(bookmarks, phase="organize")

    # Sidecar metadata
    if cfg.write_sidecar_jsonl:
        sidecar = state_dir / (out_path.stem + ".meta.jsonl")
        try:
            with sidecar.open("w", encoding="utf-8") as f:
                for b in bookmarks:
                    f.write(json.dumps({
                        "id": b.id,
                        "url": b.final_url or b.url,
                        "domain": b.domain,
                        "lang": b.lang,
                        "path": b.assigned_path,
                        "title": b.assigned_title or b.title,
                        "tags": b.tags,
                        "http_status": b.http_status,
                        "fetch_ms": b.meta.get("fetch_ms"),
                        "openai_ms": b.meta.get("openai_ms"),
                        "summary": (b.summary or "")[: cfg.summary_max_chars],
                    }, ensure_ascii=False) + "\n")
            log.info("Wrote sidecar metadata JSONL: %s", sidecar)
        except Exception as e:
            log.warning("Failed to write sidecar metadata: %s", e)

    tree = build_tree(bookmarks)
    if args.dry_run:
        log.info("Dry-run: not writing output file.")
    else:
        try:
            write_firefox_html(
                out_path=out_path,
                bookmarks_tree=tree,
                toolbar_spec=DEFAULT_TOOLBAR,
                embed_metadata=cfg.embed_metadata_in_html,
                title_root="Bookmarks (borgmarks)",
            )
        except Exception as e:
            log.error("Failed to write output HTML: %s", e)
            return _finish(2)

    if args.apply_firefox:
        try:
            secondary_sync = apply_bookmarks_to_firefox(
                firefox_places,
                bookmarks,
                favicons_db_path=favicons_db,
                apply_icons=True,
                dedupe=False,
            )
            _log_firefox_sync_stats("phase-2 emoji/icons", secondary_sync)
        except Exception as e:
            if primary_sync is not None:
                log.warning(
                    "Phase-2 emoji/icons apply failed; phase-1 links are already persisted in Firefox: %s",
                    e,
                )
            else:
                log.error("Failed to apply to Firefox places.sqlite: %s", e)
                return _finish(2)

        combined = _merge_sync_stats(primary_sync, secondary_sync)
        _log_firefox_sync_stats("combined", combined)

    # Store current bookmark state in cache (including categories/tags and summaries).
    upsert_entries(cache_db, [_cache_entry_from_bookmark(b) for b in bookmarks])

    _log_run_stats(bookmarks, exact_dupes=exact_dupes, near_dupes=near_dupes)

    # End-of-run guard: the pipeline must preserve unique links, except non-200.
    if not _sanity_check_unique_link_counts(sanity_input, bookmarks):
        return _finish(2)
    return _finish(0)


def _backup_firefox_profile(profile: Path, state_dir: Path) -> None:
    try:
        places = profile / "places.sqlite"
        if places.exists():
            ts = int(time.time())
            dest = state_dir / f"places.sqlite.bak.{ts}"
            dest.write_bytes(places.read_bytes())
            log.info("Backed up %s -> %s", places, dest)
        else:
            log.warning("Firefox profile has no places.sqlite: %s", profile)
    except Exception as e:
        log.warning("Firefox backup failed: %s", e)


def _resolve_places_db_path(profile_or_db: Path) -> Path:
    p = Path(profile_or_db)
    if p.is_file():
        return p
    db = p / "places.sqlite"
    return db


def _backup_firefox_to_tmp(places_db: Path, *, phase: str, label: str) -> Path:
    tmp_dir = Path("/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    dst = tmp_dir / f"borgmarks-{label}-{phase}-{ts}.sqlite"
    dst.write_bytes(places_db.read_bytes())
    return dst


def _resolve_favicons_db_path(profile_or_db: Path, places_db: Path) -> Path:
    p = Path(profile_or_db)
    if p.is_dir():
        return p / "favicons.sqlite"
    return places_db.parent / "favicons.sqlite"


def _preflight_firefox_write_locks(places_db: Path, favicons_db: Path | None) -> str:
    places_msg = _sqlite_write_lock_error(places_db, label="places.sqlite")
    if places_msg:
        return places_msg
    if favicons_db and favicons_db.exists():
        favicons_msg = _sqlite_write_lock_error(favicons_db, label="favicons.sqlite")
        if favicons_msg:
            return favicons_msg
    return ""


def _sqlite_write_lock_error(db_path: Path, *, label: str, timeout_ms: int = 1200) -> str:
    conn: sqlite3.Connection | None = None
    try:
        uri = f"file:{db_path.as_posix()}?mode=rw"
        conn = sqlite3.connect(uri, uri=True, timeout=max(0.1, timeout_ms / 1000.0))
        conn.execute(f"PRAGMA busy_timeout={max(100, int(timeout_ms))}")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("ROLLBACK")
        return ""
    except sqlite3.OperationalError as e:
        msg = str(e).strip()
        lower = msg.lower()
        if "locked" in lower or "busy" in lower:
            return f"{label} is locked at {db_path}. Close Firefox and rerun."
        return f"Cannot write {label} at {db_path}: {msg}"
    except Exception as e:
        return f"Cannot access {label} at {db_path}: {e}"
    finally:
        if conn is not None:
            conn.close()


def _fallback_assign(bookmarks) -> set[str]:
    touched: set[str] = set()
    for b in bookmarks:
        if b.assigned_path:
            continue
        d = (b.domain or "").lower()
        if "github.com" in d or "stackoverflow.com" in d:
            b.assigned_path = ["Computers", "🧑‍💻 Dev"]
        elif "wikipedia.org" in d:
            b.assigned_path = ["Computers", "📚 Reference"]
        elif any(x in d for x in ["allegro", "ebay", "nike", "ricardo"]):
            b.assigned_path = ["Shopping", "🛒 Marketplaces"]
        elif "strava" in d:
            b.assigned_path = ["Sport", "🏃 Running"]
        elif "youtube.com" in d:
            b.assigned_path = ["News", "📺 Video"]
        else:
            b.assigned_path = ["Reading", "📥 Inbox"]
        touched.add(b.id)
    if touched:
        log.info("Fallback classified %d uncategorized bookmarks.", len(touched))
    return touched


def _assign_sequential_ids(bookmarks: Iterable) -> None:
    for i, b in enumerate(bookmarks):
        b.id = f"b{i + 1}"


def _normalize_category_paths(bookmarks: Iterable) -> None:
    # Canonicalize folder names per parent path so emoji-prefixed variants map
    # to one bucket, e.g. "👕 Clothing" and "Clothing".
    by_parent_key = {}
    rows = list(bookmarks)
    for b in rows:
        raw = list(b.assigned_path or [])
        if not raw:
            continue
        parent_key_tuple = tuple()
        for comp in raw:
            key = _folder_name_key(comp)
            map_key = (parent_key_tuple, key)
            by_parent_key.setdefault(map_key, {})
            by_parent_key[map_key][str(comp).strip()] = by_parent_key[map_key].get(str(comp).strip(), 0) + 1
            parent_key_tuple = tuple(list(parent_key_tuple) + [key])

    canon_by_parent_and_key = {}
    for map_key, candidates in by_parent_key.items():
        # Deterministic canonical selection:
        # 1) most frequent
        # 2) prefer emoji-prefixed label
        # 3) shortest base text
        # 4) lexical
        ordered = sorted(
            candidates.items(),
            key=lambda kv: (
                -kv[1],
                0 if _has_leading_emoji(kv[0]) else 1,
                len(_strip_leading_non_alnum(kv[0])),
                kv[0].lower(),
            ),
        )
        canon_by_parent_and_key[map_key] = ordered[0][0]

    for b in rows:
        raw = list(b.assigned_path or [])
        if not raw:
            continue
        norm_path = []
        parent_key_tuple = tuple()
        changed = False
        for comp in raw:
            key = _folder_name_key(comp)
            map_key = (parent_key_tuple, key)
            canonical = canon_by_parent_and_key.get(map_key, str(comp).strip())
            if canonical != comp:
                changed = True
            norm_path.append(canonical)
            parent_key_tuple = tuple(list(parent_key_tuple) + [key])
        if changed:
            b.assigned_path = norm_path


def _folder_name_key(name: str) -> str:
    s = (name or "").strip()
    while s and not s[0].isalnum():
        s = s[1:].lstrip()
    out = []
    prev_space = False
    for ch in s:
        if ch.isspace():
            if not prev_space:
                out.append(" ")
            prev_space = True
            continue
        prev_space = False
        out.append(ch.lower())
    return "".join(out).strip()


def _has_leading_emoji(name: str) -> bool:
    s = (name or "").strip()
    return bool(s and not s[0].isalnum())


def _strip_leading_non_alnum(name: str) -> str:
    s = (name or "").strip()
    while s and not s[0].isalnum():
        s = s[1:].lstrip()
    return s


def _sanity_check_unique_link_counts(input_bookmarks: Iterable, output_bookmarks: Iterable) -> bool:
    input_urls = _counted_unique_urls(input_bookmarks)
    output_urls = _counted_unique_urls(output_bookmarks)
    if input_urls == output_urls:
        log.info(
            "Sanity check passed: %d unique links preserved (redirect-aware, excluding non-200).",
            len(input_urls),
        )
        return True

    missing = sorted(input_urls - output_urls)
    extra = sorted(output_urls - input_urls)
    log.error(
        "Sanity check failed: input/output unique link mismatch (input=%d, output=%d, missing=%d, extra=%d).",
        len(input_urls),
        len(output_urls),
        len(missing),
        len(extra),
    )
    if missing:
        log.error("Missing URLs sample: %s", missing[:5])
    if extra:
        log.error("Unexpected URLs sample: %s", extra[:5])
    return False


def _counted_unique_urls(bookmarks: Iterable) -> set[str]:
    out: set[str] = set()
    for b in bookmarks:
        # Non-200 links are excluded from this parity check by design.
        if b.http_status is not None and b.http_status != 200:
            continue
        out.add(_url_identity(b.final_url or b.url))
    return out


def _url_identity(url: str) -> str:
    norm = normalize_url(url or "")
    if not norm:
        return norm
    # "Close enough" duplicate key: host normalization + path normalization.
    from urllib.parse import parse_qsl, urlencode, urlparse

    p = urlparse(norm)
    host = (p.netloc or "").lower()
    for prefix in ("www.", "m."):
        if host.startswith(prefix):
            host = host[len(prefix):]

    path = p.path or "/"
    while "//" in path:
        path = path.replace("//", "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    for suffix in ("/index.html", "/index.htm", "/index.php"):
        if path.lower().endswith(suffix):
            path = path[: -len(suffix)] or "/"

    query_items = parse_qsl(p.query, keep_blank_values=True)
    query = urlencode(sorted(query_items), doseq=True)
    if query:
        return f"{host}{path}?{query}"
    return f"{host}{path}"


def _dedupe_near_duplicates(bookmarks: List) -> List:
    seen = set()
    out = []
    for b in bookmarks:
        key = _url_identity(b.final_url or b.url)
        if key in seen:
            continue
        seen.add(key)
        out.append(b)
    return out


def _is_strictly_inaccessible(status_code: int | None) -> bool:
    if status_code is None:
        return False
    return status_code in {401, 403, 404, 410, 451}


def _is_broken_for_stats(status_code: int | None) -> bool:
    if status_code is None:
        return False
    return status_code in {403, 404} or (500 <= status_code <= 599)


def _folder_count(bookmarks: Iterable) -> int:
    folders = set()
    for b in bookmarks:
        path = [str(x).strip() for x in (b.assigned_path or []) if str(x).strip()]
        if not path:
            continue
        prefix = []
        for comp in path:
            prefix.append(comp)
            folders.add(tuple(prefix))
    return len(folders)


def _log_run_stats(bookmarks: Iterable, *, exact_dupes: int, near_dupes: int) -> None:
    rows = list(bookmarks)
    broken = sum(1 for b in rows if _is_broken_for_stats(getattr(b, "http_status", None)))
    total_dupes = max(0, int(exact_dupes)) + max(0, int(near_dupes))
    log.info(
        "Stats: URLs=%d Folders=%d Broken URLs=%d Duplicates=%d",
        len(rows),
        _folder_count(rows),
        broken,
        total_dupes,
    )


def _all_bookmarks_have_cached_openai_enrichment(bookmarks: Iterable) -> bool:
    rows = list(bookmarks)
    if not rows:
        return False
    for b in rows:
        path = [str(x).strip() for x in (getattr(b, "assigned_path", None) or []) if str(x).strip()]
        summary = (getattr(b, "summary", None) or "").strip()
        if not path or not summary:
            return False
    return True


def _log_link_progress(bookmarks: Iterable, *, phase: str) -> None:
    rows = list(bookmarks)
    total = len(rows)
    for i, b in enumerate(rows, start=1):
        domain = (getattr(b, "domain", "") or "").strip() or "unknown-domain"
        category = "/".join(getattr(b, "assigned_path", None) or getattr(b, "folder_path", None) or ["Uncategorized"])
        log.info("Link [%d/%d] - %s - %s (phase=%s)", i, total, domain, category, phase)


def _merge_sync_stats(primary: SyncStats | None, secondary: SyncStats | None) -> SyncStats:
    p = primary or SyncStats()
    s = secondary or SyncStats()
    return SyncStats(
        added_links=int(p.added_links) + int(s.added_links),
        removed_links=int(p.removed_links) + int(s.removed_links),
        moved_links=int(p.moved_links) + int(s.moved_links),
        tagged_links=int(p.tagged_links) + int(s.tagged_links),
        removed_tag_refs=int(p.removed_tag_refs) + int(s.removed_tag_refs),
        removed_folders=int(p.removed_folders) + int(s.removed_folders),
        touched_links=int(p.touched_links) + int(s.touched_links),
        icon_links=int(p.icon_links) + int(s.icon_links),
        icon_errors=int(p.icon_errors) + int(s.icon_errors),
        deduped_bookmark_rows=int(p.deduped_bookmark_rows) + int(s.deduped_bookmark_rows),
        deduped_favicon_rows=int(p.deduped_favicon_rows) + int(s.deduped_favicon_rows),
    )


def _log_firefox_sync_stats(label: str, sync: SyncStats) -> None:
    log.info(
        "Applied to Firefox DB (%s): touched=%d added=%d removed=%d moved=%d tagged=%d removed_tag_refs=%d removed_folders=%d icons=%d icon_errors=%d deduped_bookmarks=%d deduped_favicons=%d",
        label,
        sync.touched_links,
        sync.added_links,
        sync.removed_links,
        sync.moved_links,
        sync.tagged_links,
        sync.removed_tag_refs,
        sync.removed_folders,
        sync.icon_links,
        sync.icon_errors,
        sync.deduped_bookmark_rows,
        sync.deduped_favicon_rows,
    )


def _apply_cache_entry(b, c: CacheEntry) -> None:
    b.http_status = c.status_code
    b.final_url = c.final_url
    b.page_title = c.page_title
    b.page_description = c.page_description
    b.content_snippet = c.content_snippet
    b.page_html = c.html
    if c.icon_url:
        b.meta["icon_uri"] = c.icon_url
    if c.summary:
        b.summary = c.summary
    if c.tags:
        b.tags = c.tags
    if c.categories:
        b.assigned_path = c.categories
    if c.visited_at:
        b.meta["visited_at"] = c.visited_at
    if c.final_url:
        b.url = normalize_url(c.final_url)
        b.domain = domain_of(b.url)
        b.lang = guess_lang(b.url, b.title)


def _cache_entry_from_bookmark(b) -> CacheEntry:
    return CacheEntry(
        cache_key=_url_identity(b.final_url or b.url),
        url=normalize_url(b.url),
        final_url=normalize_url(b.final_url) if b.final_url else None,
        title=b.assigned_title or b.title,
        tags=b.tags or [],
        categories=b.assigned_path or [],
        status_code=b.http_status,
        visited_at=b.meta.get("visited_at"),
        summary=(b.summary or "")[:4000] or None,
        html=b.page_html,
        page_title=b.page_title,
        page_description=b.page_description,
        content_snippet=b.content_snippet,
        icon_url=b.meta.get("icon_uri"),
    )


def _cache_entries_for_bookmark(b, *, original_url: str | None = None) -> List[CacheEntry]:
    base = _cache_entry_from_bookmark(b)
    out = [base]
    if original_url:
        orig_key = _url_identity(original_url)
        if orig_key and orig_key != base.cache_key:
            out.append(
                CacheEntry(
                    cache_key=orig_key,
                    url=normalize_url(original_url),
                    final_url=base.final_url,
                    title=base.title,
                    tags=base.tags,
                    categories=base.categories,
                    status_code=base.status_code,
                    visited_at=base.visited_at,
                    summary=base.summary,
                    html=base.html,
                page_title=base.page_title,
                page_description=base.page_description,
                content_snippet=base.content_snippet,
                icon_url=base.icon_url,
            )
        )
    return out


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
