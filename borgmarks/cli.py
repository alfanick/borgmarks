
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from . import __version__
from .cache_sqlite import CacheEntry, init_cache, load_entries, upsert_entries
from .classify import classify_bookmarks
from .config import load_settings
from .domain_lang import domain_of, guess_lang
from .fetch import fetch_many
from .log import LogConfig, get_logger, setup_logging
from .parse_firefox_places import parse_firefox_places
from .parse_netscape import parse_bookmarks_html
from .split import enforce_leaf_limits
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
    org.add_argument("--ios-html", required=True, help="Input iOS/Safari bookmarks HTML export (Netscape format).")
    org.add_argument("--firefox-profile", required=False, help="Firefox profile dir (optional, merged bookmark source + backup).")
    org.add_argument("--out", required=True, help="Output HTML path for Firefox import.")
    org.add_argument("--state-dir", required=False, help="State dir for sidecars (default: alongside --out).")
    org.add_argument("--log-level", default=None, help="DEBUG/INFO/WARN/ERROR (overrides env/config).")
    org.add_argument("--no-color", action="store_true", help="Disable colored logging.")
    org.add_argument("--no-fetch", action="store_true", help="Skip website fetching (faster, less accurate).")
    org.add_argument("--no-openai", action="store_true", help="Skip OpenAI classification (fallback bucketing).")
    org.add_argument("--skip-cache", action="store_true", help="Recreate SQLite cache and skip reading existing cache data.")
    org.add_argument("--dry-run", action="store_true", help="Run pipeline but do not write output file.")
    org.add_argument("--backup-firefox", action="store_true", help="If firefox-profile is given, back up places.sqlite to state-dir.")

    args = p.parse_args(argv)
    cfg = load_settings(args.config)
    if args.log_level:
        cfg.log_level = args.log_level
    if args.no_color:
        cfg.no_color = True
    setup_logging(LogConfig(level=cfg.log_level, no_color=cfg.no_color))

    if args.cmd == "organize":
        return _cmd_organize(args, cfg)
    return 2


def _cmd_organize(args, cfg) -> int:
    t0 = time.time()
    ios_html = Path(args.ios_html)
    if not ios_html.exists():
        log.error("Input file not found: %s", ios_html)
        return 2

    out_path = Path(args.out)
    state_dir = Path(args.state_dir) if args.state_dir else out_path.parent
    state_dir.mkdir(parents=True, exist_ok=True)
    cache_db = state_dir / "bookmarks-cache.sqlite"
    init_cache(cache_db, recreate=args.skip_cache)

    if args.firefox_profile and args.backup_firefox:
        _backup_firefox_profile(Path(args.firefox_profile), state_dir)

    try:
        ios_bookmarks, _root_title = parse_bookmarks_html(ios_html)
    except Exception as e:
        log.error("Failed to parse bookmarks HTML: %s", e)
        return 2
    log.info("Parsed %d bookmarks from iOS export: %s", len(ios_bookmarks), ios_html)

    firefox_bookmarks = []
    if args.firefox_profile:
        try:
            firefox_bookmarks = parse_firefox_places(Path(args.firefox_profile))
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
    dupes = 0
    for b in bookmarks:
        b.url = normalize_url(b.url)
        b.domain = domain_of(b.url)
        b.lang = guess_lang(b.url, b.title)

        if b.url in seen and not cfg.keep_duplicates:
            dupes += 1
            continue
        seen.add(b.url)
        deduped.append(b)
    bookmarks = deduped
    if dupes:
        log.info("Deduped %d duplicates (set BORG_KEEP_DUPLICATES=1 to keep).", dupes)

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

    # OpenAI categorization
    if args.no_openai:
        log.warning("Skipping OpenAI classification (--no-openai). Using fallback bucketing.")
        _fallback_assign(bookmarks)
    else:
        if not (os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY")):
            log.error("OPENAI_API_KEY not set. Set it or use --no-openai.")
            return 2
        classify_bookmarks(bookmarks, cfg)

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

    # Leaf folder cap
    enforce_leaf_limits(bookmarks, leaf_max_links=cfg.leaf_max_links, max_depth=cfg.max_depth)

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
            return 2

    # Store current bookmark state in cache (including categories/tags and summaries).
    upsert_entries(cache_db, [_cache_entry_from_bookmark(b) for b in bookmarks])

    # End-of-run guard: the pipeline must preserve unique links, except non-200.
    if not _sanity_check_unique_link_counts(sanity_input, bookmarks):
        return 2

    log.info("Done in %d ms.", int((time.time() - t0) * 1000))
    return 0


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


def _fallback_assign(bookmarks) -> None:
    for b in bookmarks:
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


def _assign_sequential_ids(bookmarks: Iterable) -> None:
    for i, b in enumerate(bookmarks):
        b.id = f"b{i + 1}"


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


def _apply_cache_entry(b, c: CacheEntry) -> None:
    b.http_status = c.status_code
    b.final_url = c.final_url
    b.page_title = c.page_title
    b.page_description = c.page_description
    b.content_snippet = c.content_snippet
    b.page_html = c.html
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
                )
            )
    return out


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
