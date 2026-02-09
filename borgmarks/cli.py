
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import List

from . import __version__
from .classify import classify_bookmarks
from .config import load_settings
from .domain_lang import domain_of, guess_lang
from .fetch import fetch_many
from .log import LogConfig, get_logger, setup_logging
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
    org.add_argument("--firefox-profile", required=False, help="Firefox profile dir (optional, backup only today).")
    org.add_argument("--out", required=True, help="Output HTML path for Firefox import.")
    org.add_argument("--state-dir", required=False, help="State dir for sidecars (default: alongside --out).")
    org.add_argument("--log-level", default=None, help="DEBUG/INFO/WARN/ERROR (overrides env/config).")
    org.add_argument("--no-color", action="store_true", help="Disable colored logging.")
    org.add_argument("--no-fetch", action="store_true", help="Skip website fetching (faster, less accurate).")
    org.add_argument("--no-openai", action="store_true", help="Skip OpenAI classification (fallback bucketing).")
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

    if args.firefox_profile and args.backup_firefox:
        _backup_firefox_profile(Path(args.firefox_profile), state_dir)

    try:
        bookmarks, _root_title = parse_bookmarks_html(ios_html)
    except Exception as e:
        log.error("Failed to parse bookmarks HTML: %s", e)
        return 2
    log.info("Parsed %d bookmarks from %s", len(bookmarks), ios_html)

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

    # Fetch subset (visit websites)
    if not args.no_fetch:
        urls = [b.url for b in bookmarks[: cfg.fetch_max_urls]]
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
            for b in bookmarks[: cfg.fetch_max_urls]:
                r = results.get(b.url)
                if not r:
                    continue
                b.fetched_ok = r.ok
                b.http_status = r.status
                b.final_url = r.final_url
                b.page_title = r.title
                b.page_description = r.description
                b.content_snippet = r.snippet
                b.meta["fetch_ms"] = str(r.fetch_ms)
        except Exception as e:
            log.warning("Fetch phase failed: %s", e)
    else:
        log.info("Skipping fetch phase (--no-fetch).")

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
            if b.fetched_ok is False or (b.http_status is not None and b.http_status >= 400):
                b.assigned_path = ["Archive", "🪦 Dead links"]
    else:
        before = len(bookmarks)
        bookmarks = [b for b in bookmarks if not (b.fetched_ok is False or (b.http_status is not None and b.http_status >= 400))]
        dropped = before - len(bookmarks)
        if dropped:
            log.warning("Dropped %d dead links (BORG_DROP_DEAD=0 to keep in Archive).", dropped)

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
