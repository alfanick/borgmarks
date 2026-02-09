# borgmarks 0.5.0

AI-assisted bookmark organizer for a Linux + Podman workflow.

**Input**
- An iOS/iPadOS Safari bookmarks export in “Netscape bookmark HTML” format (like `Bookmarks.html`).
- Optional Firefox profile (`places.sqlite`) bookmarks, merged at ingestion time with equal priority.
- Optional live page data fetched from each URL (status, redirect target, title, description, snippet, HTML).
- Optional SQLite cache (if `--firefox-profile` is set: `<profile>/borg_cache.sqlite`, otherwise `out/bookmarks-cache.sqlite`).

**Output**
- `<firefox-profile>/bookmarks.organized.html`
- Optional metadata sidecar: `<firefox-profile>/bookmarks.organized.meta.jsonl`

## What it does (v0.5.0)
- Parses the iOS/Safari HTML export.
- Dedupes URLs (removes common tracking params like `utm_*`, `gclid`, `fbclid`).
- Visits a configurable subset of URLs to:
  - verify they still exist (HTTP status)
  - extract page `<title>`, meta description, and a short snippet
- Uses the OpenAI **Responses API** with **Structured Outputs** to create:
  - a folder path per bookmark (max depth 4)
  - short tags (Firefox supports the `TAGS` attribute)
- Runs classification in two passes: classify, then reclassify using prior classification + summary.
- Reclassification is conservative by default to avoid noisy lateral moves between similar folders.
- Enforces: **≤ 20 links per leaf folder** by auto-splitting big folders.
- Removes exact and near-duplicate links after redirect normalization.
- Adds `[PL]`, `[DE]`, etc. prefixes to non-English titles (**English is unprefixed**).
- Writes a Firefox-importable HTML with a real **Bookmarks Toolbar** folder.

## Quick start (Podman)

### Build
```bash
podman build -t borgmarks:0.5.0 -f Containerfile .
```

### Run
```bash
export OPENAI_API_KEY="sk-..."
podman run --rm -it \
  -e OPENAI_API_KEY \
  -v "$PWD/Bookmarks.html:/in/bookmarks.html:Z" \
  -v "$HOME/.mozilla/firefox/abcd.default-release:/firefox:Z" \
  -v "$PWD/tmp:/tmp:Z" \
  borgmarks:0.5.0 organize \
    --ios-html /in/bookmarks.html \
    --firefox-profile /firefox \
    --backup-firefox
```

Import in Firefox:
- Bookmarks → Manage bookmarks → Import and Backup → Import Bookmarks from HTML…

Optional direct Firefox DB apply (bookmarks/folders/tags only; no history writes):
```bash
podman run --rm -it \
  -e OPENAI_API_KEY \
  -v "$PWD/Bookmarks.html:/in/bookmarks.html:Z" \
  -v "$HOME/.mozilla/firefox/abcd.default-release:/firefox:Z" \
  -v "$PWD/tmp:/tmp:Z" \
  borgmarks:0.5.0 organize \
    --ios-html /in/bookmarks.html \
    --firefox-profile /firefox \
    --apply-firefox
```
Backups of `places.sqlite` are always written to `/tmp` at run begin/end (bind mount `/tmp` to persist them on host).

## Configuration
- Use env vars (best for containers). See `.env.example`.
- Or YAML: `--config sample_config.yaml`

Useful knobs:
- `BORG_OPENAI_MAX_BOOKMARKS`: default `0` (classify all). Set `>0` to cap.
- `BORG_OPENAI_TIMEOUT_S`: default is 900 (15 minutes).
- `BORG_OPENAI_RECLASSIFY=1`: enable pass-2 refinement.
- `BORG_OPENAI_AGENT_BROWSER=1`: optional OpenAI web search/browser tool during classify/emoji enrichment.
- `BORG_OPENAI_REASONING_EFFORT=high`: low|medium|high for OpenAI reasoning mode.
- `BORG_RECLASSIFY_CONSERVATIVE=1`: keep reclassify conservative (default).
- `BORG_RECLASSIFY_MIN_FOLDER_GAIN=2`: minimum folder-size gain required for same-top reclass moves.
- `BORG_FETCH_MAX_URLS`: cap URL fetching.
- `BORG_FETCH_BACKEND=curl`: uses `curl` + `xargs -P` for parallel fetch (best-effort).
- `--skip-cache`: recreate SQLite cache and ignore old cache entries.

## Notes / limitations (v0.5.0)
- By default this emits an importable HTML file; with `--apply-firefox` it also updates Firefox bookmarks/folders/tags in `places.sqlite` (history is not touched).
- Some websites block automated fetches. Those are logged and kept.

## Development
```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pytest -q
```
