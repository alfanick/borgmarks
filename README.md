# borgmarks 0.0.5

AI-assisted bookmark organizer for a Linux + Podman workflow.

**Input**
- An iOS/iPadOS Safari bookmarks export in “Netscape bookmark HTML” format (like `Bookmarks.html`).
- Optional live page data fetched from each URL (status, redirect target, title, description, snippet, HTML).
- Optional SQLite cache in `out/bookmarks-cache.sqlite` (or `--state-dir`) for reuse between runs.

**Output**
- A Firefox-importable `bookmarks.organized.html`
- Optional metadata sidecar: `bookmarks.organized.meta.jsonl`

## What it does (v0.0.5)
- Parses the iOS/Safari HTML export.
- Dedupes URLs (removes common tracking params like `utm_*`, `gclid`, `fbclid`).
- Visits a configurable subset of URLs to:
  - verify they still exist (HTTP status)
  - extract page `<title>`, meta description, and a short snippet
- Uses the OpenAI **Responses API** with **Structured Outputs** to create:
  - a folder path per bookmark (max depth 4)
  - short tags (Firefox supports the `TAGS` attribute)
- Runs classification in two passes: classify, then reclassify using prior classification + summary.
- Enforces: **≤ 20 links per leaf folder** by auto-splitting big folders.
- Removes exact and near-duplicate links after redirect normalization.
- Adds `[PL]`, `[DE]`, etc. prefixes to non-English titles (**English is unprefixed**).
- Writes a Firefox-importable HTML with a real **Bookmarks Toolbar** folder.

## Quick start (Podman)

### Build
```bash
podman build -t borgmarks:0.0.5 -f Containerfile .
```

### Run
```bash
export OPENAI_API_KEY="sk-..."
podman run --rm -it \
  -e OPENAI_API_KEY \
  -v "$PWD/Bookmarks.html:/in/bookmarks.html:Z" \
  -v "$HOME/.mozilla/firefox/abcd.default-release:/firefox:Z" \
  -v "$PWD/out:/out:Z" \
  borgmarks:0.0.5 organize \
    --ios-html /in/bookmarks.html \
    --firefox-profile /firefox \
    --backup-firefox \
    --out /out/bookmarks.organized.html
```

Import in Firefox:
- Bookmarks → Manage bookmarks → Import and Backup → Import Bookmarks from HTML…

## Configuration
- Use env vars (best for containers). See `.env.example`.
- Or YAML: `--config sample_config.yaml`

Useful knobs:
- `BORG_OPENAI_MAX_BOOKMARKS`: default `0` (classify all). Set `>0` to cap.
- `BORG_OPENAI_TIMEOUT_S`: default is 900 (15 minutes).
- `BORG_OPENAI_RECLASSIFY=1`: enable pass-2 refinement.
- `BORG_FETCH_MAX_URLS`: cap URL fetching.
- `BORG_FETCH_BACKEND=curl`: uses `curl` + `xargs -P` for parallel fetch (best-effort).
- `--skip-cache`: recreate SQLite cache and ignore old cache entries.

## Notes / limitations (v0.0.5)
- This version does not modify Firefox’s `places.sqlite`. It only emits an importable HTML file.
- Some websites block automated fetches. Those are logged and kept.

## Development
```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pytest -q
```
