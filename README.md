# borgmarks 0.0.2

AI-assisted bookmark organizer for a Linux + Podman workflow.

**Input**
- An iOS/Safari bookmarks export in “Netscape bookmark HTML” format (like `Bookmarks.html`).

**Output**
- A Firefox-importable `bookmarks.organized.html`
- Optional metadata sidecar: `bookmarks.organized.meta.jsonl`

## What it does (v0.0.2)
- Parses the iOS/Safari HTML export.
- Dedupes URLs (removes common tracking params like `utm_*`, `gclid`, `fbclid`).
- Visits a configurable subset of URLs to:
  - verify they still exist (HTTP status)
  - extract page `<title>`, meta description, and a short snippet
- Uses the OpenAI **Responses API** with **Structured Outputs** to create:
  - a folder path per bookmark (max depth 4)
  - short tags (Firefox supports the `TAGS` attribute)
- Enforces: **≤ 20 links per leaf folder** by auto-splitting big folders.
- Adds `[PL]`, `[DE]`, etc. prefixes to non-English titles (**English is unprefixed**).
- Writes a Firefox-importable HTML with a real **Bookmarks Toolbar** folder.

## Quick start (Podman)

### Build
```bash
podman build -t borgmarks:0.0.2 -f Containerfile .
```

### Run
```bash
export OPENAI_API_KEY="sk-..."
podman run --rm -it \
  -e OPENAI_API_KEY \
  -v "$PWD/Bookmarks.html:/in/bookmarks.html:Z" \
  -v "$HOME/.mozilla/firefox/abcd.default-release:/firefox:Z" \
  -v "$PWD/out:/out:Z" \
  borgmarks:0.0.2 organize \
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
- `BORG_OPENAI_MAX_BOOKMARKS`: cap OpenAI classification for the first run.
- `BORG_FETCH_MAX_URLS`: cap URL fetching.
- `BORG_FETCH_BACKEND=curl`: uses `curl` + `xargs -P` for parallel fetch (best-effort).

## Notes / limitations (v0.0.2)
- This version does not modify Firefox’s `places.sqlite`. It only emits an importable HTML file.
- Some websites block automated fetches. Those are logged and kept.

## Development
```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pytest -q
```
