# borgmarks 0.7.3

Organize bookmarks from iOS/iPadOS Safari + Firefox with OpenAI, then either:
- generate an importable Firefox HTML file (safe mode), or
- apply folders/tags/bookmarks directly to Firefox `places.sqlite` (optional).

**Fastest safe path:** run container, generate `bookmarks.organized.html`, import it in Firefox.

## Quick Start (End User)

### 1. Install Podman

Linux (Ubuntu/Debian):
```bash
sudo apt update
sudo apt install -y podman
```

Linux (Fedora):
```bash
sudo dnf install -y podman
```

macOS:
```bash
brew install podman
podman machine init
podman machine start
```

Windows (PowerShell):
```powershell
winget install RedHat.Podman
podman machine init
podman machine start
```

### 2. Login and pull image (GHCR)

```bash
podman login ghcr.io -u YOUR_GITHUB_USERNAME
podman pull ghcr.io/alfanick/borgmarks:v0.7.3
```

### 3. Prepare inputs

- Find Firefox profile path (directory containing `places.sqlite`).
- Close Firefox before running.
- Optional iOS export (`Bookmarks.html`) with explicit steps:
  1. On iPhone/iPad open Safari and open the bookmark you want synced.
  2. Share it to Files (or copy links to Notes), then on Mac import them into Safari bookmarks.
  3. On Mac Safari: `File -> Export -> Bookmarks...` and save as `Bookmarks.html`.
  4. Put `Bookmarks.html` in your current working directory.

### 4. Run (safe mode: does **not** edit `places.sqlite`)

Firefox-only mode (no iOS file, useful for reclassifying new Firefox links):

```bash
export OPENAI_API_KEY="sk-..."
mkdir -p tmp

podman run --rm -it \
  -e OPENAI_API_KEY \
  -e BORG_OPENAI_JOBS=4 \
  -v "$HOME/.mozilla/firefox/abcd.default-release:/firefox:Z" \
  -v "$PWD/tmp:/tmp:Z" \
  ghcr.io/alfanick/borgmarks:v0.7.3 organize \
    --firefox-profile /firefox \
    --skip-cache
```

Merged mode (Firefox + iOS input with equal priority):

```bash
export OPENAI_API_KEY="sk-..."
mkdir -p tmp

podman run --rm -it \
  -e OPENAI_API_KEY \
  -e BORG_OPENAI_JOBS=4 \
  -v "$PWD/Bookmarks.html:/in/Bookmarks.html:Z" \
  -v "$HOME/.mozilla/firefox/abcd.default-release:/firefox:Z" \
  -v "$PWD/tmp:/tmp:Z" \
  ghcr.io/alfanick/borgmarks:v0.7.3 organize \
    --ios-html /in/Bookmarks.html \
    --firefox-profile /firefox \
    --skip-cache
```

Result files are written into Firefox profile:
- `/firefox/bookmarks.organized.html`
- `/firefox/bookmarks.organized.meta.jsonl`
- `/firefox/borg_cache.sqlite`

### 5. Optional: apply directly to Firefox DB

This modifies bookmarks/folders/tags in `places.sqlite` (history is not touched):

```bash
podman run --rm -it \
  -e OPENAI_API_KEY \
  -e BORG_OPENAI_JOBS=4 \
  -v "$PWD/Bookmarks.html:/in/Bookmarks.html:Z" \
  -v "$HOME/.mozilla/firefox/abcd.default-release:/firefox:Z" \
  -v "$PWD/tmp:/tmp:Z" \
  ghcr.io/alfanick/borgmarks:v0.7.3 organize \
    --ios-html /in/Bookmarks.html \
    --firefox-profile /firefox \
    --skip-cache \
    --apply-firefox
```

`places.sqlite` backups are always created at begin/end in `/tmp`:
- `/tmp/borgmarks-places-begin-*.sqlite`
- `/tmp/borgmarks-places-end-*.sqlite`

## What It Does

- Uses Firefox bookmarks as baseline input.
- Optionally merges iOS Safari export with equal priority.
- Deduplicates URLs (exact + near duplicates).
- Follows redirects and preserves link parity (excluding non-200 for sanity check).
- Classifies into folders + tags with OpenAI (conservative reclassification).
- Runs a dedicated tag pass over the whole tree (1-4 tags/link, max 50 global tags).
- Adds/keeps folder emojis and bookmark icons:
  - favicon when available
  - emoji icon fallback when favicon is missing
- Keeps cache in Firefox profile: `borg_cache.sqlite`.

## Stability Rules (Reruns)

- Reuses cache aggressively unless `--skip-cache` is used.
- Avoids moving bookmarks unless model has a strong reason.
- Reuses existing folders whenever possible.
- Keeps folder ordering deterministic and groups similar folders together.
- Sorts leaf links by freshness (newest first).

## Useful Options

- `--skip-cache`: rebuild cache from scratch.
- `--no-fetch`: skip website fetch.
- `--no-openai`: skip OpenAI classification.
- `--no-folder-emoji`: skip folder emoji enrichment for this run.
- `--apply-firefox`: write to `places.sqlite` bookmarks/tags/folders.
- `--backup-firefox`: extra profile backup in profile directory.
- `--log-level DEBUG`: verbose logs.

Env examples:
- `BORG_OPENAI_TIMEOUT_S=900`
- `BORG_OPENAI_RECLASSIFY=1`
- `BORG_OPENAI_AGENT_BROWSER=1` (optional Agent/browser tool usage)
- `BORG_OPENAI_REASONING_EFFORT=high`

## License

This project is licensed under the MIT License. See `LICENSE`.

## Contributions

- Whole coding is done via `gpt-5.3-thinking`.
- New features and bug reports are welcome via Issues.
- Issues are reviewed after maintainer moderation.
- Pull requests are not accepted.
