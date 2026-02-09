# borgmarks 0.5.0

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

### 2. Login and pull image

```bash
podman login registry.nakarmamana.ch
podman pull registry.nakarmamana.ch/alfanick/bookmarks-sync:0.5.0
```

### 3. Prepare inputs

- Export Safari bookmarks from iOS/iPadOS to `Bookmarks.html`.
- Find Firefox profile path (directory containing `places.sqlite`).
- Close Firefox before running.

### 4. Run (safe mode: does **not** edit `places.sqlite`)

```bash
export OPENAI_API_KEY="sk-..."
mkdir -p tmp

podman run --rm -it \
  -e OPENAI_API_KEY \
  -e BORG_OPENAI_JOBS=4 \
  -v "$PWD/Bookmarks.html:/in/Bookmarks.html:Z" \
  -v "$HOME/.mozilla/firefox/abcd.default-release:/firefox:Z" \
  -v "$PWD/tmp:/tmp:Z" \
  registry.nakarmamana.ch/alfanick/bookmarks-sync:0.5.0 organize \
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
  registry.nakarmamana.ch/alfanick/bookmarks-sync:0.5.0 organize \
    --ios-html /in/Bookmarks.html \
    --firefox-profile /firefox \
    --skip-cache \
    --apply-firefox
```

`places.sqlite` backups are always created at begin/end in `/tmp`:
- `/tmp/borgmarks-places-begin-*.sqlite`
- `/tmp/borgmarks-places-end-*.sqlite`

## What It Does

- Merges iOS Safari export + Firefox bookmarks as input.
- Deduplicates URLs (exact + near duplicates).
- Follows redirects and preserves link parity (excluding non-200 for sanity check).
- Classifies into folders + tags with OpenAI (conservative reclassification).
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
- `--apply-firefox`: write to `places.sqlite` bookmarks/tags/folders.
- `--backup-firefox`: extra profile backup in profile directory.
- `--log-level DEBUG`: verbose logs.

Env examples:
- `BORG_OPENAI_TIMEOUT_S=900`
- `BORG_OPENAI_RECLASSIFY=1`
- `BORG_OPENAI_AGENT_BROWSER=1` (optional Agent/browser tool usage)
- `BORG_OPENAI_REASONING_EFFORT=high`
