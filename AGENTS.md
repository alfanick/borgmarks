# AGENTS.md

This file is for coding agents working in this repository.

## Project Purpose

`borgmarks` syncs and reorganizes bookmarks between:
- Firefox profile (`places.sqlite` + `favicons.sqlite`)
- optional iOS/iPadOS Safari export (`Bookmarks.html`, Netscape format)

It uses OpenAI for classification/reclassification, folder-emoji suggestions, and tag enrichment.

## Repo Map

- `borgmarks/cli.py`: main pipeline and CLI (`python -m borgmarks organize ...`)
- `borgmarks/classify.py`: initial classification + conservative reclassification
- `borgmarks/folder_emoji.py`: recursive folder emoji batching
- `borgmarks/tagging.py`: global tag catalog + per-link tags
- `borgmarks/openai_client.py`: OpenAI SDK integration + robust parsing/fallbacks
- `borgmarks/fetch.py`: URL fetch + metadata/favicons + redirects
- `borgmarks/cache_sqlite.py`: `borg_cache.sqlite` schema/migrations
- `borgmarks/places_db.py`: Firefox bookmarks/tags/folders API wrapper
- `borgmarks/favicons_db.py`: Firefox favicon DB writes/dedup/integrity checks
- `borgmarks/firefox_sync.py`: apply to Firefox DB (links-first, icons second)
- `borgmarks/writer_netscape.py`: output HTML writer
- `tests/`: unit/integration tests
- `Containerfile`: runtime/test container image
- `.gitlab-ci.yml`: pytest + smoke + image build/publish

## Non-Negotiable Behavior

- Keep links unless strictly inaccessible by policy; follow redirects and use final URLs.
- Cache is first-class. If cache already has OpenAI summary+category for all links, skip OpenAI calls.
- `--skip-cache` must recreate cache.
- iOS input is optional; Firefox-only mode must work.
- Merge iOS + Firefox at ingestion with equal priority.
- Deduplicate exact and near-duplicate links.
- No duplicate rows in Firefox bookmarks/favicons DBs after apply.
- Folder dedupe should treat emoji/plain names as equivalent (`👕 Clothing` == `Clothing`).
- Applying to Firefox must be split:
  - Phase 1: links/folders/tags in `places.sqlite`
  - Phase 2: emoji/icon/favicons
  - If phase 2 fails, phase 1 data must remain persisted.
- Always back up Firefox DBs to `/tmp` at run begin/end when present.
- Progress logging must be user-friendly and visible at INFO level.

## Logging Expectations

Keep these progress logs intact:
- `Link [i/x] - domain - Category (phase=classify|reclassify|organize|apply-links)`
- icon pass progress (`phase=apply-icons`)
- clear OpenAI batch start/end/error logs
- end-of-run stats: URLs, Folders, Broken URLs, Duplicates

## OpenAI and Cost Safety

- Use OpenAI SDK (do not remove SDK path).
- Prefer cached data to avoid unnecessary OpenAI calls.
- Reclassification should be conservative (avoid churn between similar categories).
- Keep structured JSON outputs and validation checks.
- Tests must not call real OpenAI APIs; `tests/conftest.py` blocks expensive calls.

## Running and Testing

Use Podman for verification.

Build image:
```bash
podman build -f Containerfile -t borgmarks:test .
```

Run tests in container:
```bash
podman run --rm --entrypoint /bin/sh -v "$PWD":/work -w /work borgmarks:test -lc 'pytest -q'
```

CLI smoke example:
```bash
python -m borgmarks organize --ios-html tests/fixtures/sample_bookmarks.html --firefox-profile out-ci/firefox-profile --no-fetch --no-openai
```

## Release/Version Bump Checklist

When bumping version `X.Y.Z`, update at least:
- `borgmarks/__init__.py`
- `README.md` image tags/examples
- `borgmarks/config.py` (UA/comment strings)
- `borgmarks/fetch.py` version comments

Then run full tests (preferably in Podman) before commit/push.

## Git and Commit Rules

- Never use destructive git commands.
- Do not revert unrelated user changes.
- Commit identity must be:
  - name: `Codex`
  - email: `juskowiak+ai@amadeusz.me`

Set before committing:
```bash
git config user.name "Codex"
git config user.email "juskowiak+ai@amadeusz.me"
```

## File Hygiene

- Do not commit local runtime artifacts (`Bookmarks.html`, `in/`, `out/`, `tmp/`).
- Respect `.gitignore` and keep generated files out of commits.

## CI Notes

CI requires:
- pytest with JUnit XML output (`test-reports/pytest-junit.xml`)
- CLI smoke test against fixture profile directory
- container build
- publish on default branch and semver tags
