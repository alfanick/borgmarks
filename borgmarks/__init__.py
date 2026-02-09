"""borgmarks: AI-assisted bookmark organizer for Firefox (and iOS/Safari exports)."""

from pathlib import Path


def _read_version() -> str:
    p = Path(__file__).resolve().parents[1] / "VERSION"
    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        # Safe fallback for unusual packaging/runtime contexts.
        return "0.8.0"


__version__ = _read_version()
