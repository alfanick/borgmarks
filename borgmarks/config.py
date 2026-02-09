from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_bool_first(names: tuple[str, ...], default: bool) -> bool:
    for name in names:
        v = os.getenv(name)
        if v is None or v == "":
            continue
        return v.strip().lower() in ("1", "true", "yes", "y", "on")
    return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v is None else v


@dataclass
class Settings:
    # OpenAI
    openai_model: str = "gpt-5.2"
    openai_timeout_s: int = 900
    openai_jobs: int = 2
    openai_max_bookmarks: int = 0  # v0.7.10 default: classify all (set >0 to cap)
    openai_reclassify: bool = True
    openai_max_output_tokens: int = 100_000_000
    openai_agent_browser: bool = False
    openai_reasoning_effort: str = "high"
    openai_folder_emoji_enrich: bool = True
    openai_folder_emoji_max_nodes: int = 800
    openai_tags_enrich: bool = True
    openai_tags_max_global: int = 50
    openai_tags_max_per_link: int = 4
    reclassify_conservative: bool = True
    reclassify_min_folder_gain: int = 2

    # Fetching
    fetch_backend: str = "httpx"  # httpx | curl
    fetch_timeout_s: int = 15
    fetch_jobs: int = 16
    fetch_max_urls: int = 400
    fetch_user_agent: str = "borgmarks/0.7.10 (+https://example.invalid)"
    fetch_max_bytes: int = 350_000

    # Organization rules
    max_depth: int = 4
    leaf_max_links: int = 20
    keep_duplicates: bool = False
    drop_dead: bool = False  # False => move to Archive/Dead links

    # Language tagging
    prefix_non_english: bool = True

    # Metadata
    summary_max_chars: int = 220
    embed_metadata_in_html: bool = True
    write_sidecar_jsonl: bool = True

    # Logging / UX
    log_level: str = "INFO"
    no_color: bool = False

    @staticmethod
    def from_env() -> "Settings":
        s = Settings()
        s.openai_model = _env_str("BORG_OPENAI_MODEL", s.openai_model)
        s.openai_timeout_s = _env_int("BORG_OPENAI_TIMEOUT_S", s.openai_timeout_s)
        s.openai_jobs = _env_int("BORG_OPENAI_JOBS", s.openai_jobs)
        s.openai_max_bookmarks = _env_int("BORG_OPENAI_MAX_BOOKMARKS", s.openai_max_bookmarks)
        s.openai_reclassify = _env_bool("BORG_OPENAI_RECLASSIFY", s.openai_reclassify)
        s.openai_max_output_tokens = _env_int("BORG_OPENAI_MAX_OUTPUT_TOKENS", s.openai_max_output_tokens)
        # Compat: OPENAI_AGENT_BROWSER also supported; BORG_ variant wins when both are set.
        s.openai_agent_browser = _env_bool_first(
            ("BORG_OPENAI_AGENT_BROWSER", "OPENAI_AGENT_BROWSER"),
            s.openai_agent_browser,
        )
        s.openai_reasoning_effort = _env_str("BORG_OPENAI_REASONING_EFFORT", s.openai_reasoning_effort)
        s.openai_folder_emoji_enrich = _env_bool("BORG_OPENAI_FOLDER_EMOJI_ENRICH", s.openai_folder_emoji_enrich)
        s.openai_folder_emoji_max_nodes = _env_int("BORG_OPENAI_FOLDER_EMOJI_MAX_NODES", s.openai_folder_emoji_max_nodes)
        s.openai_tags_enrich = _env_bool("BORG_OPENAI_TAGS_ENRICH", s.openai_tags_enrich)
        s.openai_tags_max_global = _env_int("BORG_OPENAI_TAGS_MAX_GLOBAL", s.openai_tags_max_global)
        s.openai_tags_max_per_link = _env_int("BORG_OPENAI_TAGS_MAX_PER_LINK", s.openai_tags_max_per_link)
        s.reclassify_conservative = _env_bool("BORG_RECLASSIFY_CONSERVATIVE", s.reclassify_conservative)
        s.reclassify_min_folder_gain = _env_int("BORG_RECLASSIFY_MIN_FOLDER_GAIN", s.reclassify_min_folder_gain)

        s.fetch_backend = _env_str("BORG_FETCH_BACKEND", s.fetch_backend)
        s.fetch_timeout_s = _env_int("BORG_FETCH_TIMEOUT_S", s.fetch_timeout_s)
        s.fetch_jobs = _env_int("BORG_FETCH_JOBS", s.fetch_jobs)
        s.fetch_max_urls = _env_int("BORG_FETCH_MAX_URLS", s.fetch_max_urls)
        s.fetch_user_agent = _env_str("BORG_FETCH_UA", s.fetch_user_agent)
        s.fetch_max_bytes = _env_int("BORG_FETCH_MAX_BYTES", s.fetch_max_bytes)

        s.max_depth = _env_int("BORG_MAX_DEPTH", s.max_depth)
        s.leaf_max_links = _env_int("BORG_LEAF_MAX_LINKS", s.leaf_max_links)
        s.keep_duplicates = _env_bool("BORG_KEEP_DUPLICATES", s.keep_duplicates)
        s.drop_dead = _env_bool("BORG_DROP_DEAD", s.drop_dead)

        s.prefix_non_english = _env_bool("BORG_PREFIX_NON_ENGLISH", s.prefix_non_english)

        s.summary_max_chars = _env_int("BORG_SUMMARY_MAX_CHARS", s.summary_max_chars)
        s.embed_metadata_in_html = _env_bool("BORG_EMBED_METADATA_IN_HTML", s.embed_metadata_in_html)
        s.write_sidecar_jsonl = _env_bool("BORG_WRITE_SIDECAR_JSONL", s.write_sidecar_jsonl)

        s.log_level = _env_str("BORG_LOG_LEVEL", s.log_level)
        s.no_color = _env_bool("BORG_NO_COLOR", s.no_color)
        return s

    @staticmethod
    def from_file(path: Path) -> "Settings":
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        s = Settings.from_env()
        for k, v in data.items():
            if hasattr(s, k):
                setattr(s, k, v)
        return s


def load_settings(config_path: Optional[str]) -> Settings:
    if config_path:
        return Settings.from_file(Path(config_path))
    return Settings.from_env()
