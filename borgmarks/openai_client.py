from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, List, Optional

from pydantic import BaseModel, Field

from .log import get_logger

log = get_logger(__name__)

try:
    from openai import OpenAI
    _HAS_OPENAI = True
except Exception:
    OpenAI = None  # type: ignore
    _HAS_OPENAI = False

try:
    import openai._compat as _openai_compat  # type: ignore
except Exception:
    _openai_compat = None  # type: ignore

try:
    import openai._base_client as _openai_base_client  # type: ignore
except Exception:
    _openai_base_client = None  # type: ignore

try:
    import openai._utils._transform as _openai_utils_transform  # type: ignore
except Exception:
    _openai_utils_transform = None  # type: ignore

try:
    import openai._utils._json as _openai_utils_json  # type: ignore
except Exception:
    _openai_utils_json = None  # type: ignore

try:
    from rich.console import Console
    from rich.json import JSON as RichJSON
    _HAS_RICH = True
except Exception:
    Console = None  # type: ignore
    RichJSON = None  # type: ignore
    _HAS_RICH = False


class Assignment(BaseModel):
    id: str
    path: List[str] = Field(..., description="Folder path components, max depth 4.")
    title: Optional[str] = Field(None, description="Optional rewritten title (concise).")
    tags: List[str] = Field(default_factory=list, description="Short tags, lowercase, one word or underscore compound.")


class AssignmentBatch(BaseModel):
    assignments: List[Assignment]


class FolderEmojiSuggestion(BaseModel):
    path: List[str] = Field(..., description="Full folder path components.")
    emoji: Optional[str] = Field(None, description="One emoji for this folder path, or null/empty to skip.")


class FolderEmojiBatch(BaseModel):
    suggestions: List[FolderEmojiSuggestion]


class TagAssignment(BaseModel):
    id: str
    tags: List[str] = Field(default_factory=list, description="1-4 tags per bookmark.")


class TagBatch(BaseModel):
    tag_catalog: List[str] = Field(default_factory=list, description="Global tag catalog, max 50 tags.")
    assignments: List[TagAssignment] = Field(default_factory=list)


@dataclass
class OpenAIResult:
    parsed: AssignmentBatch
    ms: int


@dataclass
class OpenAIFolderEmojiResult:
    parsed: FolderEmojiBatch
    ms: int


@dataclass
class OpenAITagResult:
    parsed: TagBatch
    ms: int


def ensure_openai_available() -> None:
    if not _HAS_OPENAI:
        raise RuntimeError("openai python package not installed. Use container or pip install -r requirements.txt")
    _patch_openai_model_dump_by_alias()


def classify_batch(
    *,
    model: str,
    timeout_s: int,
    max_output_tokens: int,
    system_prompt: str,
    user_payload: str,
    phase_label: str,
    batch_label: str,
    use_browser_tool: bool = False,
    reasoning_effort: str = "high",
) -> OpenAIResult:
    ensure_openai_available()
    t0 = time.time()
    client = OpenAI(timeout=timeout_s)
    log.info(
        "OpenAI request start (%s %s): model=%s timeout_s=%d max_output_tokens=%d",
        phase_label,
        batch_label,
        model,
        timeout_s,
        max_output_tokens,
    )
    resp = None
    raw_json_payload: dict[str, Any] | None = None
    request_extra = _request_extras(use_browser_tool=use_browser_tool, reasoning_effort=reasoning_effort)
    try:
        resp = client.responses.parse(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
            text_format=AssignmentBatch,
            max_output_tokens=max_output_tokens,
            **request_extra,
        )
    except Exception as e:
        log.warning(
            "OpenAI parse() failed (%s %s): %s. Retrying without max_output_tokens.",
            phase_label,
            batch_label,
            e,
        )
        if request_extra:
            # Retry once without optional agent/browser features for compatibility.
            request_extra = {}
        try:
            # Compatibility fallback for SDK/pydantic combos that fail on max_output_tokens/parse internals.
            resp = client.responses.parse(
                model=model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_payload},
                ],
                text_format=AssignmentBatch,
                **request_extra,
            )
        except Exception as e2:
            log.warning(
                "OpenAI parse() retry failed (%s %s): %s. Falling back to responses.create + manual JSON parse.",
                phase_label,
                batch_label,
                e2,
            )
            raw_json_payload = _create_raw_response_json(
                client=client,
                model=model,
                system_prompt=system_prompt,
                user_payload=user_payload,
                max_output_tokens=max_output_tokens,
                phase_label=phase_label,
                batch_label=batch_label,
                request_extra=request_extra,
            )
    ms = int((time.time() - t0) * 1000)
    parsed = getattr(resp, "output_parsed", None) if resp is not None else None

    if raw_json_payload is not None:
        parsed = _parse_assignment_batch_from_response_json(
            raw_json_payload,
            phase_label=phase_label,
            batch_label=batch_label,
        )
    elif parsed is None or not isinstance(parsed, AssignmentBatch):
        log.warning(
            "OpenAI output_parsed missing/invalid (%s %s). Falling back to raw response JSON parsing.",
            phase_label,
            batch_label,
        )
        raw_json_payload = _create_raw_response_json(
            client=client,
            model=model,
            system_prompt=system_prompt,
            user_payload=user_payload,
            max_output_tokens=max_output_tokens,
            phase_label=phase_label,
            batch_label=batch_label,
            request_extra=request_extra,
        )
        parsed = _parse_assignment_batch_from_response_json(
            raw_json_payload,
            phase_label=phase_label,
            batch_label=batch_label,
        )
    if not isinstance(parsed.assignments, list):
        raise ValueError(f"OpenAI assignments must be a list for {phase_label} {batch_label}")
    log.info(
        "OpenAI request done (%s %s): assignments=%d elapsed_ms=%d",
        phase_label,
        batch_label,
        len(parsed.assignments),
        ms,
    )
    return OpenAIResult(parsed=parsed, ms=ms)


def suggest_folder_emojis(
    *,
    model: str,
    timeout_s: int,
    max_output_tokens: int,
    system_prompt: str,
    user_payload: str,
    batch_label: str = "all",
    use_browser_tool: bool = False,
    reasoning_effort: str = "high",
) -> OpenAIFolderEmojiResult:
    ensure_openai_available()
    t0 = time.time()
    client = OpenAI(timeout=timeout_s)
    phase_label = "folder-emoji"
    log.info(
        "OpenAI request start (%s %s): model=%s timeout_s=%d max_output_tokens=%d",
        phase_label,
        batch_label,
        model,
        timeout_s,
        max_output_tokens,
    )
    resp = None
    raw_json_payload: dict[str, Any] | None = None
    request_extra = _request_extras(use_browser_tool=use_browser_tool, reasoning_effort=reasoning_effort)
    try:
        resp = client.responses.parse(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
            text_format=FolderEmojiBatch,
            max_output_tokens=max_output_tokens,
            **request_extra,
        )
    except Exception as e:
        log.warning(
            "OpenAI parse() failed (%s %s): %s. Retrying without max_output_tokens.",
            phase_label,
            batch_label,
            e,
        )
        if request_extra:
            request_extra = {}
        try:
            resp = client.responses.parse(
                model=model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_payload},
                ],
                text_format=FolderEmojiBatch,
                **request_extra,
            )
        except Exception as e2:
            log.warning(
                "OpenAI parse() retry failed (%s %s): %s. Falling back to responses.create + manual JSON parse.",
                phase_label,
                batch_label,
                e2,
            )
            raw_json_payload = _create_raw_response_json(
                client=client,
                model=model,
                system_prompt=system_prompt,
                user_payload=user_payload,
                max_output_tokens=max_output_tokens,
                phase_label=phase_label,
                batch_label=batch_label,
                request_extra=request_extra,
            )
    ms = int((time.time() - t0) * 1000)
    parsed = getattr(resp, "output_parsed", None) if resp is not None else None

    if raw_json_payload is not None:
        parsed = _parse_folder_emoji_batch_from_response_json(
            raw_json_payload,
            phase_label=phase_label,
            batch_label=batch_label,
        )
    elif parsed is None or not isinstance(parsed, FolderEmojiBatch):
        log.warning(
            "OpenAI output_parsed missing/invalid (%s %s). Falling back to raw response JSON parsing.",
            phase_label,
            batch_label,
        )
        raw_json_payload = _create_raw_response_json(
            client=client,
            model=model,
            system_prompt=system_prompt,
            user_payload=user_payload,
            max_output_tokens=max_output_tokens,
            phase_label=phase_label,
            batch_label=batch_label,
            request_extra=request_extra,
        )
        parsed = _parse_folder_emoji_batch_from_response_json(
            raw_json_payload,
            phase_label=phase_label,
            batch_label=batch_label,
        )
    if not isinstance(parsed.suggestions, list):
        raise ValueError(f"OpenAI folder emoji suggestions must be a list for {phase_label} {batch_label}")
    log.info(
        "OpenAI request done (%s %s): suggestions=%d elapsed_ms=%d",
        phase_label,
        batch_label,
        len(parsed.suggestions),
        ms,
    )
    return OpenAIFolderEmojiResult(parsed=parsed, ms=ms)


def suggest_tags_for_tree(
    *,
    model: str,
    timeout_s: int,
    max_output_tokens: int,
    system_prompt: str,
    user_payload: str,
    batch_label: str = "all",
    use_browser_tool: bool = False,
    reasoning_effort: str = "high",
) -> OpenAITagResult:
    ensure_openai_available()
    t0 = time.time()
    client = OpenAI(timeout=timeout_s)
    phase_label = "tagger"
    log.info(
        "OpenAI request start (%s %s): model=%s timeout_s=%d max_output_tokens=%d",
        phase_label,
        batch_label,
        model,
        timeout_s,
        max_output_tokens,
    )
    resp = None
    raw_json_payload: dict[str, Any] | None = None
    request_extra = _request_extras(use_browser_tool=use_browser_tool, reasoning_effort=reasoning_effort)
    try:
        resp = client.responses.parse(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
            text_format=TagBatch,
            max_output_tokens=max_output_tokens,
            **request_extra,
        )
    except Exception as e:
        log.warning(
            "OpenAI parse() failed (%s %s): %s. Retrying without max_output_tokens.",
            phase_label,
            batch_label,
            e,
        )
        if request_extra:
            request_extra = {}
        try:
            resp = client.responses.parse(
                model=model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_payload},
                ],
                text_format=TagBatch,
                **request_extra,
            )
        except Exception as e2:
            log.warning(
                "OpenAI parse() retry failed (%s %s): %s. Falling back to responses.create + manual JSON parse.",
                phase_label,
                batch_label,
                e2,
            )
            raw_json_payload = _create_raw_response_json(
                client=client,
                model=model,
                system_prompt=system_prompt,
                user_payload=user_payload,
                max_output_tokens=max_output_tokens,
                phase_label=phase_label,
                batch_label=batch_label,
                request_extra=request_extra,
            )
    ms = int((time.time() - t0) * 1000)
    parsed = getattr(resp, "output_parsed", None) if resp is not None else None

    if raw_json_payload is not None:
        parsed = _parse_tag_batch_from_response_json(
            raw_json_payload,
            phase_label=phase_label,
            batch_label=batch_label,
        )
    elif parsed is None or not isinstance(parsed, TagBatch):
        log.warning(
            "OpenAI output_parsed missing/invalid (%s %s). Falling back to raw response JSON parsing.",
            phase_label,
            batch_label,
        )
        raw_json_payload = _create_raw_response_json(
            client=client,
            model=model,
            system_prompt=system_prompt,
            user_payload=user_payload,
            max_output_tokens=max_output_tokens,
            phase_label=phase_label,
            batch_label=batch_label,
            request_extra=request_extra,
        )
        parsed = _parse_tag_batch_from_response_json(
            raw_json_payload,
            phase_label=phase_label,
            batch_label=batch_label,
        )
    if not isinstance(parsed.assignments, list):
        raise ValueError(f"OpenAI tag assignments must be a list for {phase_label} {batch_label}")
    log.info(
        "OpenAI request done (%s %s): tags=%d assignments=%d elapsed_ms=%d",
        phase_label,
        batch_label,
        len(parsed.tag_catalog),
        len(parsed.assignments),
        ms,
    )
    return OpenAITagResult(parsed=parsed, ms=ms)


def _parse_assignment_batch_from_text(raw_text: str) -> AssignmentBatch:
    raw = (raw_text or "").strip()
    if not raw:
        raise ValueError("OpenAI returned empty text; cannot parse AssignmentBatch JSON")

    # Common pattern: fenced JSON output.
    m = re.search(r"```json\s*(.*?)\s*```", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        raw = m.group(1).strip()

    try:
        return AssignmentBatch.model_validate_json(raw)
    except Exception:
        # Best-effort extraction of the first JSON object in the text.
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            return AssignmentBatch.model_validate_json(raw[start : end + 1])
        raise


def _parse_folder_emoji_batch_from_text(raw_text: str) -> FolderEmojiBatch:
    raw = (raw_text or "").strip()
    if not raw:
        raise ValueError("OpenAI returned empty text; cannot parse FolderEmojiBatch JSON")

    m = re.search(r"```json\s*(.*?)\s*```", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        raw = m.group(1).strip()

    try:
        return FolderEmojiBatch.model_validate_json(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            return FolderEmojiBatch.model_validate_json(raw[start : end + 1])
        raise


def _parse_tag_batch_from_text(raw_text: str) -> TagBatch:
    raw = (raw_text or "").strip()
    if not raw:
        raise ValueError("OpenAI returned empty text; cannot parse TagBatch JSON")

    m = re.search(r"```json\s*(.*?)\s*```", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        raw = m.group(1).strip()

    try:
        return TagBatch.model_validate_json(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            return TagBatch.model_validate_json(raw[start : end + 1])
        raise


def _create_raw_response_json(
    *,
    client,
    model: str,
    system_prompt: str,
    user_payload: str,
    max_output_tokens: int,
    phase_label: str,
    batch_label: str,
    request_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_input = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_payload},
    ]
    try:
        raw_resp = client.responses.with_raw_response.create(
            model=model,
            input=request_input,
            max_output_tokens=max_output_tokens,
            **(request_extra or {}),
        )
    except Exception as e:
        log.warning(
            "OpenAI raw create() with max_output_tokens failed (%s %s): %s. Retrying without max_output_tokens.",
            phase_label,
            batch_label,
            e,
        )
        raw_resp = client.responses.with_raw_response.create(
            model=model,
            input=request_input,
            **(request_extra or {}),
        )
    payload = raw_resp.json()
    if not isinstance(payload, dict):
        raise ValueError(f"OpenAI raw response JSON is not an object for {phase_label} {batch_label}")
    return payload


def _request_extras(*, use_browser_tool: bool, reasoning_effort: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if use_browser_tool:
        out["tools"] = [{"type": "web_search_preview"}]
    eff = (reasoning_effort or "").strip().lower()
    if eff in {"low", "medium", "high"}:
        out["reasoning"] = {"effort": eff}
    return out


def _parse_assignment_batch_from_response_json(
    payload: dict[str, Any],
    *,
    phase_label: str,
    batch_label: str,
) -> AssignmentBatch:
    raw_text = _extract_output_text(payload)
    if not raw_text:
        _debug_log_response_json(
            title=f"OpenAI response JSON missing output text ({phase_label} {batch_label})",
            payload=payload,
        )
        raise ValueError(f"OpenAI response JSON has no parseable output text for {phase_label} {batch_label}")
    try:
        return _parse_assignment_batch_from_text(raw_text)
    except Exception as e:
        _debug_log_response_json(
            title=f"OpenAI response JSON parse failure ({phase_label} {batch_label})",
            payload=payload,
        )
        raise ValueError(f"Failed to parse AssignmentBatch JSON for {phase_label} {batch_label}: {e}") from e


def _parse_folder_emoji_batch_from_response_json(
    payload: dict[str, Any],
    *,
    phase_label: str,
    batch_label: str,
) -> FolderEmojiBatch:
    raw_text = _extract_output_text(payload)
    if not raw_text:
        _debug_log_response_json(
            title=f"OpenAI response JSON missing output text ({phase_label} {batch_label})",
            payload=payload,
        )
        raise ValueError(f"OpenAI response JSON has no parseable output text for {phase_label} {batch_label}")
    try:
        return _parse_folder_emoji_batch_from_text(raw_text)
    except Exception as e:
        _debug_log_response_json(
            title=f"OpenAI response JSON parse failure ({phase_label} {batch_label})",
            payload=payload,
        )
        raise ValueError(f"Failed to parse FolderEmojiBatch JSON for {phase_label} {batch_label}: {e}") from e


def _parse_tag_batch_from_response_json(
    payload: dict[str, Any],
    *,
    phase_label: str,
    batch_label: str,
) -> TagBatch:
    raw_text = _extract_output_text(payload)
    if not raw_text:
        _debug_log_response_json(
            title=f"OpenAI response JSON missing output text ({phase_label} {batch_label})",
            payload=payload,
        )
        raise ValueError(f"OpenAI response JSON has no parseable output text for {phase_label} {batch_label}")
    try:
        return _parse_tag_batch_from_text(raw_text)
    except Exception as e:
        _debug_log_response_json(
            title=f"OpenAI response JSON parse failure ({phase_label} {batch_label})",
            payload=payload,
        )
        raise ValueError(f"Failed to parse TagBatch JSON for {phase_label} {batch_label}: {e}") from e


def _extract_output_text(payload: dict[str, Any]) -> str:
    direct = payload.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct

    chunks: List[str] = []
    output = payload.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for part in content:
                if not isinstance(part, dict):
                    continue
                ptype = str(part.get("type", "")).lower()
                if ptype in {"output_text", "text"}:
                    text = part.get("text")
                    if isinstance(text, str):
                        chunks.append(text)
                    elif isinstance(text, dict):
                        value = text.get("value")
                        if isinstance(value, str):
                            chunks.append(value)
    return "\n".join(x for x in chunks if x).strip()


def _debug_log_response_json(*, title: str, payload: dict[str, Any]) -> None:
    pretty = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if _HAS_RICH and log.isEnabledFor(logging.DEBUG):
        try:
            console = Console(stderr=True)
            console.print(f"[bold yellow]{title}[/bold yellow]")
            console.print(RichJSON(pretty))
            return
        except Exception:
            pass
    log.debug("%s\n%s", title, pretty)


_OPENAI_COMPAT_PATCHED = False


def _patch_openai_model_dump_by_alias() -> None:
    global _OPENAI_COMPAT_PATCHED
    if _OPENAI_COMPAT_PATCHED:
        return
    if _openai_compat is None or not hasattr(_openai_compat, "model_dump"):
        return

    orig = _openai_compat.model_dump

    def _patched_model_dump(
        model,
        *,
        exclude=None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        warnings: bool = True,
        mode: str = "python",
        by_alias: bool | None = None,
    ):
        # Work around openai-sdk+pydantic(2.8.x) incompatibility where
        # by_alias=None can trigger:
        # "'NoneType' object cannot be converted to 'PyBool'".
        if by_alias is None:
            by_alias = False
        return orig(
            model,
            exclude=exclude,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            warnings=warnings,
            mode=mode,
            by_alias=by_alias,
        )

    _openai_compat.model_dump = _patched_model_dump
    if _openai_base_client is not None and hasattr(_openai_base_client, "model_dump"):
        _openai_base_client.model_dump = _patched_model_dump
    if _openai_utils_transform is not None and hasattr(_openai_utils_transform, "model_dump"):
        _openai_utils_transform.model_dump = _patched_model_dump
    if _openai_utils_json is not None and hasattr(_openai_utils_json, "model_dump"):
        _openai_utils_json.model_dump = _patched_model_dump
    _OPENAI_COMPAT_PATCHED = True
    log.info("Applied OpenAI SDK compatibility patch for pydantic by_alias handling.")
