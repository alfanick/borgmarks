from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import List, Optional

from pydantic import BaseModel, Field

from .log import get_logger

log = get_logger(__name__)

try:
    from openai import OpenAI
    _HAS_OPENAI = True
except Exception:
    OpenAI = None  # type: ignore
    _HAS_OPENAI = False


class Assignment(BaseModel):
    id: str
    path: List[str] = Field(..., description="Folder path components, max depth 4.")
    title: Optional[str] = Field(None, description="Optional rewritten title (concise).")
    tags: List[str] = Field(default_factory=list, description="Short tags, lowercase, no spaces.")


class AssignmentBatch(BaseModel):
    assignments: List[Assignment]


@dataclass
class OpenAIResult:
    parsed: AssignmentBatch
    ms: int


def ensure_openai_available() -> None:
    if not _HAS_OPENAI:
        raise RuntimeError("openai python package not installed. Use container or pip install -r requirements.txt")


def classify_batch(
    *,
    model: str,
    timeout_s: int,
    max_output_tokens: int,
    system_prompt: str,
    user_payload: str,
    phase_label: str,
    batch_label: str,
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
    try:
        resp = client.responses.parse(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
            text_format=AssignmentBatch,
            max_output_tokens=max_output_tokens,
        )
    except Exception as e:
        log.warning(
            "OpenAI parse() failed (%s %s): %s. Retrying without max_output_tokens.",
            phase_label,
            batch_label,
            e,
        )
        try:
            # Compatibility fallback for SDK/pydantic combos that fail on max_output_tokens/parse internals.
            resp = client.responses.parse(
                model=model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_payload},
                ],
                text_format=AssignmentBatch,
            )
        except Exception as e2:
            log.warning(
                "OpenAI parse() retry failed (%s %s): %s. Falling back to responses.create + manual JSON parse.",
                phase_label,
                batch_label,
                e2,
            )
            resp = _create_raw_response(
                client=client,
                model=model,
                system_prompt=system_prompt,
                user_payload=user_payload,
                max_output_tokens=max_output_tokens,
            )
    ms = int((time.time() - t0) * 1000)
    parsed = getattr(resp, "output_parsed", None)
    if parsed is None or not isinstance(parsed, AssignmentBatch):
        log.warning(
            "OpenAI output_parsed missing/invalid (%s %s). Falling back to raw JSON parsing.",
            phase_label,
            batch_label,
        )
        raw = getattr(resp, "output_text", "") or ""
        parsed = _parse_assignment_batch_from_text(raw)
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


def _create_raw_response(
    *,
    client,
    model: str,
    system_prompt: str,
    user_payload: str,
    max_output_tokens: int,
):
    try:
        return client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
            max_output_tokens=max_output_tokens,
        )
    except Exception:
        return client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
        )
