from __future__ import annotations

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
    except TypeError:
        # Compatibility fallback for older SDKs that don't expose max_output_tokens on parse().
        resp = client.responses.parse(
            model=model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
            text_format=AssignmentBatch,
        )
    ms = int((time.time() - t0) * 1000)
    parsed = resp.output_parsed  # type: ignore[attr-defined]
    if parsed is None or not isinstance(parsed, AssignmentBatch):
        raise ValueError(f"OpenAI parsed response missing/invalid for {phase_label} {batch_label}")
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
