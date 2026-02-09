from borgmarks.openai_client import (
    _call_with_backoff,
    _extract_output_text,
    _is_rate_limit_error,
    _parse_assignment_batch_from_text,
    _parse_tag_batch_from_text,
    _retry_delay_seconds,
)


def test_parse_assignment_batch_from_plain_json():
    raw = '{"assignments":[{"id":"b1","path":["Reading","Inbox"],"title":"T","tags":["a"]}]}'
    parsed = _parse_assignment_batch_from_text(raw)
    assert len(parsed.assignments) == 1
    assert parsed.assignments[0].id == "b1"


def test_parse_assignment_batch_from_fenced_json():
    raw = """```json
{"assignments":[{"id":"b2","path":["Computers"],"tags":[]}]}
```"""
    parsed = _parse_assignment_batch_from_text(raw)
    assert len(parsed.assignments) == 1
    assert parsed.assignments[0].id == "b2"


def test_extract_output_text_from_response_json_output_list():
    payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": '{"assignments":[{"id":"b3","path":["Reading"],"tags":[]}]}',
                    }
                ],
            }
        ]
    }
    text = _extract_output_text(payload)
    assert '"id":"b3"' in text


def test_parse_tag_batch_from_plain_json():
    raw = '{"tag_catalog":["news","machine_learning"],"assignments":[{"id":"b1","tags":["news"]}]}'
    parsed = _parse_tag_batch_from_text(raw)
    assert parsed.tag_catalog == ["news", "machine_learning"]
    assert parsed.assignments[0].id == "b1"


class _DummyResponse:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.headers = headers or {}


class _DummyRateLimitError(Exception):
    def __init__(self, message: str = "too many requests", *, retry_after: str | None = None):
        super().__init__(message)
        self.status_code = 429
        headers = {"retry-after": retry_after} if retry_after is not None else {}
        self.response = _DummyResponse(429, headers=headers)


def test_call_with_backoff_retries_rate_limit_then_succeeds(monkeypatch):
    sleeps: list[float] = []

    def _sleep(seconds: float):
        sleeps.append(seconds)

    monkeypatch.setattr("borgmarks.openai_client.time.sleep", _sleep)

    state = {"n": 0}

    def _fn():
        state["n"] += 1
        if state["n"] < 3:
            raise _DummyRateLimitError()
        return "ok"

    out = _call_with_backoff(call=_fn, phase_label="x", batch_label="y", op_label="z")
    assert out == "ok"
    assert sleeps == [1.0, 2.0]
    assert state["n"] == 3


def test_call_with_backoff_gives_up_after_three_attempts(monkeypatch):
    sleeps: list[float] = []

    def _sleep(seconds: float):
        sleeps.append(seconds)

    monkeypatch.setattr("borgmarks.openai_client.time.sleep", _sleep)

    state = {"n": 0}

    def _fn():
        state["n"] += 1
        raise _DummyRateLimitError()

    try:
        _call_with_backoff(call=_fn, phase_label="x", batch_label="y", op_label="z")
        assert False, "expected exception"
    except _DummyRateLimitError:
        pass

    assert state["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_retry_delay_uses_retry_after_header_when_present():
    err = _DummyRateLimitError(retry_after="7")
    delay = _retry_delay_seconds(exc=err, attempt=1)
    assert delay == 7.0
    assert _is_rate_limit_error(err)
