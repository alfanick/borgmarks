from borgmarks.openai_client import _parse_assignment_batch_from_text


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
