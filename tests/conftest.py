import sys
from pathlib import Path

import pytest

# Allow `import borgmarks` without installing the package.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _block_expensive_openai_calls(monkeypatch):
    """Tests must never trigger real OpenAI requests."""

    def _blocked(*_args, **_kwargs):
        raise AssertionError("OpenAI API call attempted during tests")

    import borgmarks.classify as classify
    import borgmarks.folder_emoji as folder_emoji
    import borgmarks.tagging as tagging

    monkeypatch.setattr(classify, "classify_batch", _blocked)
    monkeypatch.setattr(folder_emoji, "suggest_folder_emojis", _blocked)
    monkeypatch.setattr(tagging, "suggest_tags_for_tree", _blocked)
