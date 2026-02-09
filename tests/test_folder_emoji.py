import json
from types import SimpleNamespace

from borgmarks.config import Settings
from borgmarks.folder_emoji import _apply_emoji_mapping, _build_emoji_batches, _folder_nodes, enrich_folder_emojis
from borgmarks.model import Bookmark


def test_folder_nodes_builds_hierarchy_counts():
    b1 = Bookmark(id="1", title="a", url="https://a")
    b2 = Bookmark(id="2", title="b", url="https://b")
    b1.assigned_path = ["Computers", "Dev"]
    b2.assigned_path = ["💻 Computers", "Dev"]

    nodes = dict(_folder_nodes([b1, b2]))
    assert nodes[("Computers",)] == 2
    assert nodes[("Computers", "Dev")] == 2


def test_apply_emoji_mapping_adds_prefix_only_when_missing():
    b1 = Bookmark(id="1", title="a", url="https://a")
    b2 = Bookmark(id="2", title="b", url="https://b")
    b1.assigned_path = ["Computers", "Dev"]
    b2.assigned_path = ["💻 Computers", "Dev"]
    changed = _apply_emoji_mapping(
        [b1, b2],
        {
            ("Computers",): "💻",
            ("Computers", "Dev"): "🧑",
        },
    )
    assert changed == 2
    assert b1.assigned_path == ["💻 Computers", "🧑 Dev"]
    assert b2.assigned_path == ["💻 Computers", "🧑 Dev"]


def test_build_emoji_batches_is_recursive_parent_with_first_level_children():
    b1 = Bookmark(id="1", title="a", url="https://a")
    b2 = Bookmark(id="2", title="b", url="https://b")
    b3 = Bookmark(id="3", title="c", url="https://c")
    b1.assigned_path = ["Computers", "Dev", "Python"]
    b2.assigned_path = ["Computers", "Dev", "Rust"]
    b3.assigned_path = ["Computers", "Cloud"]

    nodes = _folder_nodes([b1, b2, b3])
    batches = _build_emoji_batches(nodes)

    assert batches
    assert batches[0] == [("Computers",), ("Computers", "Dev"), ("Computers", "Cloud")]
    assert [("Computers", "Dev"), ("Computers", "Dev", "Python"), ("Computers", "Dev", "Rust")] in batches


def test_enrich_folder_emojis_runs_in_batches(monkeypatch):
    calls = []

    def _fake_suggest_folder_emojis(**kwargs):
        payload = json.loads(kwargs["user_payload"])
        calls.append(payload)
        suggestions = []
        for row in payload["folders"]:
            p = row["path"]
            if p[-1] == "Computers":
                emoji = "💻"
            elif p[-1] == "Dev":
                emoji = "🧑"
            else:
                emoji = "📁"
            suggestions.append(SimpleNamespace(path=p, emoji=emoji))
        return SimpleNamespace(parsed=SimpleNamespace(suggestions=suggestions), ms=10)

    monkeypatch.setattr("borgmarks.folder_emoji.suggest_folder_emojis", _fake_suggest_folder_emojis)

    cfg = Settings()
    b1 = Bookmark(id="1", title="a", url="https://a")
    b2 = Bookmark(id="2", title="b", url="https://b")
    b1.assigned_path = ["Computers", "Dev", "Python"]
    b2.assigned_path = ["Computers", "Cloud"]

    enrich_folder_emojis([b1, b2], cfg)

    assert len(calls) >= 2
    assert b1.assigned_path[0].startswith("💻 ")
    assert b1.assigned_path[1].startswith("🧑 ")
