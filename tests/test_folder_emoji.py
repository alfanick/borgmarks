from borgmarks.folder_emoji import _apply_emoji_mapping, _folder_nodes
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
