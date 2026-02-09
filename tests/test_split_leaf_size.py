from borgmarks.model import Bookmark
from borgmarks.split import enforce_leaf_limits


def test_enforce_leaf_limits_splits():
    bms = []
    for i in range(55):
        b = Bookmark(id=f"b{i}", title=f"t{i}", url=f"https://example.com/{i}")
        b.domain = "example.com"
        b.assigned_path = ["Reading", "Inbox"]
        bms.append(b)

    enforce_leaf_limits(bms, leaf_max_links=20, max_depth=4)
    paths = set(tuple(b.assigned_path) for b in bms)
    assert any(len(p) >= 3 for p in paths)
