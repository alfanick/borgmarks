from borgmarks.model import Bookmark
from borgmarks.split import _bucket_for_url, enforce_leaf_limits


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


def test_enforce_leaf_limits_is_stable_for_already_bucketed_leaf():
    bms = []
    # Build a set that already belongs to A-F bucket, so re-running the
    # splitter should not append A-F again.
    i = 0
    while len(bms) < 40:
        url = f"http://example.com/item-{i}"
        i += 1
        if _bucket_for_url(url) != "A-F":
            continue
        b = Bookmark(id=f"b{len(bms)}", title=f"t{len(bms)}", url=url)
        b.domain = "example.com"
        b.assigned_path = ["Reading", "Inbox", "A-F"]
        bms.append(b)

    before = [list(b.assigned_path) for b in bms]
    enforce_leaf_limits(bms, leaf_max_links=20, max_depth=4)
    after = [list(b.assigned_path) for b in bms]

    # Never append duplicate final bucket (A-F/A-F) during re-runs.
    for p in after:
        assert len(p) < 2 or p[-1] != p[-2]
    # If leaf was already split by this bucket suffix, paths stay stable.
    assert after == before
