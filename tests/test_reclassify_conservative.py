from borgmarks.classify import _allow_conservative_reclass_move


def test_conservative_reclass_blocks_near_sibling_move_without_gain():
    allowed, reason = _allow_conservative_reclass_move(
        prev_path=["Photography", "Camera"],
        new_path=["Photography", "Video"],
        folder_sizes={
            ("Photography", "Camera"): 5,
            ("Photography", "Video"): 6,
        },
        min_folder_gain=2,
    )
    assert allowed is False
    assert "near-sibling" in reason


def test_conservative_reclass_allows_move_from_generic_bucket():
    allowed, _ = _allow_conservative_reclass_move(
        prev_path=["Archive", "Unclassified"],
        new_path=["Photography", "Video"],
        folder_sizes={
            ("Archive", "Unclassified"): 20,
            ("Photography", "Video"): 4,
        },
        min_folder_gain=2,
    )
    assert allowed is True


def test_conservative_reclass_allows_strong_cluster_move():
    allowed, reason = _allow_conservative_reclass_move(
        prev_path=["Photography", "Camera"],
        new_path=["Photography", "Video"],
        folder_sizes={
            ("Photography", "Camera"): 1,
            ("Photography", "Video"): 6,
        },
        min_folder_gain=2,
    )
    assert allowed is True
    assert reason == "accepted"
