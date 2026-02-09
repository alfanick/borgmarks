from pathlib import Path

from borgmarks.parse_netscape import parse_bookmarks_html
from borgmarks.writer_netscape import build_tree, write_firefox_html


def test_parse_and_write(tmp_path: Path):
    src = Path(__file__).parent / "fixtures" / "sample_bookmarks.html"
    bms, _ = parse_bookmarks_html(src)
    for b in bms:
        b.assigned_path = ["Test"]
    tree = build_tree(bms)

    out = tmp_path / "out.html"
    write_firefox_html(
        out_path=out,
        bookmarks_tree=tree,
        toolbar_spec={"folders": ["Now / Inbox"], "links": [{"title": "GitHub", "url": "https://github.com/"}]},
        embed_metadata=True,
        title_root="Bookmarks (test)",
    )
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "NETSCAPE-Bookmark-file-1" in text
    assert "Bookmarks Toolbar" in text
