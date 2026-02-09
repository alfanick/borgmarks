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


def test_parse_handles_nested_dt_malformed_html(tmp_path: Path):
    # Real exports can contain malformed DT nesting where logical siblings are
    # represented as nested DT tags under one DL.
    html = """<!DOCTYPE NETSCAPE-Bookmark-file-1>
<TITLE>Bookmarks</TITLE>
<H1>Bookmarks</H1>
<DL><p>
  <DT><H3>Folder A</H3>
  <DL><p>
    <DT><A HREF="https://a.example/">A</A>
    <DT><A HREF="https://b.example/">B</A>
    <DT><A HREF="https://c.example/">C</A>
  </DL><p>
</DL><p>
"""
    src = tmp_path / "nested-dt.html"
    src.write_text(html, encoding="utf-8")

    bms, _ = parse_bookmarks_html(src)

    assert len(bms) == 3
    assert [b.url for b in bms] == [
        "https://a.example/",
        "https://b.example/",
        "https://c.example/",
    ]
    assert all(b.folder_path == ["Folder A"] for b in bms)
