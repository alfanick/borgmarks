from pathlib import Path

from borgmarks.parse_netscape import parse_bookmarks_html
from borgmarks.model import Bookmark
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


def test_write_keeps_similar_folders_grouped(tmp_path: Path):
    b1 = Bookmark(id="1", title="r1", url="https://ex.com/r1")
    b2 = Bookmark(id="2", title="c1", url="https://ex.com/c1")
    b3 = Bookmark(id="3", title="s1", url="https://ex.com/s1")
    b1.assigned_path = ["Cooking", "Recipes"]
    b2.assigned_path = ["Programming", "C"]
    b3.assigned_path = ["Cooking", "Shrimp"]

    out = tmp_path / "grouped.html"
    write_firefox_html(
        out_path=out,
        bookmarks_tree=build_tree([b1, b2, b3]),
        toolbar_spec={"folders": [], "links": []},
        embed_metadata=False,
        title_root="Bookmarks",
    )
    text = out.read_text(encoding="utf-8")
    # Both Cooking subfolders must appear before Programming subtree.
    idx_recipes = text.find("<DT><H3>Recipes</H3>")
    idx_shrimp = text.find("<DT><H3>Shrimp</H3>")
    idx_prog = text.find("<DT><H3>Programming</H3>")
    assert idx_recipes != -1 and idx_shrimp != -1 and idx_prog != -1
    assert idx_recipes < idx_prog
    assert idx_shrimp < idx_prog


def test_write_orders_leaf_links_by_freshness_desc(tmp_path: Path):
    old = Bookmark(id="1", title="Old", url="https://ex.com/old", add_date=100)
    new = Bookmark(id="2", title="New", url="https://ex.com/new", add_date=200)
    mid = Bookmark(id="3", title="Mid", url="https://ex.com/mid", add_date=150)
    for b in (old, new, mid):
        b.assigned_path = ["Reading", "Inbox"]

    out = tmp_path / "freshness.html"
    write_firefox_html(
        out_path=out,
        bookmarks_tree=build_tree([old, new, mid]),
        toolbar_spec={"folders": [], "links": []},
        embed_metadata=False,
        title_root="Bookmarks",
    )
    text = out.read_text(encoding="utf-8")
    n = text.find(">New<")
    m = text.find(">Mid<")
    o = text.find(">Old<")
    assert n != -1 and m != -1 and o != -1
    assert n < m < o


def test_write_uses_favicon_or_emoji_fallback_icon(tmp_path: Path):
    with_icon = Bookmark(id="1", title="Icon", url="https://ex.com/icon")
    no_icon = Bookmark(id="2", title="NoIcon", url="https://ex.com/noicon")
    with_icon.assigned_path = ["Photography", "📷 Camera"]
    no_icon.assigned_path = ["Photography", "📷 Camera"]
    with_icon.meta["icon_uri"] = "https://ex.com/favicon.ico"

    out = tmp_path / "icons.html"
    write_firefox_html(
        out_path=out,
        bookmarks_tree=build_tree([with_icon, no_icon]),
        toolbar_spec={"folders": [], "links": []},
        embed_metadata=False,
        title_root="Bookmarks",
    )
    text = out.read_text(encoding="utf-8")
    assert 'ICON_URI="https://ex.com/favicon.ico"' in text
    assert 'ICON="data:image/svg+xml;utf8,' in text


def test_parse_ignores_seeded_toolbar_entries(tmp_path: Path):
    html = """<!DOCTYPE NETSCAPE-Bookmark-file-1>
<TITLE>Bookmarks</TITLE>
<H1>Bookmarks</H1>
<DL><p>
  <DT><H3 PERSONAL_TOOLBAR_FOLDER="true">Bookmarks Toolbar</H3>
  <DL><p>
    <DT><A HREF="https://seed.example/" data-borg-seed="1">Seed</A>
    <DT><A HREF="https://keep.example/">Keep</A>
  </DL><p>
</DL><p>
"""
    src = tmp_path / "seeded.html"
    src.write_text(html, encoding="utf-8")
    bms, _ = parse_bookmarks_html(src)
    urls = [b.url for b in bms]
    assert "https://seed.example/" not in urls
    assert "https://keep.example/" in urls
