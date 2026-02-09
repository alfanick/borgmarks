from borgmarks.fetch import _extract_meta


def test_extract_meta_prefers_explicit_favicon_link():
    html = b"""
    <html><head>
      <title>X</title>
      <link rel="icon" href="/assets/favicon-32.png">
      <meta name="description" content="desc">
    </head><body><p>hello</p></body></html>
    """
    title, desc, snippet, favicon = _extract_meta(html, base_url="https://example.com/a/b")
    assert title == "X"
    assert desc == "desc"
    assert "hello" in (snippet or "")
    assert favicon == "https://example.com/assets/favicon-32.png"


def test_extract_meta_favicon_falls_back_to_default_path():
    html = b"<html><head><title>X</title></head><body></body></html>"
    _title, _desc, _snippet, favicon = _extract_meta(html, base_url="https://example.com/abc")
    assert favicon == "https://example.com/favicon.ico"
