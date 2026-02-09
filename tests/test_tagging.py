from borgmarks.config import Settings
from borgmarks.model import Bookmark
from borgmarks.tagging import enrich_bookmark_tags


def _mk_bookmark(i: int, *, domain: str = "example.com") -> Bookmark:
    b = Bookmark(id=f"b{i}", title=f"T{i}", url=f"https://{domain}/{i}")
    b.domain = domain
    b.assigned_path = ["Reading", "Inbox"]
    return b


def test_tagging_normalizes_to_lowercase_underscore_and_caps_per_link():
    cfg = Settings()
    cfg.openai_tags_enrich = False
    cfg.openai_tags_max_per_link = 4
    cfg.openai_tags_max_global = 50

    b = _mk_bookmark(1)
    b.tags = [
        "Machine Learning",
        "machine-learning",
        "News!",
        "  BLOG  ",
        "🔥camera",
        "very long three words",
    ]
    enrich_bookmark_tags([b], cfg)
    assert b.tags == ["machine_learning", "news", "blog", "camera"]


def test_tagging_uppercases_abbreviations():
    cfg = Settings()
    cfg.openai_tags_enrich = False
    cfg.openai_tags_max_per_link = 4
    cfg.openai_tags_max_global = 50

    b = _mk_bookmark(1)
    b.tags = ["AI", "ML", "LLM", "python"]
    enrich_bookmark_tags([b], cfg)
    assert b.tags == ["AI", "ML", "LLM", "python"]


def test_tagging_enforces_global_cap_with_stable_fallback():
    cfg = Settings()
    cfg.openai_tags_enrich = False
    cfg.openai_tags_max_per_link = 4
    cfg.openai_tags_max_global = 2

    b1 = _mk_bookmark(1, domain="amazon.com")
    b1.tags = ["shopping", "deals"]
    b2 = _mk_bookmark(2, domain="news.ycombinator.com")
    b2.tags = ["news", "tech"]
    b3 = _mk_bookmark(3, domain="blog.example.com")
    b3.tags = ["blog", "reading"]

    enrich_bookmark_tags([b1, b2, b3], cfg)
    all_tags = sorted({t for b in (b1, b2, b3) for t in b.tags})
    assert len(all_tags) <= 2
    assert all("_" in t or t.isalpha() for t in all_tags)
    for b in (b1, b2, b3):
        assert 1 <= len(b.tags) <= 4
