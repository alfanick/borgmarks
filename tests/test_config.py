from borgmarks.config import Settings


def test_openai_agent_browser_defaults_disabled(monkeypatch):
    monkeypatch.delenv("BORG_OPENAI_AGENT_BROWSER", raising=False)
    monkeypatch.delenv("OPENAI_AGENT_BROWSER", raising=False)
    s = Settings.from_env()
    assert s.openai_agent_browser is False


def test_openai_agent_browser_alias_env_supported(monkeypatch):
    monkeypatch.delenv("BORG_OPENAI_AGENT_BROWSER", raising=False)
    monkeypatch.setenv("OPENAI_AGENT_BROWSER", "1")
    s = Settings.from_env()
    assert s.openai_agent_browser is True


def test_borg_openai_agent_browser_has_precedence_over_alias(monkeypatch):
    monkeypatch.setenv("BORG_OPENAI_AGENT_BROWSER", "0")
    monkeypatch.setenv("OPENAI_AGENT_BROWSER", "1")
    s = Settings.from_env()
    assert s.openai_agent_browser is False
