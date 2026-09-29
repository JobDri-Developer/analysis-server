from app.config import settings
from app.openai_client import _openai_timeout_seconds


def test_openai_timeout_uses_explicit_setting(monkeypatch):
    monkeypatch.setattr(settings, "openai_timeout_seconds", 2.5)
    monkeypatch.setattr(settings, "analysis_queue_timeout_millis", 300000)

    assert _openai_timeout_seconds() == 2.5


def test_openai_timeout_falls_back_to_queue_timeout(monkeypatch):
    monkeypatch.setattr(settings, "openai_timeout_seconds", None)
    monkeypatch.setattr(settings, "analysis_queue_timeout_millis", 300000)

    assert _openai_timeout_seconds() == 300.0
