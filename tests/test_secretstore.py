"""`_FILE` 우선 시크릿 로더 검증 (#117).

Docker/K8s secret은 파일로 마운트된다(`/run/secrets/*`). `FOO_FILE`이 있으면 그
파일을, 없으면 `FOO` env를 읽어야 한다 — 그래야 dev(.env)와 운영(secret 파일)이
같은 코드로 돈다.
"""
import pytest

from consumer.secretstore import read_secret
from producer.secretstore import read_secret as read_secret_producer


def test_file_wins_over_env_and_is_stripped(tmp_path, monkeypatch):
    f = tmp_path / "smtp_password"
    f.write_text("  s3cr3t\n", encoding="utf-8")
    monkeypatch.setenv("SMTP_PASSWORD", "from-env")
    monkeypatch.setenv("SMTP_PASSWORD_FILE", str(f))
    assert read_secret("SMTP_PASSWORD") == "s3cr3t"


def test_env_fallback_when_no_file(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL_FILE", raising=False)
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://example.test/hook")
    assert read_secret("SLACK_WEBHOOK_URL") == "https://example.test/hook"


def test_default_when_nothing_set(monkeypatch):
    monkeypatch.delenv("NOPE", raising=False)
    monkeypatch.delenv("NOPE_FILE", raising=False)
    assert read_secret("NOPE") is None
    assert read_secret("NOPE", "fallback") == "fallback"


def test_missing_file_path_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("KAKAO_CLIENT_SECRET_FILE", str(tmp_path / "absent"))
    with pytest.raises(RuntimeError, match="KAKAO_CLIENT_SECRET_FILE"):
        read_secret("KAKAO_CLIENT_SECRET")


def test_producer_twin_has_same_contract(tmp_path, monkeypatch):
    f = tmp_path / "weather_api_key"
    f.write_text("abc123", encoding="utf-8")
    monkeypatch.setenv("WEATHER_API_KEY_FILE", str(f))
    assert read_secret_producer("WEATHER_API_KEY") == "abc123"
