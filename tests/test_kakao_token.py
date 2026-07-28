"""카카오 refresh token 회전 저장 검증 (#50 회귀 방지).

이전 동작: 갱신 응답에 새 refresh token이 와도 로그만 남기고 버렸다.
기존 토큰이 만료되는 시점에 카카오 알림이 영구 중단되고, 발송 실패는
로그에만 남아 아무도 모르는 상태가 됐다.

alert 모듈은 kafka/opensearch를 import 하지 않으므로 requests만 있으면
단독 테스트가 가능하다. 없으면 스킵한다.
"""
import json
import os

import pytest

pytest.importorskip("requests", reason="alert 모듈이 requests에 의존")

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from consumer.alert import KakaoAlertSender  # noqa: E402


@pytest.fixture
def state_path(tmp_path, monkeypatch):
    path = tmp_path / "state" / "kakao_token.json"
    monkeypatch.setenv("KAKAO_ENABLED", "true")
    monkeypatch.setenv("KAKAO_REST_API_KEY", "rest-key")
    monkeypatch.setenv("KAKAO_REFRESH_TOKEN", "ENV_TOKEN")
    monkeypatch.setenv("KAKAO_TOKEN_STATE_PATH", str(path))
    return path


def test_env_token_used_when_no_state_file(state_path):
    assert KakaoAlertSender().refresh_token == "ENV_TOKEN"


def test_rotated_token_is_persisted(state_path):
    sender = KakaoAlertSender()
    sender._store_refresh_token("ROTATED", 1209600)

    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["refresh_token"] == "ROTATED"
    assert saved["refresh_token_expires_in"] == 1209600
    assert sender.refresh_token == "ROTATED"


def test_state_file_is_owner_only(state_path):
    KakaoAlertSender()._store_refresh_token("ROTATED", None)
    assert oct(state_path.stat().st_mode & 0o777) == "0o600"


def test_stored_token_survives_restart_and_wins_over_env(state_path):
    KakaoAlertSender()._store_refresh_token("ROTATED", None)
    # 컨슈머는 매시간 재시작한다 — 새 인스턴스가 저장값을 읽어야 한다
    assert KakaoAlertSender().refresh_token == "ROTATED"


def test_corrupted_state_falls_back_to_env(state_path):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{깨진 json", encoding="utf-8")
    # 상태 파일이 깨져도 발송 자체를 막지는 않는다
    assert KakaoAlertSender().refresh_token == "ENV_TOKEN"


def test_empty_stored_token_falls_back_to_env(state_path):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({"refresh_token": ""}), encoding="utf-8")
    assert KakaoAlertSender().refresh_token == "ENV_TOKEN"


def test_store_failure_does_not_raise(state_path, monkeypatch):
    sender = KakaoAlertSender()
    monkeypatch.setattr(sender, "state_path", state_path.parent / "nope" / "x.json")
    monkeypatch.setattr(
        "pathlib.Path.write_text",
        lambda *a, **k: (_ for _ in ()).throw(OSError("read-only fs")),
    )
    # 저장 실패가 이번 회차 발송을 막아서는 안 된다
    sender._store_refresh_token("ROTATED", None)
