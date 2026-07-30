"""알림 조립·발송 게이트의 순수 로직 테스트 (#43).

이전에는 rules.py만 테스트가 있었다. 순수 함수인데도 미검증이던 것들 —
심각도 계산, 이메일·카카오 본문 조립, send_all(send_external=False)의
"외부만 생략, 콘솔·이력은 유지" 계약 — 을 고정한다.
"""
import pytest

pytest.importorskip("requests")
pytest.importorskip("dotenv")
pytest.importorskip("kafka")
pytest.importorskip("opensearchpy")

from alert import (  # noqa: E402
    AlertManager,
    OpenSearchAlertSender,
    _build_kakao_text,
    _build_weather_email,
)
from consumer.consumer import WeatherDataProcessor  # noqa: E402


def alert_data(**over):
    base = {
        "timestamp": "2026-07-30T06:00:00+09:00",
        "region": "서울",
        "indices": {"pm10": 82.0, "pm25": 20.0, "precipitation_probability": 70.0},
        "levels": {"pm10_level": "나쁨", "pm25_level": "보통",
                   "precipitation_probability_level": "나쁨"},
        "recommendations": {"pm10_rec": "마스크 착용 권고"},
        "emojis": {},
        "action_groups": {
            "마스크_필수": {"description": "d", "action": "KF94 마스크 착용",
                          "color": "🔴", "reasons": ["미세먼지: 나쁨"]},
        },
        "raw_data": {"forecast_type": "morning_mixed",
                     "min_temperature": 12.0, "max_temperature": 20.0},
        "data_warnings": {},
        "grade_signature": "pm10=나쁨",
        "event_id": "서울:2026073006",
    }
    base.update(over)
    return base


# ---------- 심각도 ----------

def test_severity_ladder():
    sev = OpenSearchAlertSender._calculate_severity
    assert sev({"action_groups": {"외출_자제": {}}}) == "CRITICAL"
    assert sev({"action_groups": {"마스크_필수": {}}}) == "HIGH"
    assert sev({"action_groups": {"특보_확인": {}}}) == "HIGH"
    assert sev({"action_groups": {"우산_준비": {}}}) == "MEDIUM"
    assert sev({"action_groups": {"정상": {}}}) == "LOW"


def test_severity_worst_group_wins():
    groups = {"우산_준비": {}, "외출_자제": {}}
    assert OpenSearchAlertSender._calculate_severity({"action_groups": groups}) == "CRITICAL"


# ---------- 이메일 본문 ----------

def test_email_contains_actions_and_judgements():
    mail = _build_weather_email(alert_data())
    assert "오늘 아침 기상 알림" in mail["subject"] or "행동 권고" in mail["subject"]
    assert "KF94 마스크 착용" in mail["text"]
    assert "미세먼지 PM10: 82" in mail["text"]
    assert "최저 12℃" in mail["text"]
    # HTML 버전도 같은 내용을 담는다
    assert "KF94 마스크 착용" in mail["html"]


def test_email_shows_warnings_block_only_when_present():
    without = _build_weather_email(alert_data())
    assert "참고" not in without["text"]
    with_w = _build_weather_email(alert_data(data_warnings={"air_quality": "응답 없음"}))
    assert "참고" in with_w["text"] and "응답 없음" in with_w["text"]


def test_email_normal_case_says_all_clear():
    mail = _build_weather_email(alert_data(action_groups={"정상": {"action": "-", "reasons": []}}))
    assert "모든 지수가 정상범위" in mail["text"]


def test_email_missing_values_render_as_unknown():
    data = alert_data(indices={"pm10": None}, levels={})
    mail = _build_weather_email(data)
    assert "미세먼지 PM10: 정보없음" in mail["text"]


# ---------- 카카오 본문 ----------

def test_kakao_text_is_action_first():
    text = _build_kakao_text(alert_data())
    assert "06:00" in text
    assert "KF94 마스크 착용" in text


# ---------- send_all의 외부 생략 계약 (#15의 미검증이던 절반) ----------

class FakeOS:
    def __init__(self): self.docs = []; self.states = {}
    def index(self, index, body, id=None): self.docs.append(body); return {"_id": id or "x"}
    def update(self, index, id, body): self.states.setdefault(id, {}).update(body["doc"]); return {"_id": id}
    def get(self, index, id):
        if id not in self.states: raise LookupError(id)
        return {"_source": self.states[id]}
    def search(self, **kw): return {"hits": {"hits": []}}


@pytest.fixture
def manager(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGNATURE_STATE_PATH", str(tmp_path / "sig.json"))
    for var in ("SLACK_ENABLED", "EMAIL_ENABLED", "KAKAO_ENABLED"):
        monkeypatch.setenv(var, "false")
    return AlertManager(opensearch_client=FakeOS())


def test_suppressed_send_keeps_console_and_history(manager, capsys):
    results = manager.send_all(alert_data(), send_external=False)

    assert results["console"] is True                 # 콘솔은 항상
    assert len(manager.opensearch_sender.client.docs) == 1   # 이력도 항상
    assert results["slack"] is False and results["email"] is False and results["kakao"] is False
    assert "기상지수 알림" in capsys.readouterr().out  # 실제 콘솔 출력 확인


def test_external_send_skips_disabled_channels(manager):
    # 채널 전부 비활성 — 시도 자체가 실패로 기록되지 않아야 한다
    results = manager.send_all(alert_data(), send_external=True)
    assert results["opensearch"] is True
    assert results["slack"] is False  # 비활성은 False (미발송)


# ---------- process_current_weather 정상 경로 ----------

def test_processor_full_payload_grades_every_index():
    msg = {
        "timestamp": "2026-07-30T12:00:00+09:00", "region": "서울",
        "oak_pollen": 1, "pine_pollen": 3, "pm10": 82, "pm25": 20,
        "yellow_dust": 0, "feels_like_temp": 28,
        "other_special_notice": "없음", "precipitation_probability": 70,
        "uv_index": 6,
    }
    p = WeatherDataProcessor.process_current_weather(msg)

    assert p["levels"]["pm10_level"] == "나쁨"
    assert p["levels"]["pine_pollen_level"] == "나쁨"
    assert p["levels"]["yellow_dust_level" if "yellow_dust_level" in p["levels"] else "dust_level"] == "좋음"
    # 9개 지수 전부에 등급·권고·이모지가 붙는다
    assert len(p["levels"]) == 9
    assert len(p["recommendations"]) == 9
    assert len(p["classification_objects"]) == 9
