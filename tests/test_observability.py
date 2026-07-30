"""구조화 로깅·메트릭 검증 (#49).

관측의 최소 요건: 알림이 실제로 전달됐는가, 얼마나 걸렸는가, 무엇이
결측이었는가를 기계가 집계할 수 있는 형태로 남긴다.
"""
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("requests")
pytest.importorskip("dotenv")

from logutil import EventIdFilter, JsonFormatter, current_event_id  # noqa: E402
from metrics import (  # noqa: E402
    MetricsSink,
    build_message_metrics,
    e2e_latency_seconds,
    missing_indices,
)

KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 7, 30, 12, 0, 30, tzinfo=KST)


# ---------- JSON 로깅 ----------

def make_record(msg, **extra):
    record = logging.LogRecord("t", logging.INFO, "f", 1, msg, None, None)
    for k, v in extra.items():
        setattr(record, k, v)
    return record


def test_json_log_is_parseable_with_fields():
    line = JsonFormatter().format(make_record("처리 완료", metric_duration_ms=42.5))
    entry = json.loads(line)
    assert entry["message"] == "처리 완료"
    assert entry["level"] == "INFO"
    assert entry["duration_ms"] == 42.5      # metric_ 접두사가 벗겨져 필드로


def test_event_id_correlates_all_logs_in_scope():
    token = current_event_id.set("서울:2026073012")
    try:
        entry = json.loads(JsonFormatter().format(make_record("아무 로그")))
        assert entry["event_id"] == "서울:2026073012"
    finally:
        current_event_id.reset(token)
    # 스코프 밖에서는 붙지 않는다
    assert "event_id" not in json.loads(JsonFormatter().format(make_record("밖")))


def test_text_filter_injects_event_id():
    record = make_record("x")
    token = current_event_id.set("서울:2026073012")
    try:
        EventIdFilter().filter(record)
        assert record.event_id == " [서울:2026073012]"
    finally:
        current_event_id.reset(token)


def test_exception_is_included():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys
        record = logging.LogRecord("t", logging.ERROR, "f", 1, "err", None, sys.exc_info())
    assert "ValueError" in json.loads(JsonFormatter().format(record))["exc"]


# ---------- e2e 지연 ----------

def test_e2e_latency_from_publish_to_now():
    sent = (NOW - timedelta(seconds=12, milliseconds=500)).isoformat()
    assert e2e_latency_seconds(sent, NOW) == 12.5


def test_e2e_latency_bad_inputs_are_none():
    assert e2e_latency_seconds(None, NOW) is None
    assert e2e_latency_seconds("not-a-date", NOW) is None
    naive = NOW.replace(tzinfo=None).isoformat()
    assert e2e_latency_seconds(naive, NOW) is None   # naive는 비교 불가 — 계측 포기


# ---------- 결측 지수 ----------

def test_missing_indices_from_levels():
    levels = {"pm10_level": "나쁨", "pm25_level": "정보없음", "dust_level": "정보없음"}
    assert missing_indices(levels) == ["dust", "pm25"]


# ---------- 메시지 메트릭 문서 ----------

def processed(**over):
    base = {
        "event_id": "서울:2026073012", "region": "서울",
        "timestamp": (NOW - timedelta(seconds=3)).isoformat(),
        "levels": {"pm10_level": "나쁨", "pm25_level": "정보없음"},
        "alert_severity": "HIGH",
    }
    base.update(over)
    return base


def test_metrics_doc_delivery_success():
    doc = build_message_metrics(
        processed(), {"slack": False, "email": False, "kakao": True, "opensearch": True},
        send_external=True, duration_ms=87.3, now=NOW,
    )
    assert doc["event_id"] == "서울:2026073012"
    assert doc["delivered_channels"] == ["kakao"]
    assert doc["delivery_failed"] is False
    assert doc["suppressed"] is False
    assert doc["e2e_latency_seconds"] == 3.0
    assert doc["missing_count"] == 1 and doc["missing_indices"] == ["pm25"]


def test_metrics_doc_marks_total_delivery_failure():
    """시도했는데 전 채널 실패 — 알람 (b)의 판정 필드."""
    doc = build_message_metrics(
        processed(), {"slack": False, "email": False, "kakao": False, "opensearch": True},
        send_external=True, duration_ms=10, now=NOW,
    )
    assert doc["delivery_failed"] is True


def test_metrics_doc_suppressed_is_not_failure():
    doc = build_message_metrics(
        processed(), {"slack": False, "email": False, "kakao": False, "opensearch": True},
        send_external=False, duration_ms=10, now=NOW,
    )
    assert doc["suppressed"] is True
    assert doc["delivery_failed"] is False


# ---------- 싱크 ----------

class FakeClient:
    def __init__(self, fail=False):
        self.fail = fail; self.indexed = []
    def index(self, index, body):
        if self.fail: raise RuntimeError("down")
        self.indexed.append((index, body)); return {"_id": "1"}


def test_sink_uses_monthly_index():
    sink = MetricsSink(FakeClient())
    assert sink.emit({"timestamp": "2026-07-30T12:00:00+09:00"}) is True
    assert sink.client.indexed[0][0] == "weather-metrics-2026.07"


def test_sink_failure_never_raises():
    assert MetricsSink(FakeClient(fail=True)).emit({"timestamp": "2026-07-30T12:00:00+09:00"}) is False
    assert MetricsSink(None).emit({"timestamp": "2026-07-30T12:00:00+09:00"}) is False
