"""알람 워처의 조건 평가·쿨다운 로직 검증 (#121)."""
from datetime import datetime, timedelta

import pytest

pytest.importorskip("opensearchpy")
pytest.importorskip("kafka")

from consumer.alert_watch import Config, Snapshot, evaluate, filter_cooldown


def _cfg(**over):
    import os
    for k, v in over.items():
        os.environ[k] = str(v)
    return Config()


def snap(**over):
    base = dict(alert_docs_recent=5, delivery_failed_count=0, missing_hits=[],
                kafka_max_lag=0, cluster_status="green")
    base.update(over)
    return Snapshot(**base)


def codes(alerts):
    return {a["code"] for a in alerts}


def test_all_green_no_alerts():
    assert evaluate(snap(), Config()) == []


def test_a_fires_when_no_recent_history():
    assert codes(evaluate(snap(alert_docs_recent=0), Config())) == {"a"}
    assert evaluate(snap(alert_docs_recent=1), Config()) == []


def test_b_fires_on_delivery_failure():
    assert codes(evaluate(snap(delivery_failed_count=3), Config())) == {"b"}


def test_c_summarizes_missing_indices():
    hits = [{"event_id": "e1", "missing_indices": ["dust"]},
            {"event_id": "e2", "missing_indices": ["dust", "pm25"]}]
    out = evaluate(snap(missing_hits=hits), Config())
    assert codes(out) == {"c"}
    assert "dust" in out[0]["detail"] and "pm25" in out[0]["detail"]
    assert "2건" in out[0]["title"]


def test_d_fires_only_above_threshold():
    cfg = _cfg(ALERT_WATCH_LAG_THRESHOLD=4)
    assert evaluate(snap(kafka_max_lag=4), cfg) == []
    assert codes(evaluate(snap(kafka_max_lag=5), cfg)) == {"d"}
    assert evaluate(snap(kafka_max_lag=None), cfg) == []  # 수집 실패는 발화 안 함


def test_g_fires_only_on_red():
    assert evaluate(snap(cluster_status="yellow"), Config()) == []
    assert codes(evaluate(snap(cluster_status="red"), Config())) == {"g"}


def test_multiple_conditions_fire_together():
    out = evaluate(snap(alert_docs_recent=0, delivery_failed_count=1, cluster_status="red"),
                   Config())
    assert codes(out) == {"a", "b", "g"}


def test_cooldown_suppresses_recent_condition():
    cfg = _cfg(ALERT_WATCH_COOLDOWN_MINUTES=60)
    now = datetime(2026, 9, 11, 12, 0, 0)
    alerts = evaluate(snap(delivery_failed_count=1, cluster_status="red"), cfg)
    state = {"b": (now - timedelta(minutes=10)).isoformat()}  # b는 10분 전 발송
    fresh = filter_cooldown(alerts, state, cfg, now=now)
    assert codes(fresh) == {"g"}  # b는 쿨다운, g는 통과


def test_cooldown_expires():
    cfg = _cfg(ALERT_WATCH_COOLDOWN_MINUTES=60)
    now = datetime(2026, 9, 11, 12, 0, 0)
    alerts = evaluate(snap(delivery_failed_count=1), cfg)
    state = {"b": (now - timedelta(minutes=90)).isoformat()}  # 90분 전 → 만료
    assert codes(filter_cooldown(alerts, state, cfg, now=now)) == {"b"}


def test_cooldown_tolerates_garbage_state():
    assert codes(filter_cooldown(evaluate(snap(cluster_status="red"), Config()),
                                 {"g": "not-a-date"}, Config())) == {"g"}
