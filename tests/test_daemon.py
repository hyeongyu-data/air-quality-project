"""컨슈머 데몬 수명 관리 검증 (#40 회귀 방지).

이전 동작: run_continuous(duration_seconds=3600)가 1시간마다 스스로 종료하고
restart: unless-stopped로 부활했다. 로그에서 정상 종료와 크래시 루프가
구분되지 않았고, 재시작 시점의 연결 실패가 1시간 동안 고정됐다.

kafka/opensearch 연결 없이 수명 로직만 검증한다. WeatherAlertConsumer의
__init__은 실제 연결을 시도하므로 __new__로 우회해 필요한 필드만 만든다.
"""
import signal
import time
from pathlib import Path

import pytest

pytest.importorskip("kafka", reason="consumer 모듈이 kafka-python에 의존")
pytest.importorskip("opensearchpy")
pytest.importorskip("requests")
pytest.importorskip("dotenv")

from consumer import KafkaWeatherConsumer, WeatherAlertConsumer  # noqa: E402


def bare_consumer(tmp_path) -> WeatherAlertConsumer:
    c = WeatherAlertConsumer.__new__(WeatherAlertConsumer)
    c._running = True
    c.heartbeat_path = tmp_path / "heartbeat"
    return c


# ---------- 시그널 종료 ----------

def test_signal_handler_stops_the_loop(tmp_path):
    c = bare_consumer(tmp_path)
    c._handle_signal(signal.SIGTERM, None)
    assert c._running is False


def test_sleep_reacts_to_shutdown_quickly(tmp_path):
    """통짜 sleep이면 docker stop 유예시간(10초) 안에 못 깨어난다."""
    c = bare_consumer(tmp_path)
    c._running = False
    start = time.monotonic()
    c._sleep(30)
    assert time.monotonic() - start < 2


def test_run_forever_exits_after_signal_and_shuts_down(tmp_path, monkeypatch):
    c = bare_consumer(tmp_path)
    calls = {"once": 0, "shutdown": 0}

    def fake_once():
        calls["once"] += 1
        if calls["once"] >= 2:          # 두 사이클 뒤 SIGTERM이 온 상황
            c._handle_signal(signal.SIGTERM, None)
        return False

    monkeypatch.setattr(c, "run_once", fake_once)
    monkeypatch.setattr(c, "_shutdown", lambda: calls.__setitem__("shutdown", 1))
    monkeypatch.setattr(signal, "signal", lambda *a: None)  # pytest 스레드 보호
    c.kafka_consumer = type("K", (), {"topics": ["seoul-weather"]})()

    c.run_forever(poll_interval=0)

    assert calls["once"] == 2           # 시그널 후 다음 사이클을 돌지 않는다
    assert calls["shutdown"] == 1       # finally에서 정리가 호출된다


# ---------- 하트비트 ----------

def test_heartbeat_is_touched_each_cycle(tmp_path, monkeypatch):
    c = bare_consumer(tmp_path)
    monkeypatch.setattr(c, "run_once", lambda: c._handle_signal(signal.SIGTERM, None))
    monkeypatch.setattr(c, "_shutdown", lambda: None)
    monkeypatch.setattr(signal, "signal", lambda *a: None)
    c.kafka_consumer = type("K", (), {"topics": []})()

    assert not c.heartbeat_path.exists()
    c.run_forever(poll_interval=0)
    assert c.heartbeat_path.exists()


def test_heartbeat_failure_does_not_crash_loop(tmp_path):
    c = bare_consumer(tmp_path)
    c.heartbeat_path = Path("/nonexistent-dir/heartbeat")
    c._touch_heartbeat()  # 예외 없이 경고만


# ---------- Kafka 재연결 백오프 ----------

def make_kafka() -> KafkaWeatherConsumer:
    k = KafkaWeatherConsumer.__new__(KafkaWeatherConsumer)
    k.consumer = None
    k._next_retry_at = 0.0
    k._retry_interval = 5.0
    return k


def test_ensure_connection_respects_backoff(monkeypatch):
    k = make_kafka()
    attempts = []
    monkeypatch.setattr(k, "connect", lambda: attempts.append(1) or False)

    k._next_retry_at = time.time() + 60   # 백오프 대기 중
    assert k.ensure_connection() is False
    assert attempts == []                  # 대기 중에는 시도하지 않는다

    k._next_retry_at = 0.0
    k.ensure_connection()
    assert attempts == [1]                 # 대기가 끝나면 시도한다


def test_ensure_connection_noop_when_connected(monkeypatch):
    k = make_kafka()
    k.consumer = object()
    monkeypatch.setattr(k, "connect", lambda: pytest.fail("연결돼 있으면 재시도 금지"))
    assert k.ensure_connection() is True


def test_retry_interval_grows_and_caps():
    """연속 실패 시 5→10→…→300초로 늘고 상한에서 멈춘다."""
    k = make_kafka()
    intervals = []
    for _ in range(10):
        intervals.append(k._retry_interval)
        # connect() 실패 경로의 백오프 산식만 재현
        k._next_retry_at = time.time() + k._retry_interval
        k._retry_interval = min(k._retry_interval * 2, k.MAX_RETRY_INTERVAL_SECONDS)
    assert intervals[0] == 5.0
    assert intervals[1] == 10.0
    assert max(intervals) == k.MAX_RETRY_INTERVAL_SECONDS == 300
