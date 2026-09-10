"""메시지 계약·DLQ·수동 커밋 판단 검증 (#21 #48 회귀 방지).

이전 동작:
- 역직렬화가 poll() 안에 있어 깨진 메시지가 poison pill이 됐다.
- 자동 커밋이라 처리 실패 메시지가 조용히 유실됐다(at-most-once).
- poll 결과 중 첫 레코드만 처리하고 나머지를 버렸다.
"""
import json

import pytest
from unittest.mock import Mock

pytest.importorskip("requests")
pytest.importorskip("dotenv")
pytest.importorskip("kafka")
pytest.importorskip("opensearchpy")

import producer.contract as producer_contract  # noqa: E402
from consumer.consumer import KafkaWeatherConsumer, WeatherAlertConsumer  # noqa: E402
from kafka.structs import TopicPartition, OffsetAndMetadata  # noqa: E402
from consumer.schema import (  # noqa: E402
    REQUIRED_KEYS,
    SCHEMA_VERSION,
    InvalidMessage,
    parse_message,
)


def valid_payload(**over):
    base = {
        "schema_version": SCHEMA_VERSION,
        "event_id": "서울:2026073012",
        "timestamp": "2026-07-30T12:00:00+09:00",
        "region": "서울",
        "pm10": 45.0,
    }
    base.update(over)
    return base


def encode(payload) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


# ---------- 프로듀서·컨슈머 계약이 어긋나지 않는가 ----------

def test_schema_versions_match_across_sides():
    """양쪽 상수가 갈리면 모든 메시지가 DLQ로 간다 — 반드시 함께 올린다."""
    assert producer_contract.SCHEMA_VERSION == SCHEMA_VERSION


def test_required_keys_are_what_producer_stamps():
    # event_id·schema_version은 DAG가 각인, timestamp·region은 수집기가 만든다
    assert set(REQUIRED_KEYS) == {"event_id", "timestamp", "region"}


# ---------- parse_message ----------

def test_valid_message_round_trips():
    assert parse_message(encode(valid_payload()))["region"] == "서울"


def test_malformed_json_is_invalid_not_crash():
    with pytest.raises(InvalidMessage, match="JSON 파싱 실패"):
        parse_message(b"{broken json")


def test_non_object_payload_is_invalid():
    with pytest.raises(InvalidMessage, match="객체가 아닌"):
        parse_message(b'[1, 2, 3]')


def test_unknown_schema_version_is_invalid():
    with pytest.raises(InvalidMessage, match="schema_version"):
        parse_message(encode(valid_payload(schema_version=99)))


def test_missing_version_is_accepted_as_legacy():
    payload = valid_payload()
    del payload["schema_version"]
    assert parse_message(encode(payload))["event_id"]


def test_missing_required_keys_listed():
    payload = valid_payload(event_id=None, region="")
    with pytest.raises(InvalidMessage, match="event_id.*region"):
        parse_message(encode(payload))


def test_z_suffix_timestamp_normalized():
    """py3.10 fromisoformat이 Z를 못 읽어 하류가 조용히 실패하던 지점."""
    data = parse_message(encode(valid_payload(timestamp="2026-07-30T03:00:00Z")))
    assert data["timestamp"] == "2026-07-30T03:00:00+00:00"
    from datetime import datetime
    datetime.fromisoformat(data["timestamp"])  # 예외 없이 파싱


# ---------- handle_record / 배치 커밋 판단 ----------

class Record:
    topic = "seoul-weather"
    def __init__(self, value: bytes, offset: int = 0, partition: int = 0):
        self.value = value; self.offset = offset
        self.partition = partition


class FakeKafka:
    def __init__(self, batches):
        self._batches = list(batches)
        self.commits = 0
        self.committed = {}
        self.rewound = {}
        self.reset = False
        self.calls = []
        self.bootstrap_servers = "test:9092"
    def ensure_connection(self): return True
    def consume_batch(self, timeout_ms=5000):
        return self._batches.pop(0) if self._batches else []
    def commit(self, offsets):
        self.calls.append(("commit", offsets))
        self.commits += 1
        self.committed.update(offsets)
        return True
    def rewind(self, offsets):
        self.calls.append(("rewind", offsets))
        self.rewound.update(offsets)
    def reset_connection(self): self.reset = True


def bare(batches, dlq_ok=True, process_ok=True):
    c = WeatherAlertConsumer.__new__(WeatherAlertConsumer)
    c.kafka_consumer = FakeKafka(batches)
    c.opensearch_connector = type("O", (), {"ensure_connection": lambda s: False})()
    c.stats = {"total_processed": 0, "total_alerts_sent": 0, "errors": 0, "dlq": 0}
    c.dlq_topic = "seoul-weather-dlq"
    c._dlq_producer = None
    c.dlq_calls = []
    c._send_to_dlq = lambda record, reason: c.dlq_calls.append(reason) or dlq_ok
    if process_ok:
        c.process_message = lambda m: True
    else:
        def boom(m): raise RuntimeError("downstream broken")
        c.process_message = boom
    return c


def test_whole_batch_is_processed_and_committed():
    batch = [Record(encode(valid_payload(event_id=f"서울:{i}")), i) for i in range(3)]
    c = bare([batch])
    assert c.run_once() == 3                 # 첫 레코드만 처리하던 회귀 방지
    assert c.kafka_consumer.commits == 1


def test_invalid_record_goes_to_dlq_and_batch_commits():
    batch = [Record(b"{broken"), Record(encode(valid_payload()))]
    c = bare([batch])
    c.run_once()
    assert len(c.dlq_calls) == 1             # 깨진 것만 격리
    assert c.kafka_consumer.commits == 1     # 격리 성공했으므로 전진


def test_processing_exception_goes_to_dlq():
    c = bare([[Record(encode(valid_payload()))]], process_ok=False)
    c.run_once()
    assert c.dlq_calls and "처리 예외" in c.dlq_calls[0]
    assert c.kafka_consumer.commits == 1


def test_dlq_failure_holds_commit_for_retry():
    """격리조차 실패하면 커밋하지 않는다 — 메시지를 버리는 것보다 재처리."""
    c = bare([[Record(b"{broken")]], dlq_ok=False)
    c.run_once()
    assert c.kafka_consumer.commits == 0
    assert c.kafka_consumer.rewound == {TopicPartition("seoul-weather", 0): 0}


def test_empty_poll_is_a_noop():
    c = bare([])
    assert c.run_once() == 0
    assert c.kafka_consumer.commits == 0


def test_failed_partition_stops_at_gap_while_other_partition_commits():
    c = bare([[
        Record(encode(valid_payload()), 10),
        Record(b"{broken", 11),
        Record(encode(valid_payload()), 12),
        Record(encode(valid_payload()), 20, partition=1),
    ]], dlq_ok=False)
    c.process_message = Mock(return_value=True)
    c.run_once()
    assert c.process_message.call_count == 2
    assert c.kafka_consumer.committed == {
        TopicPartition("seoul-weather", 0): 11,
        TopicPartition("seoul-weather", 1): 21,
    }
    assert c.kafka_consumer.rewound == {TopicPartition("seoul-weather", 0): 11}
    assert [name for name, _ in c.kafka_consumer.calls] == ["commit", "rewind"]


def test_commit_failure_discards_connection_before_next_poll():
    c = bare([[Record(encode(valid_payload()))]])
    c.kafka_consumer.commit = Mock(return_value=False)
    c.run_once()
    assert c.kafka_consumer.reset


def test_commit_failure_with_retry_does_not_rewind_after_connection_reset():
    c = bare([[
        Record(encode(valid_payload()), 10),
        Record(b"{broken", 11),
        Record(encode(valid_payload()), 20, partition=1),
    ]], dlq_ok=False)
    c.kafka_consumer.commit = Mock(return_value=False)
    c.run_once()
    assert c.kafka_consumer.reset
    assert c.kafka_consumer.rewound == {}


def test_wrapper_commits_explicit_offsets():
    wrapper = KafkaWeatherConsumer.__new__(KafkaWeatherConsumer)
    wrapper.consumer = Mock()
    tp = TopicPartition("seoul-weather", 0)
    assert wrapper.commit({tp: 12})
    wrapper.consumer.commit.assert_called_once_with(
        offsets={tp: OffsetAndMetadata(12, "")},
    )


def test_failed_seek_discards_client_without_committing():
    wrapper = KafkaWeatherConsumer.__new__(KafkaWeatherConsumer)
    client = wrapper.consumer = Mock()
    client.seek.side_effect = RuntimeError("partition revoked")
    with pytest.raises(RuntimeError, match="partition revoked"):
        wrapper.rewind({TopicPartition("seoul-weather", 0): 12})
    assert wrapper.consumer is None
    client.close.assert_called_once_with(autocommit=False)
    client.commit.assert_not_called()


def test_recovery_across_polls_and_restart_uses_failed_offset():
    """실제 poll처럼 읽기 위치가 전진하는 모델로 커밋 생략만 하는 회귀를 잡는다."""
    tp = TopicPartition("seoul-weather", 0)
    records = [Record(b"{broken", 0)] + [
        Record(encode(valid_payload(event_id=f"서울:{i}")), i) for i in (1, 2)
    ]

    class PositionedKafka(FakeKafka):
        def __init__(self):
            super().__init__([])
            self.position = 0
        def consume_batch(self, timeout_ms=5000):
            batch = records[self.position:self.position + 2]
            self.position += len(batch)
            return batch
        def rewind(self, offsets):
            super().rewind(offsets)
            self.position = offsets[tp]

    c = bare([], dlq_ok=False)
    c.kafka_consumer = PositionedKafka()
    c.run_once()
    c.run_once()  # DLQ가 계속 실패해도 뒤 정상 메시지를 커밋하지 않는다.
    assert c.kafka_consumer.committed == {}
    assert c.kafka_consumer.position == 0
    c.kafka_consumer.position = c.kafka_consumer.committed.get(tp, 0)  # 재시작
    c._send_to_dlq = Mock(return_value=True)
    c.run_once()
    assert c.kafka_consumer.committed[tp] == 2
    c._send_to_dlq.assert_called_once()
    c.run_once()
    assert c.kafka_consumer.committed[tp] == 3
