"""OpenSearch 미가용 시 쿨다운 유지와 인덱스 템플릿 검증 (#37 회귀 방지).

이전 동작: 기동 시 연결에 실패하면 client=None으로 고정되고 재연결이 없었다.
그 상태에서 latest_signature가 항상 None을 돌려줘 쿨다운이 무력화되고,
매 메시지가 전 채널로 발송됐다. 이력도 한 건도 남지 않았다.
"""
import json

import pytest

pytest.importorskip("requests", reason="alert 모듈이 requests에 의존")
pytest.importorskip("dotenv", reason="alert 모듈이 python-dotenv에 의존")

import opensearch_setup  # noqa: E402
from alert import OpenSearchAlertSender  # noqa: E402


class FakeClient:
    """index(id=...)를 dict upsert로 흉내내는 최소 클라이언트."""

    def __init__(self):
        self.store = {}

    def index(self, index, body, id=None):
        self.store[(index, id)] = body
        return {"_id": id or "auto"}


@pytest.fixture
def sender(tmp_path, monkeypatch):
    monkeypatch.setenv("SIGNATURE_STATE_PATH", str(tmp_path / "state" / "sig.json"))
    monkeypatch.setenv("OPENSEARCH_INDEX_PREFIX", "weather-alert")
    return OpenSearchAlertSender(FakeClient())


DOC = {
    "timestamp": "2026-07-29T12:00:00+09:00",
    "region": "서울",
    "indices": {}, "levels": {}, "recommendations": {},
    "action_groups": {}, "grade_signature": "pm10=나쁨",
    "event_id": "서울:2026072912",
}


# ---------- 인덱스는 월 단위 ----------

def test_index_name_is_monthly(sender):
    assert sender.send(dict(DOC)) is True
    (index_name, doc_id), _ = next(iter(sender.client.store.items()))
    # 일 단위면 1년에 365개 인덱스가 쌓인다
    assert index_name == "weather-alert-2026.07"
    assert doc_id == "서울:2026072912"


# ---------- 로컬 캐시 ----------

def test_successful_send_caches_signature(sender):
    sender.send(dict(DOC))
    cache = json.loads(sender.state_path.read_text(encoding="utf-8"))
    # #42에서 캐시가 dict 형식으로 확장됐다(발송 시각 동반). 옛 str 형식은
    # 읽기 호환만 유지한다 — test_cooldown_state.py 참고.
    assert cache["서울"]["grade_signature"] == "pm10=나쁨"


def test_unrecorded_signature_is_not_cached(sender):
    """외부 발송 전량 실패 시(#34) 캐시도 전진하면 안 된다."""
    sender.send(dict(DOC), record_signature=False)
    assert not sender.state_path.exists()


def test_cache_survives_disconnection(sender):
    sender.send(dict(DOC))
    # OpenSearch가 죽은 상태를 재현
    sender.attach(None)
    assert sender.enabled is False
    # 이전에는 None을 돌려줘 쿨다운이 무력화됐다
    assert sender.latest_signature("서울") == "pm10=나쁨"


def test_unknown_region_returns_none(sender):
    sender.send(dict(DOC))
    sender.attach(None)
    assert sender.latest_signature("부산") is None


def test_corrupted_cache_does_not_raise(sender):
    sender.state_path.parent.mkdir(parents=True, exist_ok=True)
    sender.state_path.write_text("{깨진", encoding="utf-8")
    sender.attach(None)
    assert sender.latest_signature("서울") is None


def test_attach_toggles_enabled(sender):
    sender.attach(None)
    assert sender.enabled is False
    sender.attach(FakeClient())
    assert sender.enabled is True


# ---------- 인덱스 템플릿 ----------

def test_template_disables_replicas():
    """단일 노드에서 replica 1은 영구 미할당이라 클러스터가 상시 yellow가 된다."""
    settings = opensearch_setup.INDEX_TEMPLATE["template"]["settings"]
    assert settings["number_of_replicas"] == 0
    assert settings["number_of_shards"] == 1


def test_template_pins_lookup_fields_as_keyword():
    """region을 text로 두면 match 쿼리가 분석기를 타서 오매칭한다."""
    props = opensearch_setup.INDEX_TEMPLATE["template"]["mappings"]["properties"]
    for field in ("region", "event_id", "grade_signature"):
        assert props[field]["type"] == "keyword", field


def test_template_pins_numeric_indices_as_float():
    """동적 매핑이면 첫 문서가 정수일 때 long이 잡혀 45.6이 45로 절삭된다."""
    numeric = opensearch_setup.INDEX_TEMPLATE["template"]["mappings"]["properties"]["indices"]["properties"]
    for field in ("pm10", "pm25", "uv_index", "feels_like_temp"):
        assert numeric[field]["type"] == "float", field
    assert numeric["other_special_notice"]["type"] == "keyword"


def test_retention_policy_deletes_after_90_days():
    """월 단위 인덱스라 30일로 두면 사용 중인 이번 달 인덱스를 지운다."""
    states = {s["name"]: s for s in opensearch_setup.ISM_POLICY["policy"]["states"]}
    assert states["hot"]["transitions"][0]["conditions"]["min_index_age"] == "90d"
    assert states["delete"]["actions"] == [{"delete": {}}]


def test_bootstrap_is_safe_without_client():
    opensearch_setup.bootstrap(None)  # 예외 없이 통과해야 한다
