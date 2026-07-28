"""수집 결과 발행 계약 검증 (#35 회귀 방지).

이전 동작: 수집이 실패해도 예외 없이 반환돼 태스크가 성공으로 끝나고,
다음 태스크가 빈 페이로드를 발행하거나 0건 발행 후에도 success를 반환했다.
DAG는 종일 초록색인데 알림은 하나도 나가지 않는 상태가 가능했다.
"""
from contract import collected_index_count, is_publishable, missing_index_keys

FULL = {
    "oak_pollen": 1, "pine_pollen": 1, "pm10": 42.0, "pm25": 18.0,
    "yellow_dust": 42.0, "feels_like_temp": 27.5,
    "precipitation_probability": 30.0, "uv_index": 6.0,
    "other_special_notice": "없음",
}


# ---------- 개수 세기 ----------

def test_full_payload_counts_all():
    assert collected_index_count(FULL) == 9


def test_none_values_are_not_counted():
    data = dict(FULL, pm10=None, pm25=None)
    assert collected_index_count(data) == 7


def test_unknown_notice_is_not_counted():
    # "없음"은 특보 없음 확인, "정보없음"은 수집 실패 — 후자는 세지 않는다
    assert collected_index_count(dict(FULL, other_special_notice="정보없음")) == 8
    assert collected_index_count(dict(FULL, other_special_notice=None)) == 8


def test_signal_notice_is_counted():
    assert collected_index_count(dict(FULL, other_special_notice="강풍 가능")) == 9


def test_empty_and_none_payloads():
    assert collected_index_count({}) == 0
    assert collected_index_count(None) == 0


# ---------- 발행 가능 판단 ----------

def test_total_failure_is_not_publishable():
    empty = {key: None for key in FULL}
    empty["other_special_notice"] = "정보없음"
    assert is_publishable(empty) is False
    assert is_publishable({}) is False
    assert is_publishable(None) is False


def test_partial_failure_is_still_publishable():
    # 에어코리아만 죽은 상황 — 살아 있는 지수는 알릴 가치가 있다.
    # 결측은 컨슈머가 UNKNOWN으로 처리하고 핵심 지수 전량 결측이면 발송을 보류한다.
    partial = dict(FULL, pm10=None, pm25=None, yellow_dust=None)
    assert is_publishable(partial) is True


def test_single_index_is_publishable():
    only_uv = {key: None for key in FULL}
    only_uv["other_special_notice"] = "정보없음"
    only_uv["uv_index"] = 6.0
    assert is_publishable(only_uv) is True


def test_minimum_is_configurable():
    partial = dict(FULL, pm10=None, pm25=None, yellow_dust=None)
    assert is_publishable(partial, minimum=6) is True
    assert is_publishable(partial, minimum=7) is False


# ---------- 결측 목록 ----------

def test_missing_keys_listed_for_alerting():
    data = dict(FULL, pm10=None, other_special_notice="정보없음")
    missing = missing_index_keys(data)
    assert missing == ["pm10", "other_special_notice"]


def test_no_missing_keys_when_full():
    assert missing_index_keys(FULL) == []


def test_all_keys_missing_for_empty_payload():
    assert "pm10" in missing_index_keys({})
    assert "other_special_notice" in missing_index_keys(None)
