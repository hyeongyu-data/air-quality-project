"""황사 지표의 의미 검증 (#44 회귀 방지).

이전 동작: 황사 발생정보 API 권한이 없으면 PM10 평균을 황사 값으로 복사했다.
그런데 황사 판정의 "좋음" 임계가 150㎍/㎥라, 서울 PM10 평균으로는 사실상
항상 "좋음"이 나왔다. 감시되는 것처럼 보이지만 아무것도 감시하지 않는 지표였다.
"""
import pytest

from rules import AlertRuleEngine as E, AlertLevel as L


# ---------- 프록시가 왜 무의미했는지 ----------

@pytest.mark.parametrize("pm10_mean", [0, 10, 30, 45, 80, 120, 150])
def test_pm10_range_always_maps_to_good_dust(pm10_mean):
    """서울 PM10 평균이 실제로 가질 수 있는 범위는 전부 '좋음'으로 떨어진다.

    PM10 150은 미세먼지 기준으로는 이미 '나쁨'인데(rules.PM10_RULES),
    황사 기준으로는 여전히 '좋음'이다. 두 지표의 척도가 다르기 때문이고,
    이것이 프록시를 값으로 쓰면 안 되는 이유다.
    """
    assert E.classify_dust(pm10_mean)[0] is L.GOOD
    # 같은 값이 미세먼지 기준으로는 좋음이 아닐 수 있다
    if pm10_mean > 30:
        assert E.classify_pm10(pm10_mean)[0] is not L.GOOD


# ---------- 발생정보 API의 플래그 값 ----------

def test_advisory_issued_is_bad():
    # get_yellow_dust_advisory는 발생 시 500.0을 내보낸다
    assert E.classify_dust(500.0)[0] is L.BAD


def test_advisory_clear_is_good():
    # 발생 없음은 0.0
    assert E.classify_dust(0.0)[0] is L.GOOD


# ---------- 권한이 없으면 결측 ----------

def test_unavailable_is_unknown_not_good():
    """대체값을 만들지 않는다. 모른다를 괜찮다로 바꾸지 않는다."""
    level, rec, _ = E.classify_dust(None)
    assert level is L.UNKNOWN
    assert "외출 자유" not in rec


def test_unknown_dust_does_not_activate_mask_group():
    from rules import AlertGrouping as G
    groups = G.group_alerts({"dust": L.UNKNOWN, "pm10": L.GOOD, "pm25": L.GOOD})
    assert "마스크_필수" not in groups


def test_issued_dust_activates_mask_group():
    from rules import AlertGrouping as G
    groups = G.group_alerts({"dust": L.BAD, "pm10": L.GOOD, "pm25": L.GOOD})
    assert "마스크_필수" in groups
