"""consumer/rules.py 순수 함수 경계값 테스트.

외부 의존성(kafka/opensearch) 없이 도는 판정 로직만 검증한다.
값은 실제 규칙표에서 확인한 경계다.
"""
import rules
from rules import AlertRuleEngine as E, AlertGrouping as G, AlertLevel as L


# ---------- _classify 경계 (value <= max_value, 오름차순 첫 매칭) ----------

def test_pm10_boundaries():
    assert E.classify_pm10(30)[0] is L.GOOD      # 경계 포함
    assert E.classify_pm10(31)[0] is L.NORMAL
    assert E.classify_pm10(80)[0] is L.NORMAL
    assert E.classify_pm10(81)[0] is L.BAD
    assert E.classify_pm10(150)[0] is L.BAD
    assert E.classify_pm10(151)[0] is L.VERY_BAD


def test_over_range_falls_to_last_rule():
    # 최대 규칙(max_value=500)을 넘어도 마지막 규칙(VERY_BAD) 적용
    assert E.classify_pm10(9999)[0] is L.VERY_BAD


def test_uv_boundaries():
    assert E.classify_uv_index(2)[0] is L.GOOD
    assert E.classify_uv_index(3)[0] is L.NORMAL
    assert E.classify_uv_index(6)[0] is L.BAD
    assert E.classify_uv_index(8)[0] is L.VERY_BAD


def test_special_notice_string_logic():
    # "없음"(특보 없음 확인)과 "정보없음"(수집 실패)은 다른 상태다 — #33
    assert E.classify_special_notice("없음")[0] is L.GOOD
    assert E.classify_special_notice("정보없음")[0] is L.UNKNOWN
    assert E.classify_special_notice("")[0] is L.UNKNOWN
    assert E.classify_special_notice("강풍")[0] is L.BAD


def test_classify_returns_triple():
    level, rec, emoji = E.classify_pm10(120)
    assert isinstance(level, L)
    assert isinstance(rec, str) and rec
    assert isinstance(emoji, str) and emoji


# ---------- group_alerts 행동 그룹 ----------

def test_group_all_normal_returns_jeongsang():
    groups = G.group_alerts({"pm10": L.GOOD, "pm25": L.GOOD, "uv_index": L.GOOD})
    assert list(groups.keys()) == ["정상"]


def test_group_pm10_bad_activates_mask():
    assert "마스크_필수" in G.group_alerts({"pm10": L.BAD})


def test_group_uv_bad_activates_sunscreen():
    assert "자외선_차단" in G.group_alerts({"uv_index": L.VERY_BAD})


def test_group_precip_bad_activates_umbrella():
    assert "우산_준비" in G.group_alerts({"precipitation_probability": L.BAD})


def test_group_missing_keys_are_safe():
    # 키가 없어도 .get()→None 가드로 예외 없이 정상 처리 (라이브 경로 불변성)
    groups = G.group_alerts({})
    assert groups == {"정상": groups["정상"]}
    assert "reasons" in groups["정상"]


def test_removed_dead_rules_are_gone():
    # 죽은 코드 청소(#11) 회귀 방지: 제거한 규칙표/판정이 되살아나지 않도록
    assert not hasattr(rules.WeatherIndexRules, "OZONE_RULES")
    assert not hasattr(rules.WeatherIndexRules, "COLD_RISK_RULES")
    assert not hasattr(rules.WeatherIndexRules, "DISCOMFORT_RULES")
    assert not hasattr(E, "classify_ozone")
    assert not hasattr(E, "classify_cold_risk")
    assert not hasattr(E, "classify_discomfort")
