"""결측값이 실측값으로 둔갑하지 않는지 검증 (#33 회귀 방지).

이전 동작: 컨슈머의 _safe_float 기본값이 0이라 API 실패 시
  - feels_like_temp None -> 0.0 -> "매우나쁨 / 동상 위험" (한여름 오탐)
  - pm10 None -> 0.0 -> "좋음 / 외출 자유" (거짓 안전 신호)
현재 동작: None은 UNKNOWN("정보없음")으로 판정되고 행동 그룹을 활성화하지 않는다.
"""
import rules
from rules import (
    AlertRuleEngine as E,
    AlertGrouping as G,
    AlertLevel as L,
    core_indices_unknown,
    grade_signature,
)


# ---------- 결측 -> UNKNOWN ----------

def test_feels_like_none_is_unknown_not_frostbite():
    # 회귀의 핵심: 결측 체감온도가 max_value=0 규칙에 걸려 VERY_BAD가 되던 버그
    level, rec, _ = E.classify_feels_like_temp(None)
    assert level is L.UNKNOWN
    assert "동상" not in rec


def test_pm10_none_is_unknown_not_good():
    # 반대 방향 회귀: 결측이 "좋음 / 외출 자유"라는 거짓 안전 신호가 되던 버그
    level, rec, _ = E.classify_pm10(None)
    assert level is L.UNKNOWN
    assert "외출 자유" not in rec


def test_all_numeric_classifiers_handle_none():
    for classify in (
        E.classify_pm10, E.classify_pm25, E.classify_uv_index,
        E.classify_dust, E.classify_pollen,
        E.classify_precipitation_probability, E.classify_feels_like_temp,
    ):
        assert classify(None)[0] is L.UNKNOWN, classify.__name__


def test_real_zero_is_still_classified():
    # 결측과 실측 0을 구분한다 — 0은 여전히 규칙을 탄다
    assert E.classify_pm10(0)[0] is L.GOOD
    assert E.classify_feels_like_temp(0)[0] is L.VERY_BAD


# ---------- 특보: 없음 vs 정보없음 ----------

def test_special_notice_unknown_is_not_good():
    # 프로듀서는 수집 실패를 "정보없음", 특보 없음을 "없음"으로 구분해 보낸다
    assert E.classify_special_notice("정보없음")[0] is L.UNKNOWN
    assert E.classify_special_notice(None)[0] is L.UNKNOWN
    assert E.classify_special_notice("")[0] is L.UNKNOWN


def test_special_notice_confirmed_none_is_good():
    assert E.classify_special_notice("없음")[0] is L.GOOD


def test_special_notice_signal_is_bad():
    assert E.classify_special_notice("강풍 가능")[0] is L.BAD


# ---------- 행동 그룹 ----------

def test_unknown_does_not_activate_action_group():
    groups = G.group_alerts({"pm10": L.UNKNOWN, "uv_index": L.UNKNOWN})
    assert "마스크_필수" not in groups
    assert "자외선_차단" not in groups


def test_unknown_present_reports_shortage_not_normal():
    # 결측이 섞여 있으면 "모든 지수가 정상범위"라고 말하지 않는다
    groups = G.group_alerts({"pm10": L.GOOD, "feels_like_temp": L.UNKNOWN})
    assert list(groups.keys()) == ["정보부족"]
    assert "feels_like_temp" in groups["정보부족"]["reasons"][0]


def test_no_unknown_still_reports_normal():
    groups = G.group_alerts({"pm10": L.GOOD, "pm25": L.GOOD})
    assert list(groups.keys()) == ["정상"]


# ---------- 핵심 지수 결측 게이트 ----------

def test_core_indices_unknown_when_both_missing():
    assert core_indices_unknown({"pm10": L.UNKNOWN, "pm25": L.UNKNOWN}) is True


def test_core_indices_not_unknown_when_one_present():
    assert core_indices_unknown({"pm10": L.UNKNOWN, "pm25": L.GOOD}) is False


def test_core_indices_unknown_when_keys_absent():
    # 키 자체가 없는 경우도 근거 없음으로 본다
    assert core_indices_unknown({}) is True


# ---------- 시그니처 ----------

def test_unknown_changes_signature():
    # 결측 발생/복구는 상태 변화로 잡혀야 한다 (쿨다운이 삼키지 않도록)
    known = {"pm10": L.GOOD}
    unknown = {"pm10": L.UNKNOWN}
    assert grade_signature(known) != grade_signature(unknown)


def test_unknown_level_value_is_readable():
    assert L.UNKNOWN.value == "정보없음"
    assert rules.UNKNOWN_RESULT[0] is L.UNKNOWN
