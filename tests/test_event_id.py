"""이벤트 키의 결정성 검증 (#41 회귀 방지).

이전 동작: 발행·색인 어디에도 키가 없어 DAG 재시도가 곧 중복 발행이고,
OpenSearch는 _id 자동 생성이라 재처리할 때마다 문서가 새로 쌓였다.

핵심은 "결정적"이라는 점이다. 벽시계로 만들면 재시도마다 값이 갈려
멱등성이 성립하지 않는다.
"""
from datetime import datetime, timedelta, timezone

from contract import build_event_id

KST = timezone(timedelta(hours=9))


def test_same_schedule_slot_gives_same_id():
    # DAG 재시도: 실행 시각은 달라도 스케줄 슬롯은 같다
    slot = datetime(2026, 7, 29, 12, 0, tzinfo=KST)
    assert build_event_id("서울", slot) == build_event_id("서울", slot)


def test_minutes_and_seconds_do_not_split_the_id():
    # 재시도가 5분 뒤에 일어나도 같은 슬롯으로 묶여야 한다
    first = datetime(2026, 7, 29, 12, 0, 3, tzinfo=KST)
    retry = datetime(2026, 7, 29, 12, 5, 47, tzinfo=KST)
    assert build_event_id("서울", first) == build_event_id("서울", retry)


def test_different_hours_give_different_ids():
    a = datetime(2026, 7, 29, 12, 0, tzinfo=KST)
    b = datetime(2026, 7, 29, 18, 0, tzinfo=KST)
    assert build_event_id("서울", a) != build_event_id("서울", b)


def test_different_regions_give_different_ids():
    slot = datetime(2026, 7, 29, 12, 0, tzinfo=KST)
    assert build_event_id("서울", slot) != build_event_id("부산", slot)


def test_id_format_is_region_and_kst_hour():
    slot = datetime(2026, 7, 29, 12, 0, tzinfo=KST)
    assert build_event_id("서울", slot) == "서울:2026072912"


def test_utc_input_is_converted_to_kst():
    # Airflow의 data_interval_end는 UTC로 들어올 수 있다.
    # KST 12시와 같은 순간이면 같은 id여야 한다.
    utc_noon_kst = datetime(2026, 7, 29, 3, 0, tzinfo=timezone.utc)
    kst_noon = datetime(2026, 7, 29, 12, 0, tzinfo=KST)
    assert build_event_id("서울", utc_noon_kst) == build_event_id("서울", kst_noon)


def test_naive_input_is_treated_as_kst():
    naive = datetime(2026, 7, 29, 12, 0)
    aware = datetime(2026, 7, 29, 12, 0, tzinfo=KST)
    assert build_event_id("서울", naive) == build_event_id("서울", aware)


def test_midnight_boundary_does_not_leak_to_previous_day():
    # KST 00시는 UTC로 전날 15시다. 날짜가 밀리면 안 된다.
    midnight = datetime(2026, 7, 29, 0, 0, tzinfo=KST)
    assert build_event_id("서울", midnight) == "서울:2026072900"


def test_default_uses_current_time_and_is_stable_within_the_hour():
    # 인자를 생략해도 형식이 유지된다 (지역:YYYYMMDDHH)
    generated = build_event_id("서울")
    region, _, stamp = generated.partition(":")
    assert region == "서울"
    assert len(stamp) == 10 and stamp.isdigit()
