"""KST 고정 시각 유틸 검증 (#39 회귀 방지).

이전 동작: 전 구간이 naive `datetime.now()`였다. 오프셋 없는 ISO를
OpenSearch가 UTC로 해석해 이력이 9시간 밀렸고, Airflow 컨테이너만
TZ=Asia/Seoul이라 같은 스택 안에서 프로세스마다 "지금"이 달랐다.
AWS(Lambda는 UTC 기본)로 옮기면 오늘 예보 필터가 통째로 비는 구조였다.
"""
import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from timeutil import KST, kst_isoformat, now_kst, to_kst

UTC = timezone.utc


# ---------- now_kst ----------

def test_now_is_offset_aware():
    assert now_kst().tzinfo is not None
    assert now_kst().utcoffset() == timedelta(hours=9)


def test_isoformat_carries_offset():
    # 오프셋이 없으면 OpenSearch가 UTC로 읽는다 — 이게 9시간 오차의 원인이었다
    stamp = kst_isoformat()
    assert stamp.endswith("+09:00")
    assert datetime.fromisoformat(stamp).utcoffset() == timedelta(hours=9)


@pytest.mark.parametrize("tz", ["UTC", "America/New_York", "Asia/Seoul"])
def test_result_does_not_depend_on_container_tz(tz):
    """컨테이너 TZ가 무엇이든 같은 순간을 가리켜야 한다."""
    original = os.environ.get("TZ")
    try:
        os.environ["TZ"] = tz
        if hasattr(time, "tzset"):
            time.tzset()
        assert now_kst().utcoffset() == timedelta(hours=9)
        # naive now()와 달리 실제 순간이 흔들리지 않는다
        assert abs((now_kst() - datetime.now(UTC)).total_seconds()) < 5
    finally:
        if original is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original
        if hasattr(time, "tzset"):
            time.tzset()


# ---------- to_kst ----------

def test_utc_input_converts():
    utc_midnight = datetime(2026, 7, 29, 15, 0, tzinfo=UTC)
    converted = to_kst(utc_midnight)
    # UTC 15시 = KST 다음날 00시
    assert (converted.year, converted.month, converted.day, converted.hour) == (2026, 7, 30, 0)


def test_naive_input_is_treated_as_kst():
    naive = datetime(2026, 7, 29, 12, 0)
    assert to_kst(naive).utcoffset() == timedelta(hours=9)
    assert to_kst(naive).hour == 12


def test_kst_input_is_unchanged():
    aware = datetime(2026, 7, 29, 12, 0, tzinfo=KST)
    assert to_kst(aware) == aware


def test_conversion_preserves_the_instant():
    utc = datetime(2026, 7, 29, 3, 0, tzinfo=UTC)
    assert to_kst(utc).timestamp() == utc.timestamp()


def test_aware_and_naive_can_be_compared_after_conversion():
    """API 발표시각(strptime → naive)과 now를 비교하던 지점의 회귀 방지.

    to_kst를 거치지 않으면 TypeError: can't compare offset-naive and
    offset-aware datetimes 가 난다 — 자외선지수 선택 로직이 여기서 깨졌다.
    """
    base = to_kst(datetime.strptime("2026072906", "%Y%m%d%H"))
    assert base <= now_kst() or base > now_kst()  # 예외 없이 비교되면 통과
