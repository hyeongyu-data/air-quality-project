"""알림 쿨다운/중복제거 순수 함수 경계 테스트.

grade_signature/should_send는 외부 의존성(kafka/opensearch) 없이 도는
발송 게이트의 핵심이다. 등급 무변경 시 외부 발송을 생략하는 판정을 검증한다.
"""
from rules import grade_signature, should_send, AlertLevel as L


# ---------- grade_signature ----------

def test_same_levels_same_signature():
    a = {"pm10": L.BAD, "pm25": L.GOOD, "uv_index": L.NORMAL}
    b = {"pm10": L.BAD, "pm25": L.GOOD, "uv_index": L.NORMAL}
    assert grade_signature(a) == grade_signature(b)


def test_key_order_independent():
    a = {"pm10": L.BAD, "pm25": L.GOOD}
    b = {"pm25": L.GOOD, "pm10": L.BAD}
    assert grade_signature(a) == grade_signature(b)


def test_level_change_changes_signature():
    a = {"pm10": L.BAD}
    b = {"pm10": L.VERY_BAD}
    assert grade_signature(a) != grade_signature(b)


def test_empty_signature_is_empty_string():
    assert grade_signature({}) == ""


# ---------- should_send ----------

def test_first_time_none_sends():
    assert should_send(None, "pm10=나쁨") is True


def test_same_signature_skips():
    assert should_send("pm10=나쁨", "pm10=나쁨") is False


def test_changed_signature_sends():
    assert should_send("pm10=나쁨", "pm10=매우나쁨") is True
