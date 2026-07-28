"""발송 실패가 쿨다운을 전진시키지 않는지 검증 (#34 회귀 방지).

이전 동작: send_all()의 결과와 무관하게 grade_signature가 저장돼,
SMTP·카카오·Slack이 전부 실패한 순간의 등급이 그대로 굳었다. 등급이
유지되는 동안 알림이 영구히 사라지고, 채널이 복구돼도 재발송이 없었다.
"""
from rules import should_record_signature


# ---------- 시도했고 전부 실패 -> 기록하지 않는다 ----------

def test_all_external_failed_does_not_record():
    assert should_record_signature(
        send_external=True, external_enabled=True, delivered_channels=[]
    ) is False


def test_partial_success_records():
    # 하나라도 전달됐으면 그 등급은 알려진 것으로 본다
    assert should_record_signature(
        send_external=True, external_enabled=True, delivered_channels=["email"]
    ) is True


# ---------- 시도하지 않았으면 상태 유지 ----------

def test_cooldown_suppressed_keeps_state():
    # 등급 무변경으로 생략한 경우, 저장된 시그니처는 이미 같은 값이다
    assert should_record_signature(
        send_external=False, external_enabled=True, delivered_channels=[]
    ) is True


# ---------- 활성 채널이 없으면 전달할 대상이 없다 ----------

def test_no_enabled_channel_records():
    # 전 채널 비활성(콘솔·OpenSearch만 쓰는 구성)에서 쿨다운이 깨지지 않아야 한다
    assert should_record_signature(
        send_external=True, external_enabled=False, delivered_channels=[]
    ) is True


# ---------- 복구 시나리오 ----------

def test_failure_then_recovery_resends():
    """1회차 전량 실패 -> 미기록 -> 2회차에 같은 등급이어도 재발송된다."""
    from rules import should_send

    signature = "pm10=매우나쁨"

    # 1회차: 이전 상태 없음 -> 발송 시도 -> 전량 실패 -> 기록 안 함
    assert should_send(None, signature) is True
    recorded_1 = should_record_signature(True, True, [])
    assert recorded_1 is False
    stored = signature if recorded_1 else ""

    # 2회차: 저장된 시그니처가 비어 있으므로 등급이 같아도 다시 발송한다
    assert should_send(stored or None, signature) is True
    recorded_2 = should_record_signature(True, True, ["kakao"])
    assert recorded_2 is True
    stored = signature if recorded_2 else ""

    # 3회차: 전달에 성공했으므로 같은 등급은 이제 생략된다
    assert should_send(stored, signature) is False
