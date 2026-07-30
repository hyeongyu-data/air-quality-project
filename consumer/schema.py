"""Kafka 메시지 수신 계약.

프로듀서와 컨슈머가 dict 키로만 느슨하게 이어져 있어 필드 하나를 지워도
아무도 몰랐다. 여기서 수신 메시지를 검증하고, 통과하지 못한 메시지는
DLQ로 보낸다(계약 위반 메시지를 무한 재시도하지 않기 위해).

producer/contract.py의 SCHEMA_VERSION과 짝이다 — 두 상수가 같은지
테스트(test_message_schema.py)가 고정한다. Consumer 이미지는 consumer/만
포함하므로 producer 모듈을 import 할 수 없어 상수를 복제한다.
"""

import json
from typing import Dict

# 프로듀서(producer/contract.py)와 함께 올린다.
SCHEMA_VERSION = 1

# 이 키들이 없으면 판정·멱등 저장이 성립하지 않는다.
REQUIRED_KEYS = ("event_id", "timestamp", "region")


class InvalidMessage(Exception):
    """수신 메시지가 계약을 위반했다. DLQ 사유로 그대로 쓴다."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def parse_message(raw: bytes) -> Dict:
    """raw 바이트를 검증된 dict로 변환한다.

    역직렬화를 KafkaConsumer의 value_deserializer에 두면 깨진 메시지가
    poll() 안에서 터진다. 자동 커밋을 끈 상태에서는 오프셋이 전진하지 않아
    같은 메시지를 영원히 다시 받는 poison pill이 된다. 파싱을 처리 단계로
    옮겨야 실패를 DLQ로 격리하고 오프셋을 전진시킬 수 있다.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise InvalidMessage(f"JSON 파싱 실패: {e}") from e

    if not isinstance(data, dict):
        raise InvalidMessage(f"객체가 아닌 페이로드: {type(data).__name__}")

    version = data.get("schema_version")
    # 버전 없는 메시지는 스키마 도입 전 백로그(이행 경로)로 허용한다.
    if version is not None and version != SCHEMA_VERSION:
        raise InvalidMessage(f"지원하지 않는 schema_version: {version}")

    missing = [key for key in REQUIRED_KEYS if not data.get(key)]
    if missing:
        raise InvalidMessage(f"필수 키 누락: {missing}")

    # Python 3.10의 fromisoformat은 'Z' 접미사를 못 읽는다. 하류(alert.py의
    # 인덱스 날짜·Slack ts)가 조용히 실패하지 않도록 입구에서 정규화한다.
    timestamp = data["timestamp"]
    if isinstance(timestamp, str) and timestamp.endswith("Z"):
        data["timestamp"] = timestamp[:-1] + "+00:00"

    return data
