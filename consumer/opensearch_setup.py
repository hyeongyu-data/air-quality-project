"""OpenSearch 인덱스 템플릿·보존 정책 부트스트랩.

컨슈머가 연결에 성공했을 때 한 번 적용한다. 별도 관리 스크립트를 두면
"적용하는 것을 잊는" 경로가 생기고, 로컬 개발자마다 클러스터 상태가 달라진다.
PUT은 멱등하므로 매 기동마다 호출해도 안전하다.

해결하는 문제:
- 동적 매핑에 맡기면 인덱스마다 필드 타입이 갈린다. 결측이 섞인 첫 문서가
  int로 들어오면 그 인덱스의 해당 필드는 long이 되고 이후 45.6이 45로 절삭된다.
- 단일 노드인데 기본 replica가 1이라 샤드가 영구 미할당이고 클러스터가 상시
  yellow다. 진짜 문제와 구분되지 않는다.
- 보존 정책이 없어 인덱스가 무한 증가한다.
"""

import logging

logger = logging.getLogger(__name__)

TEMPLATE_NAME = "weather-alert-template"
ISM_POLICY_ID = "weather-alert-retention"

# 인덱스는 월 단위다. 일 단위면 1년에 365개 인덱스(=365 샤드)가 쌓이는데,
# 이 클러스터의 힙으로는 감당할 이유가 없는 숫자다. 조회는 weather-alert-*
# 와일드카드라 단위가 바뀌어도 그대로 동작한다.
INDEX_PATTERN = "weather-alert-*"

_NUMERIC_INDEX_FIELDS = (
    "oak_pollen", "pine_pollen", "pm10", "pm25", "dust",
    "feels_like_temp", "precipitation_probability", "uv_index",
)

INDEX_TEMPLATE = {
    "index_patterns": [INDEX_PATTERN],
    "template": {
        "settings": {
            # 단일 노드에서 replica 1은 영구 미할당이다. 0으로 두면 클러스터가
            # green이 되고, red/yellow가 실제 문제를 뜻하게 된다.
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s",
        },
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                # region/event_id/grade_signature는 정확 일치로만 쓴다.
                # text로 두면 분석기를 타서 match 쿼리가 오매칭한다.
                "region": {"type": "keyword"},
                "event_id": {"type": "keyword"},
                "grade_signature": {"type": "keyword"},
                "alert_severity": {"type": "keyword"},
                "action_groups": {"type": "keyword"},
                "delivered_channels": {"type": "keyword"},
                "signature_recorded": {"type": "boolean"},
                "indices": {
                    "properties": {
                        **{f: {"type": "float"} for f in _NUMERIC_INDEX_FIELDS},
                        "other_special_notice": {"type": "keyword"},
                    }
                },
                # 등급/권고 문자열은 집계 대상이라 keyword가 맞다.
                "levels": {"type": "object", "dynamic": True},
                "recommendations": {"type": "object", "enabled": False},
            }
        },
    },
    "priority": 100,
}

# 월 단위 인덱스라 삭제 기준도 월 단위로 잡는다. 30일로 두면 이번 달 인덱스가
# 생성 30일 뒤에 사라져 사용 중인 데이터를 지운다.
ISM_POLICY = {
    "policy": {
        "description": "weather-alert 인덱스를 90일 후 삭제",
        "default_state": "hot",
        "states": [
            {
                "name": "hot",
                "actions": [],
                "transitions": [
                    {"state_name": "delete", "conditions": {"min_index_age": "90d"}}
                ],
            },
            {"name": "delete", "actions": [{"delete": {}}], "transitions": []},
        ],
        "ism_template": [{"index_patterns": [INDEX_PATTERN], "priority": 100}],
    }
}


# 쿨다운 상태 전용 인덱스. weather-alert-* 패턴에 넣으면 ISM 90일 삭제와
# 이력용 매핑이 함께 걸리므로 이름을 분리한다. 지역당 문서 1개라 크기가 없다.
STATE_INDEX = "weather-cooldown-state"

STATE_TEMPLATE = {
    "index_patterns": [STATE_INDEX],
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "region": {"type": "keyword"},
                "grade_signature": {"type": "keyword"},
                "last_external_send_at": {"type": "date"},
                "updated_at": {"type": "date"},
            }
        },
    },
    "priority": 100,
}


def ensure_index_template(client) -> bool:
    """인덱스 템플릿을 적용한다. 실패해도 알림 처리를 막지 않는다."""
    try:
        client.indices.put_index_template(name=TEMPLATE_NAME, body=INDEX_TEMPLATE)
        client.indices.put_index_template(
            name=f"{STATE_INDEX}-template", body=STATE_TEMPLATE
        )
        logger.info(f"OpenSearch 인덱스 템플릿 적용: {TEMPLATE_NAME}, {STATE_INDEX}")
        return True
    except Exception as e:
        logger.warning(f"인덱스 템플릿 적용 실패(동적 매핑으로 진행): {str(e)}")
        return False


def ensure_ism_policy(client) -> bool:
    """보존 정책을 적용한다. ISM 플러그인이 없으면 건너뛴다."""
    try:
        client.transport.perform_request(
            "PUT", f"/_plugins/_ism/policies/{ISM_POLICY_ID}", body=ISM_POLICY
        )
        logger.info(f"OpenSearch 보존 정책 적용: {ISM_POLICY_ID} (90일)")
        return True
    except Exception as e:
        # 이미 존재하면 409가 온다. 정책 갱신은 seq_no가 필요해 여기서는 다루지 않는다.
        message = str(e)
        if "version_conflict" in message or "409" in message:
            logger.debug(f"보존 정책이 이미 존재합니다: {ISM_POLICY_ID}")
            return True
        logger.warning(f"보존 정책 적용 실패(보존 없이 진행): {message}")
        return False


def bootstrap(client) -> None:
    """연결 직후 한 번 호출한다."""
    if client is None:
        return
    ensure_index_template(client)
    ensure_ism_policy(client)
