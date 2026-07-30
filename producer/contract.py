"""수집 결과가 발행할 만한지 판단하는 계약.

외부 의존성이 없다. DAG(airflow)와 테스트가 함께 import 하기 위해서다.
공공 API가 스키마나 발표시각 규약을 바꾸면 파서가 빈 리스트를 반환하고
상위가 `or {}`로 흡수해, 전 항목이 결측인 페이로드가 조용히 발행된다.
그 상태를 "수집 성공"으로 부르지 않기 위한 최소 계약이다.
"""

from datetime import datetime
from typing import Dict, Optional

try:
    from .timeutil import now_kst, to_kst
except ImportError:  # 직접 실행 / pythonpath 경유
    from timeutil import now_kst, to_kst

# 알림 판정에 실제로 쓰이는 수치 지수
NUMERIC_INDEX_KEYS = (
    "oak_pollen",
    "pine_pollen",
    "pm10",
    "pm25",
    "yellow_dust",
    "feels_like_temp",
    "precipitation_probability",
    "uv_index",
)

# 프로듀서는 특보 없음을 "없음", 수집 실패를 "정보없음"으로 구분해 보낸다.
UNKNOWN_NOTICE_VALUES = (None, "", "정보없음")


def collected_index_count(data: Dict) -> int:
    """실제로 값을 받아온 지수의 개수."""
    if not data:
        return 0
    count = sum(1 for key in NUMERIC_INDEX_KEYS if data.get(key) is not None)
    if data.get("other_special_notice") not in UNKNOWN_NOTICE_VALUES:
        count += 1
    return count


def is_publishable(data: Dict, minimum: int = 1) -> bool:
    """발행할 가치가 있는 수집 결과인지 판단.

    기준을 1로 둔 이유: 일부 API만 죽는 부분 실패는 흔하고, 그때도 살아 있는
    지수는 알릴 가치가 있다. 결측 지수는 컨슈머가 UNKNOWN으로 처리하고
    핵심 지수가 전부 없으면 외부 발송을 보류한다. 반면 하나도 못 받은 결과는
    발행해도 알릴 것이 없으므로, 성공으로 보고하지 않고 태스크를 실패시킨다.
    """
    return collected_index_count(data) >= minimum


def missing_index_keys(data: Dict) -> list:
    """결측 지수 이름 목록 — 로그·경보용."""
    if not data:
        return list(NUMERIC_INDEX_KEYS) + ["other_special_notice"]
    missing = [key for key in NUMERIC_INDEX_KEYS if data.get(key) is None]
    if data.get("other_special_notice") in UNKNOWN_NOTICE_VALUES:
        missing.append("other_special_notice")
    return missing


def build_event_id(region: str, run_dt: Optional[datetime] = None) -> str:
    """이벤트를 식별하는 결정적 키.

    벽시계가 아니라 **예정된 실행 시각**에서 만든다. `datetime.now()`로 만들면
    DAG가 재시도할 때마다 값이 달라져 중복 발행·중복 색인을 막지 못한다.
    같은 스케줄 슬롯의 재시도는 같은 id를 갖고, 하류는 이 값으로 upsert 한다.

    이 파이프라인은 시간당 1건을 발행하므로 지역 + KST 기준 시각(시 단위)이면
    충분하다. 분 단위로 내리면 재시도 간 값이 갈린다.
    """
    if run_dt is None:
        run_dt = now_kst()
    return f"{region}:{to_kst(run_dt).strftime('%Y%m%d%H')}"
