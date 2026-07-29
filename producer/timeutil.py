"""KST 고정 시각 유틸.

컨테이너 TZ에 의존하지 않기 위해 코드에서 시간대를 명시한다. naive
`datetime.now()`로 만든 값은 오프셋이 없어 OpenSearch가 UTC로 해석하고,
같은 스택 안에서도 프로세스마다 TZ가 다르면 "지금"이 달라진다.

consumer/timeutil.py와 같은 내용이다. 복제 이유는 그쪽 주석 참고.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def now_kst() -> datetime:
    """오프셋이 붙은 현재 KST 시각."""
    return datetime.now(KST)


def to_kst(value: datetime) -> datetime:
    """다른 시간대의 시각을 KST로 옮긴다. naive 입력은 KST로 간주한다."""
    if value.tzinfo is None:
        return value.replace(tzinfo=KST)
    return value.astimezone(KST)


def kst_isoformat() -> str:
    """오프셋을 포함한 ISO8601 문자열. 발행 타임스탬프의 표준 형식."""
    return now_kst().isoformat()
