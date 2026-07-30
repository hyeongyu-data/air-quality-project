"""처리 메트릭.

"알림이 실제로 전달됐는가, 얼마나 걸렸는가, 무엇이 결측이었는가"를
문서로 남긴다. 별도 메트릭 스택(Prometheus) 없이 이미 있는 OpenSearch에
쌓는다 — 운영 관측 대시보드(#24)의 데이터 소스가 된다.

빌더는 순수 함수다. 색인은 best-effort — 메트릭 저장 실패가 알림 처리를
막아서는 안 된다.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

try:
    from .timeutil import now_kst
except ImportError:  # 직접 실행 시
    from timeutil import now_kst

logger = logging.getLogger(__name__)

METRICS_INDEX_PREFIX = "weather-metrics"


def e2e_latency_seconds(payload_timestamp: Optional[str], now: Optional[datetime] = None) -> Optional[float]:
    """발행 타임스탬프 → 처리 완료까지의 지연(초). 파싱 불가면 None."""
    if not payload_timestamp:
        return None
    try:
        sent = datetime.fromisoformat(payload_timestamp)
    except (TypeError, ValueError):
        return None
    if sent.tzinfo is None:
        return None
    current = now or now_kst()
    return round((current - sent).total_seconds(), 3)


def missing_indices(levels: Dict) -> List[str]:
    """판정 결과에서 결측('정보없음')이었던 지수 이름 목록."""
    return sorted(
        key[: -len("_level")]
        for key, value in levels.items()
        if value == "정보없음"
    )


def build_message_metrics(
    processed: Dict,
    results: Dict,
    send_external: bool,
    duration_ms: float,
    now: Optional[datetime] = None,
) -> Dict:
    """메시지 1건의 처리 메트릭 문서를 만든다."""
    current = now or now_kst()
    levels = processed.get("levels", {})
    delivered = [c for c in ("slack", "email", "kakao") if results.get(c)]
    attempted = send_external
    return {
        "timestamp": current.isoformat(),
        "event_id": processed.get("event_id"),
        "region": processed.get("region"),
        "process_duration_ms": round(duration_ms, 1),
        "e2e_latency_seconds": e2e_latency_seconds(processed.get("timestamp"), current),
        "external_attempted": attempted,
        "delivered_channels": delivered,
        "delivery_failed": bool(attempted and not delivered),
        "suppressed": not attempted,
        "missing_indices": missing_indices(levels),
        "missing_count": len(missing_indices(levels)),
        "alert_severity": processed.get("alert_severity"),
        "opensearch_indexed": bool(results.get("opensearch")),
    }


class MetricsSink:
    """메트릭 문서를 OpenSearch에 색인한다(월 단위 인덱스, best-effort)."""

    def __init__(self, client=None):
        self.client = client

    def attach(self, client) -> None:
        self.client = client

    def emit(self, doc: Dict) -> bool:
        if self.client is None:
            return False
        try:
            index_name = f"{METRICS_INDEX_PREFIX}-{doc['timestamp'][:7].replace('-', '.')}"
            self.client.index(index=index_name, body=doc)
            return True
        except Exception as e:
            # 메트릭은 관측 수단이지 처리의 일부가 아니다 — 실패해도 진행한다.
            logger.warning(f"메트릭 색인 실패(처리는 계속): {str(e)}")
            return False
