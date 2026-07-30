"""구조화 로깅.

평문 f-string 로그는 사람이 읽기엔 좋지만 기계가 집계할 수 없다.
LOG_FORMAT=json이면 한 줄 JSON으로 남겨 OpenSearch·CloudWatch 등에서
필드 단위로 조회할 수 있게 한다. 기본은 text — 로컬 docker logs 가독성.

event_id를 contextvar로 전파해, 메시지 처리 중에 찍히는 모든 로그가
같은 상관관계 ID를 갖는다. 프로듀서 → 컨슈머 → 알림 → 이력을 한 ID로
따라갈 수 있다.
"""

import json
import logging
import os
from contextvars import ContextVar

# 처리 중인 메시지의 event_id. 메시지 시작 시 set, 종료 시 reset.
current_event_id: ContextVar = ContextVar("current_event_id", default=None)


class JsonFormatter(logging.Formatter):
    """한 줄 JSON 포매터. 표준 필드 + event_id + extra를 담는다."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        event_id = current_event_id.get()
        if event_id:
            entry["event_id"] = event_id
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        # logger.info(..., extra={"k": v})로 넘긴 커스텀 필드 포함
        for key, value in record.__dict__.items():
            if key.startswith("metric_"):
                entry[key[len("metric_"):]] = value
        return json.dumps(entry, ensure_ascii=False, default=str)


class EventIdFilter(logging.Filter):
    """text 포맷에서도 event_id가 보이도록 레코드에 심는다."""

    def filter(self, record: logging.LogRecord) -> bool:
        event_id = current_event_id.get()
        record.event_id = f" [{event_id}]" if event_id else ""
        return True


def setup_logging() -> None:
    """루트 로거를 LOG_FORMAT에 맞게 구성한다. 여러 번 불려도 안전하다."""
    root = logging.getLogger()
    root.setLevel(os.getenv("LOG_LEVEL", "INFO"))

    if os.getenv("LOG_FORMAT", "text").lower() == "json":
        formatter = logging.Formatter()  # 자리표시 — 아래에서 교체
        formatter = JsonFormatter()
        fmt_filter = None
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s -%(event_id)s %(message)s"
        )
        fmt_filter = EventIdFilter()

    if not root.handlers:
        root.addHandler(logging.StreamHandler())
    for handler in root.handlers:
        handler.setFormatter(formatter)
        if fmt_filter and not any(isinstance(f, EventIdFilter) for f in handler.filters):
            handler.addFilter(fmt_filter)
