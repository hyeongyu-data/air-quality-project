"""공공데이터포털 API 공통 유틸.

세 클라이언트에 같은 유틸이 복사돼 있었고, _safe_float의 기본값이 서로
달랐다(0 / 0 / None). 결측이 0으로 둔갑하는 계열 결함(#33)의 온상이라
기본값을 None으로 통일한다 — 0이 필요한 곳은 호출부가 명시적으로 넘긴다.
"""

import logging
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


def timed_get(api_name: str, url: str, **kwargs):
    """requests.get + 소요시간·상태 구조화 로그.

    API별 응답시간·실패율은 알람 기준(관측 문서)의 입력이다. Airflow 태스크
    로그에 남으므로 별도 수집기 없이도 grep으로 집계할 수 있다.
    """
    started = time.monotonic()
    try:
        response = requests.get(url, **kwargs)
        logger.info(
            "api_call api=%s status=%s duration_ms=%.0f",
            api_name, response.status_code, (time.monotonic() - started) * 1000,
        )
        return response
    except Exception:
        logger.warning(
            "api_call api=%s status=EXC duration_ms=%.0f",
            api_name, (time.monotonic() - started) * 1000,
        )
        raise


class BasePublicDataClient:
    """공공데이터포털 응답의 공통 파싱 유틸"""

    @staticmethod
    def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        """빈 값/결측 마커("", "-")를 안전하게 float로 변환. 기본은 결측(None)."""
        if value in (None, "", "-"):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _is_success_response(data: Dict) -> bool:
        """성공 판정. 포털 계열별 표기(response.status / header.resultCode "0"·"00")를 모두 수용한다."""
        response = data.get("response", {})
        header = response.get("header", {})
        return (
            response.get("status") == "00"
            or header.get("resultCode") in ("0", "00")
        )

    @staticmethod
    def _extract_items(data: Dict) -> List[Dict]:
        """JSON 응답에서 item 리스트 추출 (dict-wrapped / bare list / 단건 dict 모두 수용)"""
        items = data.get("response", {}).get("body", {}).get("items", [])
        if isinstance(items, dict):
            items = items.get("item", items)
        if isinstance(items, dict):
            return [items]
        return items if isinstance(items, list) else []

    @staticmethod
    def _parse_xml_response(xml_text: str) -> Dict:
        """XML 응답을 JSON 응답과 같은 구조로 변환 (폴백 경로)"""
        root = ET.fromstring(xml_text)

        def text(path: str) -> Optional[str]:
            node = root.find(path)
            return node.text if node is not None else None

        items = [
            {child.tag: child.text for child in list(item)}
            for item in root.findall(".//item")
        ]

        return {
            "response": {
                "header": {
                    "resultCode": text(".//header/resultCode"),
                    "resultMsg": text(".//header/resultMsg"),
                },
                "body": {"items": {"item": items}},
            }
        }
