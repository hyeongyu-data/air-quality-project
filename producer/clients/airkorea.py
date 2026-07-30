"""에어코리아 클라이언트 (실시간 대기질·황사 발생정보)"""

import logging
import xml.etree.ElementTree as ET  # noqa: F401 (일부 클라이언트만 사용)
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from ..api_common import BasePublicDataClient
from ..masking import mask_secrets
from ..timeutil import now_kst, to_kst

logger = logging.getLogger(__name__)


class AirKoreaAPIClient(BasePublicDataClient):
    """에어코리아 공공API 클라이언트"""
    
    # 에어코리아 실시간 대기질 API
    REALTIME_URL = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
    FORECAST_URL = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMinuDustFrcstDspth"
    YELLOW_DUST_URL = "http://apis.data.go.kr/B552584/OzYlwsndOccrrncInforInqireSvc/getYlwsndAdvsryOccrrncInfo"
    PM_ALARM_URL = "http://apis.data.go.kr/B552584/UlfptcaAlarmInqireSvc/getUlfptcaAlarmInfo"
    
    def __init__(self, api_key: str):
        """
        에어코리아 API 클라이언트 초기화
        
        Args:
            api_key: 에어코리아 공공데이터 API 키
        """
        self.api_key = api_key
        self.timeout = 10
        self.last_error = None
    
    



    def _request_items(
        self,
        url: str,
        params: Dict,
        service_name: str,
        optional: bool = False
    ) -> List[Dict]:
        """에어코리아 문서 기준 JSON/XML 응답을 모두 처리하는 공통 요청"""
        base_params = {
            "serviceKey": self.api_key,
            "pageNo": 1,
            "numOfRows": 100,
            "returnType": params.pop("returnType", "xml"),
        }
        base_params.update(params)
        
        response = None
        for attempt in range(2):
            try:
                response = requests.get(url, params=base_params, timeout=self.timeout)
                break
            except requests.exceptions.RequestException as e:
                self.last_error = mask_secrets(f"{service_name} 연결 실패: {str(e)}")
                if attempt == 0:
                    logger.warning(f"에어코리아 API 재시도: {self.last_error}")
                    continue
                logger.error(f"에어코리아 API 호출 실패: {self.last_error}")
                return []
        
        if response is None:
            return []
        if response.status_code in (401, 403):
            self.last_error = (
                "에어코리아 API 인증/활용신청 권한 오류. "
                f"현재 공공데이터 서비스키에 {service_name} 활용 권한이 있는지 확인하세요."
            )
            if optional:
                logger.info(self.last_error)
            else:
                logger.warning(self.last_error)
            return []
        if response.status_code >= 400:
            self.last_error = mask_secrets(f"{service_name} {response.status_code} {response.text[:120]}")
            logger.warning(f"에어코리아 API 응답 오류: {self.last_error}")
            return []
        
        try:
            data = response.json()
        except ValueError:
            data = self._parse_xml_response(response.text)
        
        if not self._is_success_response(data):
            self.last_error = mask_secrets(str(data.get("response", {}).get("header", data))[:200])
            logger.warning(f"에어코리아 API 오류({service_name}): {data}")
            return []
        
        self.last_error = None
        return self._extract_items(data)
    
    def get_air_quality(self, region: str = "서울") -> Optional[Dict]:
        """
        실시간 대기질 데이터 조회
        
        Args:
            region: 지역명 (기본값: 서울)
        
        Returns:
            {
                "timestamp": "2026-04-28T10:00:00",
                "region": "서울",
                "pm10": 45,
                "pm25": 20,
                "o3": 0.065,
                "no2": 0.045,
                "so2": 0.005,
                "co": 0.5,
                ...
            }
        """
        try:
            items = self._request_items(
                self.REALTIME_URL,
                {
                    "sidoName": region,
                    "ver": "1.0",
                },
                "ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
            )
            if items:
                parsed = self._parse_air_quality_items(items, region)
                logger.info(f"대기질 데이터 수집 성공: {region}")
                return parsed
            else:
                logger.warning(f"에어코리아 데이터 없음: {region}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.last_error = mask_secrets(str(e))
            logger.error(f"에어코리아 API 호출 실패: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"대기질 데이터 파싱 오류: {str(e)}")
            return None
    
    def get_yellow_dust_advisory(self, region: str = "서울") -> Dict:
        """황사 발생정보 문서 기준 현재 연도 황사 발생 지역 조회"""
        try:
            year = now_kst().strftime("%Y")
            items = self._request_items(
                self.YELLOW_DUST_URL,
                {"year": year},
                "OzYlwsndOccrrncInforInqireSvc/getYlwsndAdvsryOccrrncInfo",
                optional=True
            )
            if not items and self.last_error:
                return {
                    "yellow_dust_advisory": None,
                    "yellow_dust": None,
                    "yellow_dust_source": "ylwsnd_advisory_unavailable",
                    "yellow_dust_status": "unavailable",
                    "yellow_dust_message": self.last_error,
                }
            
            matched = []
            for item in items:
                area = item.get("tmArea") or item.get("issueArea") or ""
                if region in area:
                    matched.append(item)
            
            if matched:
                latest = matched[0]
                latest_time = latest.get("dataTime") or ""
                is_today = latest_time[:8] == now_kst().strftime("%Y%m%d")
                if is_today:
                    return {
                        "yellow_dust_advisory": "발생",
                        "yellow_dust": 500.0,
                        "yellow_dust_source": "ylwsnd_advisory",
                        "yellow_dust_area": latest.get("tmArea"),
                        "yellow_dust_time": latest_time,
                        "yellow_dust_count": latest.get("tmCnt"),
                    }
                return {
                    "yellow_dust_advisory": "없음",
                    "yellow_dust": 0.0,
                    "yellow_dust_source": "ylwsnd_advisory",
                    "last_yellow_dust_area": latest.get("tmArea"),
                    "last_yellow_dust_time": latest_time,
                }
            
            return {
                "yellow_dust_advisory": "없음",
                "yellow_dust": 0.0,
                "yellow_dust_source": "ylwsnd_advisory",
            }
            
        except Exception as e:
            logger.error(f"황사 발생정보 파싱 오류: {str(e)}")
            return {}
    
    @staticmethod
    def _parse_air_quality(item: Dict, region: str) -> Dict:
        """실시간 대기질 데이터 파싱"""
        def value(*keys: str) -> Any:
            for key in keys:
                if item.get(key) not in (None, "", "-"):
                    return item.get(key)
            return None
        
        return {
            "timestamp": now_kst().isoformat(),
            "region": region,
            "station_name": item.get("stationName", "정보없음"),
            "pm10": AirKoreaAPIClient._safe_float(value("pm10", "pm10Value"), default=None),
            "pm25": AirKoreaAPIClient._safe_float(value("pm25", "pm25Value"), default=None),
            "o3": AirKoreaAPIClient._safe_float(value("o3", "o3Value"), default=None),
            "no2": AirKoreaAPIClient._safe_float(value("no2", "no2Value"), default=None),
            "so2": AirKoreaAPIClient._safe_float(value("so2", "so2Value"), default=None),
            "co": AirKoreaAPIClient._safe_float(value("co", "coValue"), default=None),
            # 등급 필드는 내보내지 않는다. 대표 레코드의 수치는 전 측정소 평균인데
            # 등급만 첫 측정소 것이 남아 같은 문서 안에서 서로 다른 대상을 가리켰다.
            # 하류는 consumer/rules.py가 수치로 직접 판정하므로 필요도 없다.
            "data_time": item.get("dataTime"),
        }

    @staticmethod
    def _parse_air_quality_items(items: List[Dict], region: str) -> Dict:
        """시도별 측정소 목록을 서울 대표 평균값으로 집계"""
        parsed_items = [
            AirKoreaAPIClient._parse_air_quality(item, region)
            for item in items
        ]

        def mean_field(field: str) -> Optional[float]:
            values = [
                item[field]
                for item in parsed_items
                if item.get(field) is not None
            ]
            if not values:
                return None
            return round(sum(values) / len(values), 1)

        representative = parsed_items[0]
        representative.update({
            "station_name": "서울 평균",
            "station_count": len(parsed_items),
            "pm10": mean_field("pm10"),
            "pm25": mean_field("pm25"),
            "o3": mean_field("o3"),
            "no2": mean_field("no2"),
            "so2": mean_field("so2"),
            "co": mean_field("co"),
        })
        return representative
