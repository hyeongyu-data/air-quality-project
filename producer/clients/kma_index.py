"""기상청 생활·보건기상지수 클라이언트 (꽃가루·자외선·체감온도)"""

import logging
import xml.etree.ElementTree as ET  # noqa: F401 (일부 클라이언트만 사용)
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

from ..api_common import BasePublicDataClient
from ..masking import mask_secrets
from ..timeutil import now_kst, to_kst

logger = logging.getLogger(__name__)


class WeatherAPIClient(BasePublicDataClient):
    """기상청 공공API 클라이언트"""

    # 기상청 생활기상지수/보건기상지수 공공데이터포털 API
    LIVING_INDEX_BASE_URL = "http://apis.data.go.kr/1360000/LivingWthrIdxServiceV4"
    HEALTH_INDEX_BASE_URL = "http://apis.data.go.kr/1360000/HealthWthrIdxServiceV3"
    UV_INDEX_URL = f"{LIVING_INDEX_BASE_URL}/getUVIdxV4"
    SUMMER_FEELS_LIKE_URL = f"{LIVING_INDEX_BASE_URL}/getSenTaIdxV4"
    POLLEN_ENDPOINTS = {
        "oak_pollen": f"{HEALTH_INDEX_BASE_URL}/getOakPollenRiskIdxV3",
        "pine_pollen": f"{HEALTH_INDEX_BASE_URL}/getPinePollenRiskIdxV3",
    }
    
    def __init__(self, api_key: str):
        """
        기상청 API 클라이언트 초기화
        
        Args:
            api_key: 기상청 공공데이터 API 키
        """
        self.api_key = api_key
        self.timeout = 10
        self.last_error = None
    
    
    
    
    
    def _request_items(
        self,
        url: str,
        area_no: str,
        time: Optional[str] = None,
        extra_params: Optional[Dict] = None
    ) -> List[Dict]:
        """생활/보건기상지수 공통 요청 처리"""
        params = {
            "serviceKey": self.api_key,
            "pageNo": 1,
            "numOfRows": 10,
            "dataType": "JSON",
            "areaNo": area_no,
            "time": time or now_kst().strftime("%Y%m%d%H"),
        }
        if extra_params:
            params.update(extra_params)
        
        response = requests.get(url, params=params, timeout=self.timeout)
        if response.status_code >= 400:
            self.last_error = mask_secrets(f"{response.status_code} {response.text[:120]}")
            logger.warning(f"기상청 API 응답 오류: {self.last_error}")
            return []
        
        try:
            data = response.json()
        except ValueError:
            data = self._parse_xml_response(response.text)
        
        if not self._is_success_response(data):
            self.last_error = mask_secrets(str(data.get("response", {}).get("header", data))[:200])
            logger.warning(f"기상청 API 오류: {data}")
            return []
        
        return self._extract_items(data)
    
    @staticmethod
    def _latest_index_time(hours: List[int] = None, delay_minutes: int = 30) -> str:
        """생활/보건기상지수 발표시각 생성"""
        hours = hours or [6, 18]
        now = now_kst()
        candidate = now.replace(minute=0, second=0, microsecond=0)
        while True:
            if candidate.hour in hours and now >= candidate + timedelta(minutes=delay_minutes):
                return candidate.strftime("%Y%m%d%H")
            candidate -= timedelta(hours=1)
    
    def _build_forecast_record(
        self,
        item: Dict,
        region: str,
        value_keys: Optional[List[str]] = None
    ) -> Dict:
        """일/시간별 예측값을 표준 필드로 정리"""
        value_keys = value_keys or ["today", "tomorrow", "dayaftertomorrow", "twodaysaftertomorrow"]
        forecast = {
            key: self._safe_float(item.get(key), default=None)
            for key in value_keys
            if item.get(key) not in (None, "")
        }
        hourly = {
            key: self._safe_float(item.get(key), default=None)
            for key in sorted(
                [key for key in item if key.startswith("h") and key[1:].isdigit()],
                key=lambda x: int(x[1:])
            )
            if item.get(key) not in (None, "")
        }
        first_value = next(
            (value for value in list(forecast.values()) + list(hourly.values()) if value is not None),
            0
        )
        
        return {
            "timestamp": now_kst().isoformat(),
            "region": region,
            "area_no": item.get("areaNo"),
            "base_time": item.get("date"),
            "code": item.get("code"),
            "value": first_value,
            "forecast": forecast,
            "hourly_forecast": hourly,
        }
    
    def get_health_index(self, region_code: str = "1100000000") -> Optional[Dict]:
        """
        보건기상지수 조회 (꽃가루농도위험지수: 참나무/소나무/잡초류)
        
        Args:
            region_code: 지역 코드 (기본값: 서울 1100000000)
        
        Returns:
            {
                "timestamp": "2026-04-28T10:00:00",
                "region": "서울",
                "oak_pollen": 2,
                "pine_pollen": 1,
                "weeds_pollen": 3,
                "pollen_risk": 3
            }
        """
        try:
            parsed = self._parse_health_index(region_code)
            logger.info(f"보건기상지수 수집 성공: {parsed}")
            return parsed or None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"기상청 API 호출 실패: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"보건기상지수 파싱 오류: {str(e)}")
            return None
    
    def get_uv_index(self, region_code: str = "1100000000") -> Optional[Dict]:
        """
        자외선지수 조회
        
        Args:
            region_code: 지역 코드
        
        Returns:
            {
                "timestamp": "2026-04-28T10:00:00",
                "region": "서울",
                "uv_index": 6,
                "uv_today": 6,
                "uv_tomorrow": 8,
                "uv_day_after": 4
            }
        """
        try:
            items = self._request_items(
                self.UV_INDEX_URL,
                region_code,
                time=self._latest_index_time()
            )
            parsed = self._parse_uv_index(items)
            logger.info(f"자외선지수 수집 성공: {parsed}")
            return parsed or None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"기상청 API 호출 실패: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"자외선지수 파싱 오류: {str(e)}")
            return None
    
    def get_living_index(self, region_code: str = "1100000000") -> Optional[Dict]:
        """
        생활기상지수 조회 (체감온도(여름철))
        
        Args:
            region_code: 지역 코드
        
        Returns:
            {
                "timestamp": "2026-04-28T10:00:00",
                "region": "서울",
                "feels_like_temp": 15,
                "feels_like_temp_forecast": {"h1": 25, "h2": 25}
            }
        """
        try:
            items = self._request_items(
                self.SUMMER_FEELS_LIKE_URL,
                region_code,
                time=self._latest_index_time(),
                extra_params={"requestCode": "A41"}
            )
            parsed = self._parse_living_index(items)
            logger.info(f"생활기상지수 수집 성공: {parsed}")
            return parsed or None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"기상청 API 호출 실패: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"생활기상지수 파싱 오류: {str(e)}")
            return None
    
    def _parse_health_index(self, region_code: str) -> Dict:
        """보건기상지수 파싱"""
        result = {
            "timestamp": now_kst().isoformat(),
            "region": "서울",
            "area_no": region_code,
            "data_type": "health_index",
            "indices": {},
            "forecasts": {},
        }
        
        for index_name, url in self.POLLEN_ENDPOINTS.items():
            items = self._request_items(url, region_code)
            if not items:
                continue
            record = self._build_forecast_record(items[0], "서울")
            result[index_name] = record["value"]
            result["indices"][index_name] = record["value"]
            result["forecasts"][index_name] = record["forecast"]
            result[f"{index_name}_code"] = record["code"]
        
        pollen_values = [value for value in result["indices"].values() if value is not None]
        if pollen_values:
            result["pollen_risk"] = max(pollen_values)
        
        return result if result["indices"] else {}
    
    def _parse_uv_index(self, items: List[Dict]) -> Dict:
        """자외선지수 파싱"""
        if not items:
            return {}
        
        record = self._build_forecast_record(items[0], "서울")
        latest_uv = self._select_latest_hourly_value(
            items[0],
            base_time=record["base_time"],
            now=now_kst()
        )
        uv_index = latest_uv.get("value") if latest_uv else record["value"]
        return {
            "timestamp": now_kst().isoformat(),
            "region": "서울",
            "area_no": record["area_no"],
            "base_time": record["base_time"],
            "code": record["code"],
            "uv_index": uv_index,
            "uv_selected_hour": latest_uv.get("hour") if latest_uv else None,
            "uv_selected_time": latest_uv.get("time") if latest_uv else None,
            "uv_today": record["forecast"].get("today"),
            "uv_tomorrow": record["forecast"].get("tomorrow"),
            "uv_day_after": record["forecast"].get("dayaftertomorrow"),
            "uv_two_days_after": record["forecast"].get("twodaysaftertomorrow"),
            "uv_hourly_forecast": record["hourly_forecast"],
        }
    
    @staticmethod
    def _select_latest_hourly_value(
        item: Dict,
        base_time: Optional[str],
        now: datetime
    ) -> Optional[Dict]:
        """h0/h3/h6... 값을 실제 예보시각으로 풀어 현재 이하 최신값 선택"""
        if not base_time:
            return None
        try:
            # 기상청 발표시각은 KST다. aware로 만들지 않으면 아래에서 aware인
            # now와 비교할 때 TypeError가 난다.
            base_dt = to_kst(datetime.strptime(base_time, "%Y%m%d%H"))
        except ValueError:
            return None
        
        candidates = []
        for key, raw_value in item.items():
            if not (key.startswith("h") and key[1:].isdigit()):
                continue
            if raw_value in (None, "", "-"):
                continue
            value = WeatherAPIClient._safe_float(raw_value, default=None)
            if value is None:
                continue
            hour_offset = int(key[1:])
            forecast_dt = base_dt + timedelta(hours=hour_offset)
            candidates.append({
                "hour": key,
                "time": forecast_dt.strftime("%Y%m%d%H"),
                "datetime": forecast_dt,
                "value": value,
            })
        
        if not candidates:
            return None
        
        past_or_now = [candidate for candidate in candidates if candidate["datetime"] <= now]
        selected = max(past_or_now, key=lambda x: x["datetime"]) if past_or_now else min(
            candidates,
            key=lambda x: x["datetime"]
        )
        return {
            "hour": selected["hour"],
            "time": selected["time"],
            "value": selected["value"],
        }
    
    def _parse_living_index(self, items: List[Dict]) -> Dict:
        """생활기상지수 파싱"""
        if not items:
            return {}
        
        record = self._build_forecast_record(items[0], "서울")
        return {
            "timestamp": now_kst().isoformat(),
            "region": "서울",
            "area_no": record["area_no"],
            "base_time": record["base_time"],
            "code": record["code"],
            "feels_like_temp": record["value"],
            "feels_like_temp_forecast": record["hourly_forecast"],
        }
