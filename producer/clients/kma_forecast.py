"""기상청 단기·초단기예보 클라이언트 (강수확률·기온·특보성 신호)"""

import logging
import xml.etree.ElementTree as ET  # noqa: F401 (일부 클라이언트만 사용)
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

from ..api_common import BasePublicDataClient, timed_get
from ..masking import mask_secrets
from ..timeutil import now_kst

logger = logging.getLogger(__name__)


class KMAForecastAPIClient(BasePublicDataClient):
    """기상청 단기예보 조회서비스 클라이언트"""
    
    BASE_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0"
    VILAGE_FORECAST_URL = f"{BASE_URL}/getVilageFcst"
    ULTRA_SHORT_FORECAST_URL = f"{BASE_URL}/getUltraSrtFcst"
    
    SEOUL_GRID = {
        "area_no": "1100000000",
        "region": "서울",
        "nx": 60,
        "ny": 127,
        "longitude": 126.980008333333,
        "latitude": 37.5635694444444,
    }
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.timeout = 10
        self.last_error = None
    
    
    
    
    @staticmethod
    def _latest_base_datetime(hours: List[int], delay_minutes: int) -> datetime:
        now = now_kst()
        candidate = now.replace(minute=0, second=0, microsecond=0)
        while True:
            if candidate.hour in hours and now >= candidate + timedelta(minutes=delay_minutes):
                return candidate
            candidate -= timedelta(hours=1)
    
    def _request_items(
        self,
        url: str,
        base_dt: datetime,
        nx: int = 60,
        ny: int = 127,
        num_rows: int = 1000
    ) -> List[Dict]:
        params = {
            "serviceKey": self.api_key,
            "pageNo": 1,
            "numOfRows": num_rows,
            "dataType": "JSON",
            "base_date": base_dt.strftime("%Y%m%d"),
            "base_time": base_dt.strftime("%H%M"),
            "nx": nx,
            "ny": ny,
        }
        response = timed_get("kma_forecast", url, params=params, timeout=self.timeout)
        if response.status_code >= 400:
            self.last_error = mask_secrets(f"{response.status_code} {response.text[:120]}")
            logger.warning(f"단기예보 API 응답 오류: {self.last_error}")
            return []
        data = response.json()
        
        if not self._is_success_response(data):
            self.last_error = mask_secrets(str(data.get("response", {}).get("header", data))[:200])
            logger.warning(f"단기예보 API 오류: {data}")
            return []
        
        return self._extract_items(data)
    
    def get_precipitation_probability(self, nx: int = 60, ny: int = 127) -> Optional[Dict]:
        """서울 격자 기준 가장 가까운 단기예보 강수확률(POP) 조회"""
        try:
            base_dt = self._latest_base_datetime([2, 5, 8, 11, 14, 17, 20, 23], 10)
            items = self._request_items(self.VILAGE_FORECAST_URL, base_dt, nx, ny)
            pop_items = [item for item in items if item.get("category") == "POP"]
            if not pop_items:
                return None
            
            now_key = now_kst().strftime("%Y%m%d%H%M")
            
            def fcst_key(item: Dict) -> str:
                return f"{item.get('fcstDate', '')}{item.get('fcstTime', '')}"
            
            selected = next((item for item in pop_items if fcst_key(item) >= now_key), pop_items[0])
            return {
                "precipitation_probability": self._safe_float(selected.get("fcstValue"), 0),
                "precipitation_base_time": base_dt.strftime("%Y%m%d%H%M"),
                "precipitation_forecast_time": fcst_key(selected),
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"강수확률 API 호출 실패: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"강수확률 파싱 오류: {str(e)}")
            return None
    
    def get_today_forecast_summary(
        self,
        nx: int = 60,
        ny: int = 127,
        include_elapsed: bool = False
    ) -> Dict:
        """오늘 단기예보를 하루 대표 위험값으로 요약"""
        try:
            base_dt = self._latest_base_datetime([2, 5, 8, 11, 14, 17, 20, 23], 10)
            items = self._request_items(self.VILAGE_FORECAST_URL, base_dt, nx, ny, num_rows=2000)
            if not items:
                return {}
            
            now = now_kst()
            today = now.strftime("%Y%m%d")
            now_key = now.strftime("%Y%m%d%H%M")
            
            def fcst_key(item: Dict) -> str:
                return f"{item.get('fcstDate', '')}{item.get('fcstTime', '')}"
            
            if include_elapsed:
                today_items = [item for item in items if item.get("fcstDate") == today]
            else:
                today_items = [
                    item for item in items
                    if item.get("fcstDate") == today and fcst_key(item) >= now_key
                ]
                if not today_items:
                    today_items = [item for item in items if item.get("fcstDate") == today]
            if not today_items:
                return {}
            
            def values_for(category: str) -> List[float]:
                return [
                    value for value in (
                        self._safe_float(item.get("fcstValue"), default=None)
                        for item in today_items
                        if item.get("category") == category
                    )
                    if value is not None
                ]
            
            pop_values = values_for("POP")
            tmp_values = values_for("TMP")
            wsd_values = values_for("WSD")
            lgt_values = values_for("LGT")
            pty_values = [
                str(item.get("fcstValue", "0"))
                for item in today_items
                if item.get("category") == "PTY"
            ]
            
            alerts = []
            max_lgt = max(lgt_values) if lgt_values else 0
            max_wsd = max(wsd_values) if wsd_values else 0
            has_precipitation_type = any(value not in ("0", "", "None") for value in pty_values)
            
            if max_lgt > 0:
                alerts.append("오늘 낙뢰 가능")
            if max_wsd >= 9:
                alerts.append("오늘 강풍 가능")
            if has_precipitation_type:
                alerts.append("오늘 강수 가능")
            
            max_temp = max(tmp_values) if tmp_values else None
            min_temp = min(tmp_values) if tmp_values else None
            
            return {
                "forecast_date": today,
                "forecast_period": "today_full" if include_elapsed else "today_remaining",
                "forecast_base_time": base_dt.strftime("%Y%m%d%H%M"),
                "precipitation_probability": max(pop_values) if pop_values else None,
                "precipitation_summary": {
                    "max_probability": max(pop_values) if pop_values else None,
                    "min_probability": min(pop_values) if pop_values else None,
                    "values": pop_values,
                },
                "feels_like_temp": max_temp,
                "temperature_summary": {
                    "min_temperature": min_temp,
                    "max_temperature": max_temp,
                    "values": tmp_values,
                },
                "other_special_notice": ", ".join(alerts) if alerts else "없음",
                "other_special_notice_source": "today_vilage_forecast",
                "forecast_signals": {
                    "LGT_MAX": max_lgt,
                    "WSD_MAX": max_wsd,
                    "PTY_VALUES": sorted(set(pty_values)),
                },
                "forecast_time_range": {
                    "start": min(fcst_key(item) for item in today_items),
                    "end": max(fcst_key(item) for item in today_items),
                },
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"오늘 예보 요약 API 호출 실패: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"오늘 예보 요약 파싱 오류: {str(e)}")
            return {}
    
    def get_current_temperature(self, nx: int = 60, ny: int = 127) -> Optional[Dict]:
        """초단기실황/예보 기반 현재 기온 조회"""
        try:
            base_dt = self._latest_base_datetime(list(range(24)), 10)
            items = self._request_items(f"{self.BASE_URL}/getUltraSrtNcst", base_dt, nx, ny)
            temp_item = next((item for item in items if item.get("category") == "T1H"), None)
            
            if not temp_item:
                fcst_dt = self._latest_base_datetime(list(range(24)), 45)
                items = self._request_items(self.ULTRA_SHORT_FORECAST_URL, fcst_dt, nx, ny)
                temp_item = next((item for item in items if item.get("category") == "T1H"), None)
            
            if not temp_item:
                return None
            
            value = temp_item.get("obsrValue", temp_item.get("fcstValue"))
            return {
                "current_temperature": self._safe_float(value, 0),
                "temperature_base_time": (
                    f"{temp_item.get('baseDate', '')}{temp_item.get('baseTime', '')}"
                ),
                "temperature_source": temp_item.get("category", "T1H"),
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"현재 기온 API 호출 실패: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"현재 기온 파싱 오류: {str(e)}")
            return None
    
    def get_other_special_notice(self, nx: int = 60, ny: int = 127) -> Dict:
        """초단기예보 코드 기반 기타 특보성 신호 요약"""
        try:
            base_dt = self._latest_base_datetime(list(range(24)), 45)
            items = self._request_items(self.ULTRA_SHORT_FORECAST_URL, base_dt, nx, ny)
            latest_by_category = {}
            for item in items:
                category = item.get("category")
                if category and category not in latest_by_category:
                    latest_by_category[category] = item.get("fcstValue")
            
            alerts = []
            lightning = self._safe_float(latest_by_category.get("LGT"), 0) or 0
            wind_speed = self._safe_float(latest_by_category.get("WSD"), 0) or 0
            precipitation_type = str(latest_by_category.get("PTY", "0"))
            
            if lightning > 0:
                alerts.append("낙뢰 가능")
            if wind_speed >= 9:
                alerts.append("강풍 가능")
            if precipitation_type not in ("0", "", "None"):
                alerts.append("강수 가능")
            
            return {
                "other_special_notice": ", ".join(alerts) if alerts else "없음",
                "other_special_notice_source": "ultra_short_forecast",
                "forecast_signals": {
                    "LGT": lightning,
                    "WSD": wind_speed,
                    "PTY": precipitation_type,
                    "SKY": latest_by_category.get("SKY"),
                },
                "special_notice_base_time": base_dt.strftime("%Y%m%d%H%M"),
            }
            
        except Exception as e:
            logger.error(f"기타특보성 신호 파싱 오류: {str(e)}")
            return {
                "other_special_notice": "정보없음",
                "other_special_notice_source": "ultra_short_forecast",
                "forecast_signals": {},
            }
