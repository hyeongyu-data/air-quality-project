"""
기상지수 데이터 프로듀서

기상청/에어코리아 공공API에서 실시간 예측 데이터를 수집하고
Kafka 토픽으로 발행하는 프로듀서입니다.

데이터 흐름:
1. API 호출 (기상청, 에어코리아)
2. JSON 파싱 및 검증
3. 필요한 필드만 추출
4. Kafka 토픽으로 발행
"""

import os
import json
import logging
import xml.etree.ElementTree as ET
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

try:
    from .timeutil import now_kst, to_kst
except ImportError:  # 직접 실행 시
    from timeutil import now_kst, to_kst


try:
    from .masking import mask_secrets, install_secret_filter
except ImportError:  # 직접 실행 시
    from masking import mask_secrets, install_secret_filter

# 값 생성 지점 마스킹의 안전망. 이후 누가 URL을 그대로 로깅해도 출력 단계에서 걸린다.
install_secret_filter()


class WeatherAPIClient:
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
    
    @staticmethod
    def _safe_float(value: Any, default: Optional[float] = 0) -> Optional[float]:
        """공공API 빈 값/문자열 수치를 안전하게 float로 변환"""
        if value in (None, "", "-"):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    @staticmethod
    def _is_success_response(data: Dict) -> bool:
        """공공API 응답 성공 여부 확인"""
        response = data.get("response", {})
        header = response.get("header", {})
        return (
            response.get("status") == "00" or
            header.get("resultCode") == "00"
        )
    
    @staticmethod
    def _extract_items(data: Dict) -> List[Dict]:
        """공공데이터포털 JSON 응답에서 item 리스트 추출"""
        items = data.get("response", {}).get("body", {}).get("items", [])
        if isinstance(items, dict):
            items = items.get("item", items)
        if isinstance(items, dict):
            return [items]
        return items if isinstance(items, list) else []
    
    @staticmethod
    def _parse_xml_response(xml_text: str) -> Dict:
        """API가 XML로 응답했을 때 JSON 응답과 비슷한 구조로 변환"""
        root = ET.fromstring(xml_text)
        
        def text(path: str) -> Optional[str]:
            node = root.find(path)
            return node.text if node is not None else None
        
        items = []
        for item in root.findall(".//item"):
            items.append({child.tag: child.text for child in list(item)})
        
        return {
            "response": {
                "header": {
                    "resultCode": text(".//header/resultCode"),
                    "resultMsg": text(".//header/resultMsg"),
                },
                "body": {
                    "items": {"item": items}
                }
            }
        }
    
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


class AirKoreaAPIClient:
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
    
    @staticmethod
    def _safe_float(value: Any, default: float = 0) -> float:
        """에어코리아 결측 문자열을 안전하게 float로 변환"""
        if value in (None, "", "-"):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    @staticmethod
    def _is_success_response(data: Dict) -> bool:
        """공공API 응답 성공 여부 확인"""
        response = data.get("response", {})
        header = response.get("header", {})
        return (
            response.get("status") == "00" or
            header.get("resultCode") == "00"
        )

    @staticmethod
    def _parse_xml_response(xml_text: str) -> Dict:
        """에어코리아 XML 응답을 JSON 응답과 같은 형태로 변환"""
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
                "body": {
                    "items": {"item": items}
                }
            }
        }

    @staticmethod
    def _extract_items(data: Dict) -> List[Dict]:
        """에어코리아 JSON/XML 표준 응답에서 item 리스트 추출"""
        items = data.get("response", {}).get("body", {}).get("items", [])
        if isinstance(items, dict):
            items = items.get("item", items)
        if isinstance(items, dict):
            return [items]
        return items if isinstance(items, list) else []

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
    

class KMAForecastAPIClient:
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
    def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        if value in (None, "", "-"):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    @staticmethod
    def _is_success_response(data: Dict) -> bool:
        header = data.get("response", {}).get("header", {})
        return header.get("resultCode") in ("0", "00")
    
    @staticmethod
    def _extract_items(data: Dict) -> List[Dict]:
        items = data.get("response", {}).get("body", {}).get("items", [])
        if isinstance(items, dict):
            items = items.get("item", items)
        if isinstance(items, dict):
            return [items]
        return items if isinstance(items, list) else []
    
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
        response = requests.get(url, params=params, timeout=self.timeout)
        if response.status_code >= 400:
            self.last_error = mask_secrets(f"{response.status_code} {response.text[:120]}")
            logger.warning(f"단기예보 API 응답 오류: {self.last_error}")
            return []
        response.raise_for_status()
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


class KafkaWeatherProducer:
    """Kafka 프로듀서"""
    
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic_current_weather: str = "seoul-weather"
    ):
        """
        Kafka 프로듀서 초기화

        Args:
            bootstrap_servers: Kafka 브로커 주소 (기본값: 환경변수)
            topic_current_weather: 서울 현재 기상 통합 토픽명
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic_current_weather = topic_current_weather
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks='all',
                retries=3,
                # 재시도 시 브로커가 중복 레코드를 걸러낸다. acks='all'이 전제 조건이라
                # 위 설정과 함께여야 의미가 있다.
                enable_idempotence=True,
            )
            logger.info(f"Kafka 프로듀서 초기화 성공: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Kafka 프로듀서 초기화 실패: {str(e)}")
            self.producer = None
    
    def send_current_weather(self, data: Dict) -> bool:
        """서울 현재 기상 통합 데이터 발행"""
        if not self.producer:
            logger.warning("Kafka 프로듀서가 초기화되지 않음")
            return False
        
        try:
            future = self.producer.send(
                self.topic_current_weather,
                value=data,
                key=data.get("region", "서울").encode('utf-8')
            )
            record_metadata = future.get(timeout=5)
            logger.info(
                "서울 현재 기상 통합 데이터 발행 성공: "
                f"event_id={data.get('event_id')} 오프셋={record_metadata.offset}"
            )
            return True
            
        except Exception as e:
            logger.error(f"서울 현재 기상 통합 데이터 발행 실패: {str(e)}")
            return False
    
    def flush(self):
        """대기 중인 모든 메시지 발행"""
        if self.producer:
            self.producer.flush()
    
    def close(self):
        """프로듀서 종료"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka 프로듀서 종료")


class WeatherDataCollector:
    """모든 기상 데이터를 수집하고 발행하는 통합 수집기"""
    
    def __init__(self):
        """수집기 초기화"""
        weather_api_key = os.getenv("WEATHER_API_KEY", "")
        airkorea_api_key = os.getenv("AIRKOREA_API_KEY") or weather_api_key
        self.weather_api = WeatherAPIClient(
            api_key=weather_api_key
        )
        self.airkorea_api = AirKoreaAPIClient(
            api_key=airkorea_api_key
        )
        self.forecast_api = KMAForecastAPIClient(
            api_key=weather_api_key
        )
        self.producer = KafkaWeatherProducer()
    
    def collect_current_weather(self, region: str = "서울") -> Dict:
        """요청 항목만 모은 서울 현재 기상 데이터 생성"""
        grid = KMAForecastAPIClient.SEOUL_GRID
        air_data = self.airkorea_api.get_air_quality(region) or {}
        air_quality_error = self.airkorea_api.last_error
        yellow_dust_data = self.airkorea_api.get_yellow_dust_advisory(region) or {}
        yellow_dust_error = self.airkorea_api.last_error if not yellow_dust_data else None
        health_data = self.weather_api.get_health_index(grid["area_no"]) or {}
        living_data = self.weather_api.get_living_index(grid["area_no"]) or {}
        living_error = self.weather_api.last_error
        uv_data = self.weather_api.get_uv_index(grid["area_no"]) or {}
        uv_error = self.weather_api.last_error
        pop_data = self.forecast_api.get_precipitation_probability(grid["nx"], grid["ny"]) or {}
        temp_data = self.forecast_api.get_current_temperature(grid["nx"], grid["ny"]) or {}
        notice_data = self.forecast_api.get_other_special_notice(grid["nx"], grid["ny"])
        feels_like_temp = temp_data.get("current_temperature")
        feels_like_source = temp_data.get("temperature_source", "ultra_short_temperature")
        if feels_like_temp is None:
            feels_like_temp = living_data.get("feels_like_temp")
            feels_like_source = "living_weather_index"
        
        data_warnings = {}
        if not air_data:
            data_warnings["air_quality"] = air_quality_error or "대기질 데이터 없음"
        if (
            yellow_dust_data.get("yellow_dust_status") == "unavailable" and
            air_data.get("yellow_dust") is None
        ):
            data_warnings["yellow_dust"] = yellow_dust_data.get("yellow_dust_message")
        elif not yellow_dust_data:
            data_warnings["yellow_dust"] = yellow_dust_error or "황사 발생정보 데이터 없음"
        if not living_data:
            if feels_like_temp is None:
                data_warnings["living_index"] = living_error or "생활기상지수 데이터 없음"
            else:
                data_warnings["living_index"] = "생활기상지수 API 응답 없음, 초단기실황 기온으로 대체"
        if not temp_data and living_data:
            data_warnings["temperature"] = "초단기실황 기온 없음, 생활기상지수 값으로 대체"
        if not uv_data:
            data_warnings["uv_index"] = uv_error or "자외선지수 데이터 없음"
        
        # 황사는 발생정보 API 결과만 쓴다. 예전에는 권한이 없을 때 PM10 평균을
        # 그대로 복사했는데, 황사 판정의 "좋음" 임계가 150이라 서울 PM10 평균으로는
        # 사실상 항상 좋음이 나왔다. 감시되는 것처럼 보이지만 실제로는 죽은 지표였다.
        # 권한이 없으면 결측(None)으로 두고 컨슈머가 "정보없음"으로 표시한다.
        yellow_dust = yellow_dust_data.get("yellow_dust")
        yellow_dust_source = yellow_dust_data.get("yellow_dust_source")
        
        return {
            "timestamp": now_kst().isoformat(),
            "region": region,
            "data_type": "current_weather",
            "area_no": grid["area_no"],
            "nx": grid["nx"],
            "ny": grid["ny"],
            "latitude": grid["latitude"],
            "longitude": grid["longitude"],
            "oak_pollen": health_data.get("oak_pollen"),
            "pine_pollen": health_data.get("pine_pollen"),
            "pm10": air_data.get("pm10"),
            "pm25": air_data.get("pm25"),
            "yellow_dust": yellow_dust,
            "yellow_dust_advisory": yellow_dust_data.get("yellow_dust_advisory"),
            "yellow_dust_source": yellow_dust_source,
            "feels_like_temp": feels_like_temp,
            "feels_like_temp_source": feels_like_source,
            "other_special_notice": notice_data.get("other_special_notice"),
            "other_special_notice_source": notice_data.get("other_special_notice_source"),
            "precipitation_probability": pop_data.get("precipitation_probability"),
            "uv_index": uv_data.get("uv_index"),
            "data_warnings": data_warnings,
            "source_details": {
                "air_quality": air_data,
                "yellow_dust": yellow_dust_data,
                "health_index": health_data,
                "living_index": living_data,
                "temperature": temp_data,
                "uv_index": uv_data,
                "precipitation": pop_data,
                "forecast_signals": notice_data.get("forecast_signals", {}),
            }
        }
    
    def collect_daily_weather_forecast(
        self,
        region: str = "서울",
        include_elapsed: bool = False,
        forecast_type: str = "daily_forecast"
    ) -> Dict:
        """오늘 하루 예보의 대표 위험값을 모은 서울 기상 알림 데이터 생성"""
        grid = KMAForecastAPIClient.SEOUL_GRID
        air_data = self.airkorea_api.get_air_quality(region) or {}
        air_quality_error = self.airkorea_api.last_error
        yellow_dust_data = self.airkorea_api.get_yellow_dust_advisory(region) or {}
        yellow_dust_error = self.airkorea_api.last_error if not yellow_dust_data else None
        health_data = self.weather_api.get_health_index(grid["area_no"]) or {}
        living_data = self.weather_api.get_living_index(grid["area_no"]) or {}
        living_error = self.weather_api.last_error
        uv_data = self.weather_api.get_uv_index(grid["area_no"]) or {}
        uv_error = self.weather_api.last_error
        today_forecast = self.forecast_api.get_today_forecast_summary(
            grid["nx"],
            grid["ny"],
            include_elapsed=include_elapsed
        ) or {}
        
        living_forecast_values = [
            value for value in living_data.get("feels_like_temp_forecast", {}).values()
            if value is not None
        ]
        feels_like_temp = max(living_forecast_values) if living_forecast_values else living_data.get("feels_like_temp")
        feels_like_source = "living_weather_index_today_max"
        if feels_like_temp is None:
            feels_like_temp = today_forecast.get("feels_like_temp")
            feels_like_source = "today_max_forecast_temperature"
        
        data_warnings = {}
        if not air_data:
            data_warnings["air_quality"] = air_quality_error or "대기질 데이터 없음"
        if (
            yellow_dust_data.get("yellow_dust_status") == "unavailable" and
            air_data.get("yellow_dust") is None
        ):
            data_warnings["yellow_dust"] = yellow_dust_data.get("yellow_dust_message")
        elif not yellow_dust_data:
            data_warnings["yellow_dust"] = yellow_dust_error or "황사 발생정보 데이터 없음"
        if not living_data:
            if feels_like_temp is None:
                data_warnings["living_index"] = living_error or "생활기상지수 데이터 없음"
            else:
                data_warnings["living_index"] = "생활기상지수 API 응답 없음, 오늘 최고 예보 기온으로 대체"
        if not uv_data:
            data_warnings["uv_index"] = uv_error or "자외선지수 데이터 없음"
        if not today_forecast:
            data_warnings["today_forecast"] = self.forecast_api.last_error or "오늘 단기예보 요약 데이터 없음"
        
        # 황사는 발생정보 API 결과만 쓴다. 예전에는 권한이 없을 때 PM10 평균을
        # 그대로 복사했는데, 황사 판정의 "좋음" 임계가 150이라 서울 PM10 평균으로는
        # 사실상 항상 좋음이 나왔다. 감시되는 것처럼 보이지만 실제로는 죽은 지표였다.
        # 권한이 없으면 결측(None)으로 두고 컨슈머가 "정보없음"으로 표시한다.
        yellow_dust = yellow_dust_data.get("yellow_dust")
        yellow_dust_source = yellow_dust_data.get("yellow_dust_source")
        
        return {
            "timestamp": now_kst().isoformat(),
            "region": region,
            "data_type": "current_weather",
            "forecast_type": forecast_type,
            "forecast_period": today_forecast.get("forecast_period", "today_remaining"),
            "forecast_date": today_forecast.get("forecast_date", now_kst().strftime("%Y%m%d")),
            "area_no": grid["area_no"],
            "nx": grid["nx"],
            "ny": grid["ny"],
            "latitude": grid["latitude"],
            "longitude": grid["longitude"],
            "oak_pollen": health_data.get("oak_pollen"),
            "pine_pollen": health_data.get("pine_pollen"),
            "pm10": air_data.get("pm10"),
            "pm25": air_data.get("pm25"),
            "yellow_dust": yellow_dust,
            "yellow_dust_advisory": yellow_dust_data.get("yellow_dust_advisory"),
            "yellow_dust_source": yellow_dust_source,
            "feels_like_temp": feels_like_temp,
            "feels_like_temp_source": feels_like_source,
            "min_temperature": today_forecast.get("temperature_summary", {}).get("min_temperature"),
            "max_temperature": today_forecast.get("temperature_summary", {}).get("max_temperature"),
            "other_special_notice": today_forecast.get("other_special_notice", "정보없음"),
            "other_special_notice_source": today_forecast.get("other_special_notice_source"),
            "precipitation_probability": today_forecast.get("precipitation_probability"),
            "uv_index": uv_data.get("uv_index"),
            "data_warnings": data_warnings,
            "source_details": {
                "air_quality_reference": air_data,
                "yellow_dust": yellow_dust_data,
                "health_index": health_data,
                "living_index": living_data,
                "uv_index": uv_data,
                "today_forecast": today_forecast,
                "temperature": today_forecast.get("temperature_summary", {}),
                "precipitation": today_forecast.get("precipitation_summary", {}),
                "forecast_signals": today_forecast.get("forecast_signals", {}),
            }
        }
    
    def collect_morning_weather_summary(self, region: str = "서울") -> Dict:
        """06시용: 오늘 주요 예보값과 나머지 실시간 기준을 섞은 알림 데이터 생성"""
        grid = KMAForecastAPIClient.SEOUL_GRID
        current_data = self.collect_current_weather(region)
        today_forecast = self.forecast_api.get_today_forecast_summary(
            grid["nx"],
            grid["ny"],
            include_elapsed=True
        ) or {}
        temperature_summary = today_forecast.get("temperature_summary", {})
        
        current_data.update({
            "forecast_type": "morning_mixed",
            "forecast_period": "today_full_with_current_conditions",
            "forecast_date": today_forecast.get("forecast_date", now_kst().strftime("%Y%m%d")),
            "min_temperature": temperature_summary.get("min_temperature"),
            "max_temperature": temperature_summary.get("max_temperature"),
            "precipitation_probability": today_forecast.get("precipitation_probability"),
            "precipitation_probability_source": "today_max_forecast_probability",
        })
        current_data.setdefault("source_details", {})
        current_data["source_details"]["today_forecast"] = today_forecast
        current_data["source_details"]["temperature"] = temperature_summary
        current_data["source_details"]["precipitation"] = today_forecast.get("precipitation_summary", {})
        if not today_forecast:
            current_data.setdefault("data_warnings", {})
            current_data["data_warnings"]["today_forecast"] = (
                self.forecast_api.last_error or "오늘 단기예보 요약 데이터 없음"
            )
        
        return current_data
    
    def collect_scheduled_weather(self, region: str = "서울", run_hour: Optional[int] = None) -> Dict:
        """매시간 스케줄의 실행 시각에 맞는 알림 데이터 생성"""
        hour = now_kst().hour if run_hour is None else run_hour
        if hour == 0:
            return self.collect_daily_weather_forecast(
                region=region,
                include_elapsed=True,
                forecast_type="daily_full_forecast"
            )
        if hour == 6:
            return self.collect_morning_weather_summary(region=region)
        if hour in (12, 18):
            data = self.collect_current_weather(region=region)
            data["forecast_type"] = "current_conditions"
            data["forecast_period"] = "current"
            return data
        
        logger.info(f"정의되지 않은 실행 시각({hour}시): 현재 기준 알림으로 처리합니다.")
        data = self.collect_current_weather(region=region)
        data["forecast_type"] = "current_conditions"
        data["forecast_period"] = "current"
        return data
    
    def collect_and_publish(self, region: str = "서울") -> Dict[str, bool]:
        """
        모든 기상 데이터를 수집하고 Kafka로 발행
        
        Args:
            region: 지역명
        
        Returns:
            {
                "current_weather": True
            }
        """
        logger.info(f"기상 데이터 수집 시작: {region}")
        current_weather = self.collect_scheduled_weather(region)
        results = {
            "current_weather": self.producer.send_current_weather(current_weather)
        }
        
        # 모든 메시지 발행 완료 대기
        self.producer.flush()
        
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"기상 데이터 수집 완료: {success_count}/{len(results)} 성공")
        
        return results
    
    def close(self):
        """리소스 정리"""
        self.producer.close()


def main():
    """
    메인 실행 함수
    Airflow DAG에서 호출될 함수
    """
    try:
        collector = WeatherDataCollector()
        results = collector.collect_and_publish(region="서울")
        
        print("\n" + "=" * 80)
        print("기상 데이터 수집 결과")
        print("=" * 80)
        for data_type, success in results.items():
            status = "✅ 성공" if success else "❌ 실패"
            print(f"{data_type}: {status}")
        
        collector.close()
        
        return results
        
    except Exception as e:
        logger.error(f"프로듀서 실행 중 오류: {str(e)}")
        return {
            "current_weather": False
        }


if __name__ == "__main__":
    # 테스트 코드
    print("=" * 80)
    print("기상지수 프로듀서 테스트")
    print("=" * 80)
    
    # API 키 없을 때 테스트 (더미 데이터)
    print("\n📝 테스트 모드: 실제 API 호출 대신 샘플 데이터 사용")
    print("-" * 80)
    
    # 더미 데이터로 프로듀서 테스트
    producer = KafkaWeatherProducer()

    # 샘플 현재 기상 통합 데이터
    sample_weather = {
        "timestamp": now_kst().isoformat(),
        "region": "서울",
        "data_type": "current_weather",
        "pm10": 45,
        "pm25": 20,
        "feels_like_temp": 18,
        "precipitation_probability": 30,
        "uv_index": 5,
    }

    print("\n발행할 샘플 데이터:")
    print(json.dumps(sample_weather, ensure_ascii=False, indent=2))

    # Kafka 발행 테스트
    if producer.send_current_weather(sample_weather):
        print("\n✅ Kafka 발행 성공")
    else:
        print("\n❌ Kafka 발행 실패 (Kafka 서버가 실행 중인지 확인하세요)")

    producer.close()
    
    print("\n" + "=" * 80)
