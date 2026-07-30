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
from typing import Dict, Optional
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
    from .timeutil import now_kst
except ImportError:  # 직접 실행 시
    from timeutil import now_kst


try:
    from .masking import install_secret_filter
except ImportError:  # 직접 실행 시
    from masking import install_secret_filter

# 값 생성 지점 마스킹의 안전망. 이후 누가 URL을 그대로 로깅해도 출력 단계에서 걸린다.
install_secret_filter()



# 클라이언트는 producer/clients/로 분리됐다. 기존 import 경로
# (from producer.producer import WeatherAPIClient ...)는 그대로 동작한다.
from .clients import AirKoreaAPIClient, KMAForecastAPIClient, WeatherAPIClient  # noqa: E402


class KafkaWeatherProducer:
    """Kafka 프로듀서"""
    
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic_current_weather: str = None
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
        # 컨슈머와 같은 환경변수를 읽는다 — 토픽명 단일 출처
        self.topic_current_weather = topic_current_weather or os.getenv(
            "KAFKA_TOPIC", "seoul-weather"
        )
        
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
    
    def close(self):
        """리소스 정리.

        예전에는 여기서 항상 만들어지던 KafkaProducer를 닫았다. 수집만 하는
        DAG 태스크까지 브로커 연결을 열었고, Kafka가 죽으면 수집도 실패했다.
        발행은 KafkaWeatherProducer를 직접 쓰는 발행 태스크의 일이다.
        클라이언트들은 세션 없는 requests 호출이라 정리할 자원이 없다.
        """
