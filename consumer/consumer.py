"""
기상지수 데이터 컨슈머

Kafka 토픽에서 메시지를 구독하여:
1. rules.py로 기상지수 판정 (등급 분류)
2. alert.py로 알림 발송 (콘솔, Slack, OpenSearch)
3. OpenSearch에 알림 이력 저장

지속적으로 토픽을 모니터링하며 실시간 알림을 제공합니다.
"""

import os
import json
import logging
import time
from typing import Dict, Optional, List
from datetime import datetime
from kafka import KafkaConsumer
from opensearchpy import OpenSearch
from dotenv import load_dotenv

# 상대 import
try:
    from .rules import (
        AlertRuleEngine, AlertGrouping, AlertLevel,
        grade_signature, should_send, core_indices_unknown,
    )
    from .alert import AlertManager
except ImportError:
    # 직접 실행 시
    from rules import (
        AlertRuleEngine, AlertGrouping, AlertLevel,
        grade_signature, should_send, core_indices_unknown,
    )
    from alert import AlertManager

# 환경변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OpenSearchConnector:
    """OpenSearch 연결 관리"""
    
    def __init__(self):
        """OpenSearch 클라이언트 초기화"""
        try:
            host = os.getenv("OPENSEARCH_HOST", "localhost")
            port = int(os.getenv("OPENSEARCH_PORT", 9200))
            
            self.client = OpenSearch(
                hosts=[{"host": host, "port": port}],
                http_auth=None,  # 보안 비활성화 (개발용)
                use_ssl=False,
                verify_certs=False,
                ssl_show_warn=False
            )
            
            # 연결 테스트
            info = self.client.info()
            logger.info(f"OpenSearch 연결 성공: {host}:{port}")
            
        except Exception as e:
            logger.error(f"OpenSearch 연결 실패: {str(e)}")
            self.client = None
    
    def is_connected(self) -> bool:
        """OpenSearch 연결 상태 확인"""
        return self.client is not None


class WeatherDataProcessor:
    """기상 데이터 처리 및 판정"""
    
    @staticmethod
    def _safe_float(value, default: Optional[float] = None) -> Optional[float]:
        """결측은 결측으로 남긴다.

        기본값을 0으로 두면 프로듀서가 정직하게 보낸 None이 실측값 0으로
        둔갑한다. 판정 규칙은 None을 받으면 UNKNOWN을 돌려주므로 결측이
        등급 체계 안에서 그대로 표현된다.
        """
        if value in (None, "", "-"):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    @staticmethod
    def process_current_weather(data: Dict) -> Dict:
        """서울 현재 기상 통합 데이터 처리"""
        timestamp = data.get("timestamp", datetime.now().isoformat())
        region = data.get("region", "서울")
        
        oak_pollen_val = WeatherDataProcessor._safe_float(data.get("oak_pollen"))
        pine_pollen_val = WeatherDataProcessor._safe_float(data.get("pine_pollen"))
        pm10_val = WeatherDataProcessor._safe_float(data.get("pm10"))
        pm25_val = WeatherDataProcessor._safe_float(data.get("pm25"))
        dust_val = WeatherDataProcessor._safe_float(data.get("yellow_dust"))
        feels_like_val = WeatherDataProcessor._safe_float(data.get("feels_like_temp"))
        special_notice_val = data.get("other_special_notice")
        precipitation_val = WeatherDataProcessor._safe_float(data.get("precipitation_probability"))
        uv_val = WeatherDataProcessor._safe_float(data.get("uv_index"))
        
        oak_level, oak_rec, oak_emoji = AlertRuleEngine.classify_pollen(oak_pollen_val)
        pine_level, pine_rec, pine_emoji = AlertRuleEngine.classify_pollen(pine_pollen_val)
        pm10_level, pm10_rec, pm10_emoji = AlertRuleEngine.classify_pm10(pm10_val)
        pm25_level, pm25_rec, pm25_emoji = AlertRuleEngine.classify_pm25(pm25_val)
        dust_level, dust_rec, dust_emoji = AlertRuleEngine.classify_dust(dust_val)
        feels_level, feels_rec, feels_emoji = AlertRuleEngine.classify_feels_like_temp(feels_like_val)
        special_level, special_rec, special_emoji = AlertRuleEngine.classify_special_notice(special_notice_val)
        precipitation_level, precipitation_rec, precipitation_emoji = (
            AlertRuleEngine.classify_precipitation_probability(precipitation_val)
        )
        uv_level, uv_rec, uv_emoji = AlertRuleEngine.classify_uv_index(uv_val)
        
        return {
            "timestamp": timestamp,
            "region": region,
            "data_type": "current_weather",
            "indices": {
                "oak_pollen": oak_pollen_val,
                "pine_pollen": pine_pollen_val,
                "pm10": pm10_val,
                "pm25": pm25_val,
                "dust": dust_val,
                "feels_like_temp": feels_like_val,
                "other_special_notice": special_notice_val,
                "precipitation_probability": precipitation_val,
                "uv_index": uv_val
            },
            "levels": {
                "oak_pollen_level": oak_level.value,
                "pine_pollen_level": pine_level.value,
                "pm10_level": pm10_level.value,
                "pm25_level": pm25_level.value,
                "dust_level": dust_level.value,
                "feels_like_temp_level": feels_level.value,
                "other_special_notice_level": special_level.value,
                "precipitation_probability_level": precipitation_level.value,
                "uv_index_level": uv_level.value
            },
            "recommendations": {
                "oak_pollen_rec": oak_rec,
                "pine_pollen_rec": pine_rec,
                "pm10_rec": pm10_rec,
                "pm25_rec": pm25_rec,
                "dust_rec": dust_rec,
                "feels_like_temp_rec": feels_rec,
                "other_special_notice_rec": special_rec,
                "precipitation_probability_rec": precipitation_rec,
                "uv_index_rec": uv_rec
            },
            "emojis": {
                "oak_pollen_emoji": oak_emoji,
                "pine_pollen_emoji": pine_emoji,
                "pm10_emoji": pm10_emoji,
                "pm25_emoji": pm25_emoji,
                "dust_emoji": dust_emoji,
                "feels_like_temp_emoji": feels_emoji,
                "other_special_notice_emoji": special_emoji,
                "precipitation_probability_emoji": precipitation_emoji,
                "uv_index_emoji": uv_emoji
            },
            "classification_objects": {
                "oak_pollen": oak_level,
                "pine_pollen": pine_level,
                "pm10": pm10_level,
                "pm25": pm25_level,
                "dust": dust_level,
                "feels_like_temp": feels_level,
                "other_special_notice": special_level,
                "precipitation_probability": precipitation_level,
                "uv_index": uv_level
            },
            "source_details": data.get("source_details", {})
        }
    

class KafkaWeatherConsumer:
    """Kafka 컨슈머"""
    
    def __init__(
        self,
        bootstrap_servers: str = None,
        group_id: str = "weather-alert-group",
        topics: List[str] = None,
        opensearch_client: Optional[OpenSearch] = None
    ):
        """
        Kafka 컨슈머 초기화
        
        Args:
            bootstrap_servers: Kafka 브로커 주소
            group_id: 컨슈머 그룹 ID
            topics: 구독할 토픽 리스트
            opensearch_client: OpenSearch 클라이언트
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.group_id = group_id
        self.topics = topics or ["seoul-weather"]
        self.opensearch_client = opensearch_client
        
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                max_poll_records=1
            )
            logger.info(f"Kafka 컨슈머 초기화 성공: {self.bootstrap_servers}")
            
        except Exception as e:
            logger.error(f"Kafka 컨슈머 초기화 실패: {str(e)}")
            self.consumer = None
    
    def consume_messages(self, timeout_ms: int = 5000) -> Optional[Dict]:
        """
        메시지 구독 (non-blocking)
        
        Args:
            timeout_ms: 대기 시간 (밀리초)
        
        Returns:
            메시지 또는 None
        """
        if not self.consumer:
            logger.warning("Kafka 컨슈머가 초기화되지 않음")
            return None
        
        try:
            messages = self.consumer.poll(timeout_ms=timeout_ms)
            
            for topic_partition, records in messages.items():
                for record in records:
                    return record.value  # 첫 번째 메시지만 반환
            
            return None
            
        except Exception as e:
            logger.error(f"메시지 구독 오류: {str(e)}")
            return None
    
    def close(self):
        """컨슈머 종료"""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka 컨슈머 종료")


class WeatherAlertConsumer:
    """기상지수 알림 컨슈머 (메인 로직)"""
    
    def __init__(self):
        """컨슈머 초기화"""
        # OpenSearch 연결
        self.opensearch_connector = OpenSearchConnector()
        
        # Kafka 컨슈머
        self.kafka_consumer = KafkaWeatherConsumer(
            opensearch_client=self.opensearch_connector.client
        )
        
        # 알림 매니저
        self.alert_manager = AlertManager(
            opensearch_client=self.opensearch_connector.client
        )
        
        # 데이터 프로세서
        self.processor = WeatherDataProcessor()
        
        # 통계
        self.stats = {
            "total_processed": 0,
            "total_alerts_sent": 0,
            "errors": 0
        }
    
    def process_message(self, message: Dict) -> bool:
        """
        단일 메시지 처리
        
        Args:
            message: Kafka 메시지
        
        Returns:
            bool: 처리 성공 여부
        """
        try:
            if not message:
                return False
            
            logger.info(f"메시지 수신: {json.dumps(message, ensure_ascii=False)}")

            # seoul-weather 토픽은 현재 기상 통합 데이터만 발행된다
            processed = self.processor.process_current_weather(message)

            # 알림 그룹 생성
            alert_groups = AlertGrouping.group_alerts(
                processed.get("classification_objects", {})
            )
            processed["action_groups"] = alert_groups
            processed["data_warnings"] = message.get("data_warnings", {})
            processed["raw_data"] = message

            # 쿨다운/중복제거: 등급 시그니처가 직전과 바뀔 때만 외부 채널 발송.
            # 직전 상태는 재시작에도 살아남는 OpenSearch에서 읽는다(fail-open).
            classification_objects = processed.get("classification_objects", {})

            # 핵심 지수가 전부 결측이면 사용자에게 보낼 근거가 없다.
            # 콘솔/OpenSearch 이력은 남겨 운영자가 결측을 추적할 수 있게 한다.
            core_missing = core_indices_unknown(classification_objects)
            if core_missing:
                logger.warning(
                    "핵심 지수(미세먼지/초미세먼지) 전량 결측 → 외부 채널 발송 보류. "
                    f"data_warnings={message.get('data_warnings', {})}"
                )

            current_signature = grade_signature(classification_objects)
            processed["grade_signature"] = current_signature
            previous_signature = self.alert_manager.opensearch_sender.latest_signature(
                processed.get("region", "전국")
            )
            send_external = should_send(previous_signature, current_signature) and not core_missing
            if not send_external:
                logger.info(f"등급 무변경 → 외부 채널 발송 생략 (시그니처={current_signature})")

            # 알림 발송 (등급 무변경 시 콘솔/OpenSearch 이력은 유지, 외부만 생략)
            results = self.alert_manager.send_all(processed, send_external=send_external)

            self.stats["total_processed"] += 1
            # 콘솔은 항상 성공하므로 카운터에 넣으면 외부 발송이 전멸해도 증가한다.
            if any(results.get(channel) for channel in ("slack", "email", "kakao")):
                self.stats["total_alerts_sent"] += 1
            
            logger.info(f"메시지 처리 완료: {processed.get('region')}")
            return True
            
        except Exception as e:
            logger.error(f"메시지 처리 오류: {str(e)}")
            self.stats["errors"] += 1
            return False
    
    def run_once(self) -> bool:
        """
        한 번의 메시지 처리 실행
        (Airflow task 호출용)
        
        Returns:
            bool: 메시지 처리 여부
        """
        message = self.kafka_consumer.consume_messages(timeout_ms=5000)
        
        if message:
            return self.process_message(message)
        else:
            logger.debug("대기 중인 메시지 없음")
            return False
    
    def run_continuous(self, duration_seconds: int = 3600, poll_interval: int = 10):
        """
        지속적인 메시지 처리 (배경 프로세스)
        
        Args:
            duration_seconds: 실행 시간 (기본값: 1시간)
            poll_interval: 폴링 간격 (초)
        """
        start_time = time.time()
        
        logger.info(f"컨슈머 시작: {duration_seconds}초 동안 실행")
        logger.info(f"구독 토픽: {self.kafka_consumer.topics}")
        
        try:
            while time.time() - start_time < duration_seconds:
                try:
                    self.run_once()
                    time.sleep(poll_interval)
                    
                except KeyboardInterrupt:
                    logger.info("사용자 중단")
                    break
                except Exception as e:
                    logger.error(f"실행 중 오류: {str(e)}")
                    self.stats["errors"] += 1
                    time.sleep(poll_interval)
        
        finally:
            self._shutdown()
    
    def _shutdown(self):
        """정상 종료 처리"""
        logger.info("컨슈머 종료 중...")
        self.kafka_consumer.close()
        
        # 최종 통계 출력
        print("\n" + "=" * 80)
        print("컨슈머 종료 통계")
        print("=" * 80)
        print(f"처리된 메시지: {self.stats['total_processed']}")
        print(f"발송된 알림: {self.stats['total_alerts_sent']}")
        print(f"발생한 오류: {self.stats['errors']}")
        print("=" * 80 + "\n")
    
    def print_stats(self):
        """통계 출력"""
        print(f"\n현재 통계:")
        print(f"  처리된 메시지: {self.stats['total_processed']}")
        print(f"  발송된 알림: {self.stats['total_alerts_sent']}")
        print(f"  발생한 오류: {self.stats['errors']}\n")


def main():
    """
    메인 실행 함수
    
    Docker에서 실행될 진입점
    """
    consumer = WeatherAlertConsumer()
    
    # 1시간 동안 지속적으로 메시지 처리
    consumer.run_continuous(duration_seconds=3600, poll_interval=10)


if __name__ == "__main__" and os.getenv("RUN_CONSUMER_TESTS", "false").lower() != "true":
    main()


if __name__ == "__main__" and os.getenv("RUN_CONSUMER_TESTS", "false").lower() == "true":
    # 테스트 코드
    print("=" * 80)
    print("기상지수 알림 컨슈머 테스트")
    print("=" * 80)
    
    # 컨슈머 생성
    consumer = WeatherAlertConsumer()
    
    # 테스트 메시지 (실제 Kafka 메시지 형식)
    test_messages = [
        {
            "timestamp": datetime.now().isoformat(),
            "region": "서울",
            "pm10": 120,
            "pm25": 45,
            "o3": 0.065,
            "no2": 0.045,
            "so2": 0.005,
            "co": 0.5,
            "data_type": "air_quality"
        },
        {
            "timestamp": datetime.now().isoformat(),
            "region": "서울",
            "uv_index": 8,
            "data_type": "uv_index"
        },
        {
            "timestamp": datetime.now().isoformat(),
            "region": "서울",
            "data_type": "current_weather",
            "oak_pollen": 3,
            "pine_pollen": 2,
            "pm10": 120,
            "pm25": 45,
            "yellow_dust": 120,
            "feels_like_temp": 24,
            "other_special_notice": "없음",
            "precipitation_probability": 40,
            "uv_index": 8
        }
    ]
    
    print("\n📝 테스트 메시지 처리:")
    print("-" * 80)
    
    for i, msg in enumerate(test_messages, 1):
        print(f"\n테스트 메시지 {i}/{len(test_messages)}:")
        consumer.process_message(msg)
    
    consumer.print_stats()
    consumer._shutdown()
