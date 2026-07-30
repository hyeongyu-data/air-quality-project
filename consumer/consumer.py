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
import signal
import time
from pathlib import Path
from typing import Dict, Optional, List
from kafka import KafkaConsumer, KafkaProducer
from opensearchpy import OpenSearch
from dotenv import load_dotenv

# 상대 import
try:  # noqa: SIM105
    from . import opensearch_setup
    from .logutil import current_event_id, setup_logging
    from .metrics import MetricsSink, build_message_metrics
    from .schema import InvalidMessage, parse_message
    from .rules import (
        AlertRuleEngine, AlertGrouping,
        grade_signature, should_send, core_indices_unknown,
    )
    from .alert import AlertManager
except ImportError:
    # 직접 실행 시
    import opensearch_setup
    from logutil import current_event_id, setup_logging
    from metrics import MetricsSink, build_message_metrics
    from schema import InvalidMessage, parse_message
    from rules import (
        AlertRuleEngine, AlertGrouping, grade_signature, should_send, core_indices_unknown,
    )
    from alert import AlertManager

# 환경변수 로드
load_dotenv()

# LOG_FORMAT=json이면 구조화 로그. 필드 단위 집계가 가능해진다.
setup_logging()

logger = logging.getLogger(__name__)

try:
    from .timeutil import now_kst
except ImportError:  # 직접 실행 시
    from timeutil import now_kst



class OpenSearchConnector:
    """OpenSearch 연결 관리.

    기동 시 한 번 실패하면 client=None으로 고정되던 구조를 고쳤다. 그 상태에서는
    쿨다운 조회가 항상 None을 돌려줘 매 메시지마다 전 채널로 발송되고, 이력도
    한 건도 남지 않는다. 재연결을 시도하되 백오프로 폭주하지 않게 한다.
    """

    # 연속 실패 시 대기 시간 상한. 매 메시지마다 재시도하면 처리가 느려진다.
    MAX_RETRY_INTERVAL_SECONDS = 300

    def __init__(self):
        self.host = os.getenv("OPENSEARCH_HOST", "localhost")
        self.port = int(os.getenv("OPENSEARCH_PORT", 9200))
        self.client = None
        self._next_retry_at = 0.0
        self._retry_interval = 5.0
        self.connect()

    def _build_client(self) -> OpenSearch:
        return OpenSearch(
            hosts=[{"host": self.host, "port": self.port}],
            http_auth=None,  # 보안 비활성화 (개발용)
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False,
        )

    def connect(self) -> bool:
        """연결을 시도한다. 성공하면 인덱스 템플릿·보존 정책을 적용한다."""
        try:
            client = self._build_client()
            client.info()
            self.client = client
            self._retry_interval = 5.0
            logger.info(f"OpenSearch 연결 성공: {self.host}:{self.port}")
            opensearch_setup.bootstrap(client)
            return True
        except Exception as e:
            self.client = None
            self._next_retry_at = time.time() + self._retry_interval
            logger.error(
                f"OpenSearch 연결 실패({self._retry_interval:.0f}초 뒤 재시도): {str(e)}"
            )
            self._retry_interval = min(
                self._retry_interval * 2, self.MAX_RETRY_INTERVAL_SECONDS
            )
            return False

    def ensure_connection(self) -> bool:
        """끊겨 있으면 백오프 간격이 지난 뒤 다시 연결을 시도한다."""
        if self.client is not None:
            return True
        if time.time() < self._next_retry_at:
            return False
        return self.connect()

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
        timestamp = data.get("timestamp", now_kst().isoformat())
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
    """Kafka 컨슈머.

    기동 시 브로커가 없으면 consumer=None으로 고정되던 구조를 고쳤다.
    그 상태로는 다음 재시작 전까지 아무것도 소비하지 못한다. 재연결을
    시도하되 지수 백오프로 폭주하지 않게 한다.
    """

    MAX_RETRY_INTERVAL_SECONDS = 300

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
        # 토픽명 단일 출처. 프로듀서와 같은 환경변수를 읽어 오타가 나도
        # 양쪽이 같은 토픽을 본다(auto-create 환경에서 서로 다른 빈 토픽을
        # 보며 조용히 동작하던 사고 방지).
        self.topics = topics or [os.getenv("KAFKA_TOPIC", "seoul-weather")]
        self.opensearch_client = opensearch_client
        self.consumer = None
        self._next_retry_at = 0.0
        self._retry_interval = 5.0
        self.connect()

    def connect(self) -> bool:
        """브로커 연결을 시도한다."""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                # 역직렬화는 처리 단계(schema.parse_message)에서 한다.
                # 여기 두면 깨진 메시지가 poll() 안에서 터지는데, 수동 커밋에서는
                # 오프셋이 전진하지 않아 같은 메시지를 영원히 받는 poison pill이 된다.
                value_deserializer=None,
                # earliest: 컨슈머가 죽어 있던 동안 쌓인 메시지를 건너뛰지 않는다.
                # latest면 재구독 시 백로그가 조용히 사라진다. 중복은 등급
                # 시그니처 쿨다운과 event_id 기반 upsert가 흡수한다.
                auto_offset_reset='earliest',
                # 처리(판정·저장·발송)가 끝난 뒤에만 커밋한다. 자동 커밋은
                # 처리 성공과 무관하게 오프셋을 전진시켜 실패 메시지를 유실했다.
                enable_auto_commit=False,
                max_poll_records=10
            )
            self._retry_interval = 5.0
            logger.info(f"Kafka 컨슈머 초기화 성공: {self.bootstrap_servers}")
            return True
        except Exception as e:
            self.consumer = None
            self._next_retry_at = time.time() + self._retry_interval
            logger.error(
                f"Kafka 컨슈머 초기화 실패({self._retry_interval:.0f}초 뒤 재시도): {str(e)}"
            )
            self._retry_interval = min(
                self._retry_interval * 2, self.MAX_RETRY_INTERVAL_SECONDS
            )
            return False

    def ensure_connection(self) -> bool:
        """끊겨 있으면 백오프 간격이 지난 뒤 다시 연결을 시도한다."""
        if self.consumer is not None:
            return True
        if time.time() < self._next_retry_at:
            return False
        return self.connect()
    
    def consume_batch(self, timeout_ms: int = 5000) -> List:
        """한 번의 poll이 돌려준 레코드 전부를 반환한다.

        예전에는 첫 레코드만 반환하고 나머지를 버렸다. max_poll_records=1이라
        사고가 안 났을 뿐, 그 설정을 바꾸는 순간 조용한 유실이었다.
        """
        if not self.consumer:
            logger.warning("Kafka 컨슈머가 초기화되지 않음")
            return []

        try:
            polled = self.consumer.poll(timeout_ms=timeout_ms)
            return [record for records in polled.values() for record in records]
        except Exception as e:
            logger.error(f"메시지 구독 오류: {str(e)}")
            return []

    def commit(self) -> bool:
        """현재 poll 위치까지 오프셋을 커밋한다."""
        if not self.consumer:
            return False
        try:
            self.consumer.commit()
            return True
        except Exception as e:
            logger.error(f"오프셋 커밋 실패(배치 재처리됨): {str(e)}")
            return False
    
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

        # 처리 메트릭 (OpenSearch weather-metrics-*, best-effort)
        self.metrics = MetricsSink(self.opensearch_connector.client)
        
        # 데이터 프로세서
        self.processor = WeatherDataProcessor()

        # 등급이 같아도 이 시간(초)이 지나면 재발송한다. 0이면 끈다.
        # 조용한 것이 정상(쿨다운)인지 고장(파이프라인 정지)인지 사용자가
        # 구분할 수 있게 하는 최소 장치다.
        self.max_silence_seconds = float(
            os.getenv("MAX_SILENCE_HOURS", "12")
        ) * 3600

        # 루프 제어 / 생존 신호
        self._running = False
        self.heartbeat_path = Path(
            os.getenv("CONSUMER_HEARTBEAT_PATH", "/tmp/consumer-heartbeat")
        )
        
        # DLQ — 계약 위반·처리 불가 메시지의 격리처
        self.dlq_topic = os.getenv(
            "KAFKA_DLQ_TOPIC", f"{os.getenv('KAFKA_TOPIC', 'seoul-weather')}-dlq"
        )
        self._dlq_producer = None

        # 통계
        self.stats = {
            "total_processed": 0,
            "total_alerts_sent": 0,
            "errors": 0,
            "dlq": 0,
        }
    
    def process_message(self, message: Dict) -> bool:
        """
        단일 메시지 처리
        
        Args:
            message: Kafka 메시지
        
        Returns:
            bool: 처리 성공 여부
        """
        started = time.monotonic()
        # 이 메시지를 처리하는 동안 찍히는 모든 로그에 event_id가 붙는다.
        token = current_event_id.set(message.get("event_id") if message else None)
        try:
            if not message:
                return False
            
            # 페이로드 전문을 찍으면 로그 볼륨이 커지고, data_warnings에 실려 온
            # 외부 API 오류 문자열까지 그대로 남는다. 식별에 필요한 것만 남긴다.
            logger.info(
                "메시지 수신: region=%s timestamp=%s warnings=%s",
                message.get("region"), message.get("timestamp"),
                list(message.get("data_warnings", {}).keys()),
            )

            # seoul-weather 토픽은 현재 기상 통합 데이터만 발행된다
            processed = self.processor.process_current_weather(message)

            # 알림 그룹 생성
            alert_groups = AlertGrouping.group_alerts(
                processed.get("classification_objects", {})
            )
            processed["action_groups"] = alert_groups
            processed["data_warnings"] = message.get("data_warnings", {})
            processed["raw_data"] = message
            # 재처리·재시도 시 같은 문서를 덮어쓰도록 이벤트 키를 넘긴다.
            processed["event_id"] = message.get("event_id")

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
            state = self.alert_manager.opensearch_sender.cooldown_state(
                processed.get("region", "전국")
            )
            send_external = should_send(
                state.get("grade_signature"),
                current_signature,
                last_sent_at=state.get("last_external_send_at"),
                max_silence_seconds=self.max_silence_seconds,
            ) and not core_missing
            if not send_external:
                logger.info(f"등급 무변경 → 외부 채널 발송 생략 (시그니처={current_signature})")
            elif state.get("grade_signature") == current_signature:
                logger.info(
                    "등급은 같지만 최대 무발송 간격 초과 → 재발송 "
                    f"(마지막 발송 {state.get('last_external_send_at')})"
                )

            # 알림 발송 (등급 무변경 시 콘솔/OpenSearch 이력은 유지, 외부만 생략)
            results = self.alert_manager.send_all(processed, send_external=send_external)

            self.stats["total_processed"] += 1
            # 콘솔은 항상 성공하므로 카운터에 넣으면 외부 발송이 전멸해도 증가한다.
            if any(results.get(channel) for channel in ("slack", "email", "kakao")):
                self.stats["total_alerts_sent"] += 1
            
            duration_ms = (time.monotonic() - started) * 1000
            processed["alert_severity"] = (
                self.alert_manager.opensearch_sender._calculate_severity(processed)
            )
            self.metrics.emit(build_message_metrics(
                processed, results, send_external, duration_ms
            ))
            logger.info(
                "메시지 처리 완료: %s",
                processed.get("region"),
                extra={
                    "metric_duration_ms": round(duration_ms, 1),
                    "metric_delivered": [c for c in ("slack", "email", "kakao") if results.get(c)],
                    "metric_suppressed": not send_external,
                },
            )
            return True

        except Exception:
            # 예외는 handle_record가 받아 DLQ로 격리한다. 여기서 삼키면
            # 실패 메시지가 커밋되어 조용히 유실된다(예전 동작).
            self.stats["errors"] += 1
            raise
        finally:
            current_event_id.reset(token)
    
    def _send_to_dlq(self, record, reason: str) -> bool:
        """처리 불가 메시지를 DLQ 토픽으로 격리한다.

        DLQ 발행이 실패하면 False — 호출측이 커밋을 보류해 배치가 재처리된다.
        메시지를 버리는 것보다 재시도가 낫다.
        """
        try:
            if self._dlq_producer is None:
                self._dlq_producer = KafkaProducer(
                    bootstrap_servers=self.kafka_consumer.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    acks="all",
                )
            self._dlq_producer.send(self.dlq_topic, {
                "reason": reason,
                "source_topic": record.topic,
                "source_offset": record.offset,
                "failed_at": now_kst().isoformat(),
                "raw": record.value.decode("utf-8", errors="replace") if record.value else None,
            }).get(timeout=5)
            self.stats["dlq"] += 1
            logger.warning(f"DLQ 격리: offset={record.offset} 사유={reason}")
            return True
        except Exception as e:
            logger.error(f"DLQ 발행 실패(커밋 보류, 재처리 예정): {str(e)}")
            return False

    def handle_record(self, record) -> str:
        """레코드 하나를 처리하고 결과를 돌려준다: ok | dlq | dlq_failed"""
        try:
            message = parse_message(record.value)
        except InvalidMessage as e:
            return "dlq" if self._send_to_dlq(record, e.reason) else "dlq_failed"

        try:
            self.process_message(message)
            return "ok"
        except Exception as e:
            # process_message는 내부에서 예외를 삼키지만, 여기까지 오는 예외는
            # 메시지 자체가 처리 불가라는 뜻이다. 무한 재시도 대신 격리한다.
            return "dlq" if self._send_to_dlq(record, f"처리 예외: {e}") else "dlq_failed"

    def run_once(self) -> int:
        """
        한 번의 폴링·처리 사이클

        Returns:
            int: 처리한 레코드 수
        """
        # 끊긴 상태로 굳지 않도록 폴링마다 재연결 기회를 준다(백오프 적용).
        self.kafka_consumer.ensure_connection()
        if self.opensearch_connector.ensure_connection():
            self.alert_manager.opensearch_sender.attach(self.opensearch_connector.client)
            self.metrics.attach(self.opensearch_connector.client)

        records = self.kafka_consumer.consume_batch(timeout_ms=5000)
        if not records:
            logger.debug("대기 중인 메시지 없음")
            return 0

        results = [self.handle_record(record) for record in records]

        # DLQ 발행까지 실패한 레코드가 있으면 커밋하지 않는다 → 배치 재처리.
        # 정상 처리분의 중복은 event_id upsert와 등급 시그니처가 흡수한다.
        if "dlq_failed" in results:
            logger.error("배치에 격리 실패 레코드 존재 → 커밋 보류")
        else:
            self.kafka_consumer.commit()
        return len(records)

    def _handle_signal(self, signum, frame) -> None:
        """SIGTERM/SIGINT를 받으면 루프를 빠져나가 정상 종료한다."""
        logger.info(f"종료 시그널 수신({signum}) → 정리 후 종료")
        self._running = False

    def _sleep(self, seconds: int) -> None:
        """1초 단위로 쪼개 자면서 종료 시그널에 바로 반응한다.

        통짜로 자면 docker stop의 유예시간(기본 10초) 안에 못 깨어나
        SIGKILL을 맞고, 처리 중이던 메시지의 정리가 생략된다.
        """
        for _ in range(seconds):
            if not self._running:
                return
            time.sleep(1)

    def _touch_heartbeat(self) -> None:
        """루프 생존 신호. compose healthcheck가 이 파일의 나이를 본다."""
        try:
            self.heartbeat_path.touch()
        except OSError as e:
            logger.warning(f"하트비트 기록 실패: {str(e)}")

    def run_forever(self, poll_interval: int = 10):
        """메시지 처리 루프 (Docker 진입점).

        예전에는 duration_seconds=3600으로 1시간마다 스스로 종료하고
        restart 정책으로 부활했다. 로그에서 정상 종료와 크래시 루프가
        구분되지 않았고, 재시작 시점의 연결 실패가 1시간 동안 고정됐다.
        수명 관리는 컨테이너 오케스트레이터의 일이다.

        처리량 상한: max_poll_records=1 + poll_interval sleep이라
        초당 약 1/poll_interval 건이다. 하루 4건 워크로드에는 충분하다.
        """
        self._running = True
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        logger.info(f"컨슈머 시작 (폴링 간격 {poll_interval}초)")
        logger.info(f"구독 토픽: {self.kafka_consumer.topics}")

        try:
            while self._running:
                try:
                    self.run_once()
                    self._touch_heartbeat()
                except Exception as e:
                    logger.error(f"실행 중 오류: {str(e)}")
                    self.stats["errors"] += 1
                self._sleep(poll_interval)
        finally:
            self._shutdown()
    
    def _shutdown(self):
        """정상 종료 처리"""
        logger.info("컨슈머 종료 중...")
        self.kafka_consumer.close()
        if self._dlq_producer is not None:
            self._dlq_producer.close()
        
        # 최종 통계 출력
        print("\n" + "=" * 80)
        print("컨슈머 종료 통계")
        print("=" * 80)
        print(f"처리된 메시지: {self.stats['total_processed']}")
        print(f"발송된 알림: {self.stats['total_alerts_sent']}")
        print(f"발생한 오류: {self.stats['errors']}")
        print(f"DLQ 격리: {self.stats['dlq']}")
        print("=" * 80 + "\n")
    
    def print_stats(self):
        """통계 출력"""
        print("\n현재 통계:")
        print(f"  처리된 메시지: {self.stats['total_processed']}")
        print(f"  발송된 알림: {self.stats['total_alerts_sent']}")
        print(f"  발생한 오류: {self.stats['errors']}\n")


def main():
    """
    메인 실행 함수
    
    Docker에서 실행될 진입점
    """
    consumer = WeatherAlertConsumer()
    consumer.run_forever(poll_interval=10)


if __name__ == "__main__":
    main()
