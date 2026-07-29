"""
기상지수 알림 Airflow DAG

매 시간마다 기상청/에어코리아 API에서 데이터를 수집하고
Kafka로 발행하는 자동화된 워크플로우입니다.
알림 발송은 Kafka 메시지를 소비하는 consumer/alert.py에서 처리합니다.

DAG 구성:
1. [start] 시작
2. [validate_env] 환경변수 검증
3. [fetch_current_weather] 서울 오늘 기상 예보 통합 데이터 수집
4. [publish_to_kafka] Kafka 발행
5. [end] 완료

실행 스케줄: 매 시간
"""

from datetime import datetime, timedelta
import os
import json
import logging
from typing import Dict
import pendulum

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from dotenv import load_dotenv


# 환경변수 로드
load_dotenv()

# 로깅 설정
logger = logging.getLogger(__name__)

# ============================================================================
# DAG 기본 설정
# ============================================================================

DAG_ID = "realtime_weather_alert"


def notify_failure(context) -> None:
    """태스크 실패를 사람에게 알린다.

    이 콜백이 없으면 수집·발행이 실패해도 Airflow UI 밖에서는 아무도 모른다.
    email_on_failure는 SMTP 설정에 의존하므로, 이미 쓰고 있는 Slack Webhook을
    재사용한다. 통보 자체가 실패해도 태스크 실패 처리를 방해하지 않는다.
    """
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_at = context.get("logical_date") or context.get("data_interval_end")
    reason = context.get("exception")
    message = f"[DAG 실패] {DAG_ID}.{task_id} ({run_at}) - {reason}"

    logger.error(message)

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if os.getenv("SLACK_ENABLED", "false").lower() != "true" or not webhook_url:
        return
    try:
        requests.post(webhook_url, json={"text": message}, timeout=5)
    except Exception as e:
        logger.error(f"실패 통보 발송 실패: {str(e)}")


DEFAULT_ARGS = {
    "owner": "weather-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": notify_failure,
}

local_tz = pendulum.timezone("Asia/Seoul")

# DAG 정의
dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="매 시간 기상지수 데이터 수집 및 알림 DAG",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["weather", "realtime", "alert"],
    doc_md=__doc__
)


# ============================================================================
# Task 함수들
# ============================================================================

def validate_environment(**context) -> Dict:
    """
    환경변수 및 의존성 검증
    
    필수 환경변수:
    - WEATHER_API_KEY
    - KAFKA_BOOTSTRAP_SERVERS
    
    Returns:
        검증 결과 딕셔너리
    """
    logger.info("=" * 80)
    logger.info("기상지수 알림 DAG 시작")
    logger.info("=" * 80)
    
    required_env_vars = [
        "WEATHER_API_KEY",
        "KAFKA_BOOTSTRAP_SERVERS"
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"필수 환경변수 누락: {', '.join(missing_vars)}")
        raise AirflowException(f"Missing environment variables: {missing_vars}")
    
    logger.info("✅ 모든 필수 환경변수 존재")
    if not os.getenv("AIRKOREA_API_KEY"):
        logger.info("AIRKOREA_API_KEY 없음: WEATHER_API_KEY로 에어코리아 API를 함께 호출합니다.")
    
    # 의존성 패키지 확인
    try:
        import requests
        import kafka
        from opensearchpy import OpenSearch
        logger.info("✅ 모든 필수 패키지 설치됨")
    except ImportError as e:
        logger.error(f"패키지 설치 누락: {str(e)}")
        raise AirflowException(f"Missing package: {str(e)}")
    
    # 현재 시간 정보
    execution_date = context["execution_date"]
    logger.info(f"실행 시간: {execution_date.isoformat()}")
    
    return {
        "status": "success",
        "timestamp": execution_date.isoformat(),
        "message": "환경변수 및 의존성 검증 완료"
    }


def fetch_current_weather_data(**context) -> Dict:
    """
    서울 오늘 기상 예보 통합 데이터 수집
    
    수집 항목:
    꽃가루참나무, 꽃가루소나무, 미세먼지, 초미세먼지, 황사,
    오늘 최고 체감온도/기온, 오늘 특보성 신호, 오늘 최대 강수확률, 자외선지수
    """
    try:
        logger.info("=" * 80)
        logger.info("서울 오늘 기상 예보 통합 데이터 수집 시작")
        logger.info("=" * 80)
        
        try:
            from producer.producer import WeatherDataCollector
            from producer.contract import (
                build_event_id, collected_index_count, is_publishable, missing_index_keys,
            )
        except ImportError:
            import sys
            sys.path.insert(0, "/opt/airflow")
            from producer.producer import WeatherDataCollector
            from producer.contract import (
                build_event_id, collected_index_count, is_publishable, missing_index_keys,
            )
        
        run_dt = context.get("data_interval_end") or pendulum.now(local_tz)
        run_hour = run_dt.in_timezone(local_tz).hour if hasattr(run_dt, "in_timezone") else datetime.now().hour
        collector = WeatherDataCollector()
        current_weather = collector.collect_scheduled_weather(region="서울", run_hour=run_hour)
        collector.close()
        
        # 값을 하나도 받지 못한 결과는 발행해도 알릴 것이 없다.
        # 성공으로 반환하면 다음 태스크가 빈 페이로드를 발행하고 DAG는 초록색으로
        # 끝나므로, 여기서 실패시켜 retry와 실패 통보가 실제로 동작하게 한다.
        if not is_publishable(current_weather):
            raise AirflowException(
                "수집된 기상 지수가 하나도 없습니다 "
                f"(결측: {missing_index_keys(current_weather)}, "
                f"경고: {(current_weather or {}).get('data_warnings', {})})"
            )

        # 스케줄 슬롯 기준 결정적 키. 태스크가 재시도돼도 같은 값이라
        # 중복 발행·중복 색인이 하류에서 걸러진다.
        current_weather["event_id"] = build_event_id("서울", run_dt)

        collected = collected_index_count(current_weather)
        missing = missing_index_keys(current_weather)
        if missing:
            logger.warning(f"부분 결측 수집: {collected}개 수집, 결측 {missing}")
        logger.info(f"서울 기상 알림 데이터 수집 성공({collected}개 지수)")
        context["task_instance"].xcom_push(
            key="current_weather_data",
            value=current_weather
        )
        
        return {
            "status": "success",
            "region": "서울",
            "data": current_weather
        }
        
    except AirflowException:
        raise
    except Exception as e:
        logger.error(f"서울 오늘 기상 예보 통합 데이터 수집 오류: {str(e)}")
        raise AirflowException(f"Failed to fetch daily weather forecast data: {str(e)}")


def publish_to_kafka(**context) -> Dict:
    """
    수집된 모든 데이터를 Kafka 토픽으로 발행
    
    XCom에서 이전 Task의 데이터를 꺼내서 Kafka로 발행합니다.
    
    Returns:
        {
            "status": "success",
            "published_messages": 3
        }
    """
    try:
        logger.info("=" * 80)
        logger.info("Kafka 발행 시작")
        logger.info("=" * 80)
        
        try:
            from producer.producer import KafkaWeatherProducer
        except ImportError:
            import sys
            sys.path.insert(0, "/opt/airflow")
            from producer.producer import KafkaWeatherProducer
        
        # Kafka 프로듀서 생성
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        producer = KafkaWeatherProducer(bootstrap_servers=bootstrap_servers)
        
        # XCom에서 이전 Task의 결과 꺼내기
        task_instance = context["task_instance"]
        current_weather = task_instance.xcom_pull(
            task_ids="fetch_current_weather",
            key="current_weather_data"
        )
        
        published_count = 0
        
        try:
            if current_weather and producer.send_current_weather(current_weather):
                published_count += 1
                logger.info("서울 오늘 기상 예보 통합 데이터 발행 성공")
        finally:
            producer.flush()
            producer.close()

        # 발행 0건은 "알림이 나가지 않았다"는 뜻이다. success로 반환하면
        # 종일 알림이 없어도 Airflow UI는 전부 초록색으로 남는다.
        if published_count == 0:
            raise AirflowException(
                "Kafka에 발행된 메시지가 없습니다 "
                f"(XCom 데이터 {'있음' if current_weather else '없음'})"
            )

        logger.info(f"Kafka 발행 완료: {published_count}개 메시지 발행")
        return {
            "status": "success",
            "published_messages": published_count
        }

    except AirflowException:
        raise
    except Exception as e:
        logger.error(f"Kafka 발행 오류: {str(e)}")
        raise AirflowException(f"Failed to publish to Kafka: {str(e)}")


def notify_completion(**context) -> Dict:
    """
    DAG 실행 완료 알림
    """
    execution_date = context["execution_date"]
    logger.info("=" * 80)
    logger.info(f"✅ DAG 실행 완료 ({execution_date.isoformat()})")
    logger.info("=" * 80)
    logger.info("다음 실행: 1시간 후")
    
    return {
        "status": "completed",
        "execution_date": execution_date.isoformat()
    }


# ============================================================================
# Task 정의
# ============================================================================

# Task 1: 환경변수 검증
task_validate_env = PythonOperator(
    task_id="validate_environment",
    python_callable=validate_environment,
    provide_context=True,
    dag=dag
)

# Task 2: 서울 오늘 기상 예보 통합 데이터 수집
task_fetch_current = PythonOperator(
    task_id="fetch_current_weather",
    python_callable=fetch_current_weather_data,
    provide_context=True,
    dag=dag
)

# Task 3: Kafka 발행
task_publish_kafka = PythonOperator(
    task_id="publish_to_kafka",
    python_callable=publish_to_kafka,
    provide_context=True,
    dag=dag
)

# Task 4: 완료 알림
task_notify = PythonOperator(
    task_id="notify_completion",
    python_callable=notify_completion,
    provide_context=True,
    dag=dag
)

# ============================================================================
# DAG 의존성 정의
# ============================================================================

# 워크플로우: validate → fetch_current_weather → publish → notify
# 이메일/카카오톡 알림은 Kafka 메시지를 읽은 consumer/alert.py에서 처리합니다.
task_validate_env >> task_fetch_current >> task_publish_kafka >> task_notify
