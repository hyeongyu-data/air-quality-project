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

DEFAULT_ARGS = {
    "owner": "weather-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
    "email_on_failure": False,
    "email_on_retry": False,
}

DAG_ID = "realtime_weather_alert"

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


def fetch_air_quality_data(**context) -> Dict:
    """
    에어코리아 API에서 대기질 데이터 수집
    
    Returns:
        {
            "status": "success",
            "region": "서울",
            "data": {...}
        }
    """
    try:
        logger.info("=" * 80)
        logger.info("대기질 데이터 수집 시작")
        logger.info("=" * 80)
        
        # producer 모듈 import
        try:
            from producer.producer import AirKoreaAPIClient
        except ImportError:
            # 개발 환경
            import sys
            sys.path.insert(0, "/opt/airflow")
            from producer.producer import AirKoreaAPIClient
        
        api_key = os.getenv("AIRKOREA_API_KEY") or os.getenv("WEATHER_API_KEY")
        client = AirKoreaAPIClient(api_key=api_key)
        
        # 대기질 데이터 수집
        air_data = client.get_air_quality(region="서울")
        
        if not air_data:
            logger.warning("대기질 데이터 수집 실패")
            return {
                "status": "failed",
                "region": "서울",
                "data": None
            }
        
        logger.info(f"대기질 데이터 수집 성공: {json.dumps(air_data, ensure_ascii=False)}")
        
        # XCom에 데이터 저장 (다음 Task에서 사용 가능)
        context["task_instance"].xcom_push(
            key="air_quality_data",
            value=air_data
        )
        
        return {
            "status": "success",
            "region": "서울",
            "data": air_data
        }
        
    except Exception as e:
        logger.error(f"대기질 데이터 수집 오류: {str(e)}")
        raise AirflowException(f"Failed to fetch air quality data: {str(e)}")


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
        except ImportError:
            import sys
            sys.path.insert(0, "/opt/airflow")
            from producer.producer import WeatherDataCollector
        
        run_dt = context.get("data_interval_end") or pendulum.now(local_tz)
        run_hour = run_dt.in_timezone(local_tz).hour if hasattr(run_dt, "in_timezone") else datetime.now().hour
        collector = WeatherDataCollector()
        current_weather = collector.collect_scheduled_weather(region="서울", run_hour=run_hour)
        collector.close()
        
        if not current_weather:
            logger.warning("서울 오늘 기상 예보 통합 데이터 수집 실패")
            return {
                "status": "failed",
                "region": "서울",
                "data": None
            }
        
        logger.info(f"서울 기상 알림 데이터 수집 성공: {json.dumps(current_weather, ensure_ascii=False)}")
        context["task_instance"].xcom_push(
            key="current_weather_data",
            value=current_weather
        )
        
        return {
            "status": "success",
            "region": "서울",
            "data": current_weather
        }
        
    except Exception as e:
        logger.error(f"서울 오늘 기상 예보 통합 데이터 수집 오류: {str(e)}")
        raise AirflowException(f"Failed to fetch daily weather forecast data: {str(e)}")


def fetch_health_index_data(**context) -> Dict:
    """
    기상청 API에서 보건기상지수 데이터 수집
    
    Returns:
        {
            "status": "success",
            "data": {...}
        }
    """
    try:
        logger.info("=" * 80)
        logger.info("보건기상지수 데이터 수집 시작")
        logger.info("=" * 80)
        
        try:
            from producer.producer import WeatherAPIClient
        except ImportError:
            import sys
            sys.path.insert(0, "/opt/airflow")
            from producer.producer import WeatherAPIClient
        
        api_key = os.getenv("WEATHER_API_KEY")
        client = WeatherAPIClient(api_key=api_key)
        
        # 보건기상지수 데이터 수집
        health_data = client.get_health_index()
        
        if not health_data:
            logger.warning("보건기상지수 데이터 수집 실패 (정상, API 공식 지연 가능)")
            return {
                "status": "no_data",
                "data": None
            }
        
        logger.info(f"보건기상지수 수집 성공: {json.dumps(health_data, ensure_ascii=False)}")
        
        context["task_instance"].xcom_push(
            key="health_index_data",
            value=health_data
        )
        
        return {
            "status": "success",
            "data": health_data
        }
        
    except Exception as e:
        logger.warning(f"보건기상지수 수집 오류 (무시): {str(e)}")
        return {
            "status": "failed",
            "data": None
        }


def fetch_uv_index_data(**context) -> Dict:
    """
    기상청 API에서 자외선지수 데이터 수집
    
    Returns:
        {
            "status": "success",
            "data": {...}
        }
    """
    try:
        logger.info("=" * 80)
        logger.info("자외선지수 데이터 수집 시작")
        logger.info("=" * 80)
        
        try:
            from producer.producer import WeatherAPIClient
        except ImportError:
            import sys
            sys.path.insert(0, "/opt/airflow")
            from producer.producer import WeatherAPIClient
        
        api_key = os.getenv("WEATHER_API_KEY")
        client = WeatherAPIClient(api_key=api_key)
        
        # 자외선지수 데이터 수집
        uv_data = client.get_uv_index()
        
        if not uv_data:
            logger.warning("자외선지수 데이터 수집 실패 (정상, API 공식 지연 가능)")
            return {
                "status": "no_data",
                "data": None
            }
        
        logger.info(f"자외선지수 수집 성공: {json.dumps(uv_data, ensure_ascii=False)}")
        
        context["task_instance"].xcom_push(
            key="uv_index_data",
            value=uv_data
        )
        
        return {
            "status": "success",
            "data": uv_data
        }
        
    except Exception as e:
        logger.warning(f"자외선지수 수집 오류 (무시): {str(e)}")
        return {
            "status": "failed",
            "data": None
        }


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
        
        if current_weather:
            if producer.send_current_weather(current_weather):
                published_count += 1
                logger.info("✅ 서울 오늘 기상 예보 통합 데이터 발행 성공")
            else:
                logger.warning("❌ 서울 오늘 기상 예보 통합 데이터 발행 실패")
            
            producer.flush()
            producer.close()
            
            logger.info(f"Kafka 발행 완료: {published_count}개 메시지 발행")
            return {
                "status": "success",
                "published_messages": published_count
            }
        
        logger.warning("발행할 서울 오늘 기상 예보 데이터가 없습니다.")
        
        # 버퍼 플러시
        producer.flush()
        producer.close()
        
        logger.info(f"Kafka 발행 완료: {published_count}개 메시지 발행")
        
        return {
            "status": "success",
            "published_messages": published_count
        }
        
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
