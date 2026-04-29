"""
기상지수 알림 Airflow DAG

매시간 기상청/에어코리아 API에서 데이터를 수집하고
Kafka로 발행하는 자동화된 워크플로우입니다.

DAG 구성:
1. [start] 시작
2. [validate_env] 환경변수 검증
3. [fetch_current_weather] 서울 현재 기상 통합 데이터 수집
4. [publish_to_kafka] Kafka 발행
5. [end] 완료

실행 스케줄: 매시간 (0시, 1시, 2시, ...)
"""

from datetime import datetime, timedelta
import os
import json
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict

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

DEFAULT_ARGS = {
    "owner": "weather-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
    "email_on_failure": False,
    "email_on_retry": False,
}

DAG_ID = "realtime_weather_alert"

# DAG 정의
dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="매시간 기상지수 데이터 수집 및 알림 DAG",
    schedule_interval="0 * * * *",  # 매시간 정각
    start_date=datetime(2026, 4, 28),
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
    서울 현재 기상 통합 데이터 수집
    
    수집 항목:
    꽃가루참나무, 꽃가루소나무, 미세먼지, 초미세먼지, 황사,
    체감온도, 기타특보, 강수확률, 자외선지수
    """
    try:
        logger.info("=" * 80)
        logger.info("서울 현재 기상 통합 데이터 수집 시작")
        logger.info("=" * 80)
        
        try:
            from producer.producer import WeatherDataCollector
        except ImportError:
            import sys
            sys.path.insert(0, "/opt/airflow")
            from producer.producer import WeatherDataCollector
        
        collector = WeatherDataCollector()
        current_weather = collector.collect_current_weather(region="서울")
        collector.close()
        
        if not current_weather:
            logger.warning("서울 현재 기상 통합 데이터 수집 실패")
            return {
                "status": "failed",
                "region": "서울",
                "data": None
            }
        
        logger.info(f"서울 현재 기상 수집 성공: {json.dumps(current_weather, ensure_ascii=False)}")
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
        logger.error(f"서울 현재 기상 통합 데이터 수집 오류: {str(e)}")
        raise AirflowException(f"Failed to fetch current weather data: {str(e)}")


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
                logger.info("✅ 서울 현재 기상 통합 데이터 발행 성공")
            else:
                logger.warning("❌ 서울 현재 기상 통합 데이터 발행 실패")
            
            producer.flush()
            producer.close()
            
            logger.info(f"Kafka 발행 완료: {published_count}개 메시지 발행")
            return {
                "status": "success",
                "published_messages": published_count
            }
        
        logger.warning("발행할 서울 현재 기상 데이터가 없습니다.")
        
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


def _format_weather_value(value, suffix: str = "", empty: str = "정보없음") -> str:
    """알림 본문용 값 포맷팅"""
    if value in (None, "", "None"):
        return empty
    if isinstance(value, float):
        formatted = f"{value:.1f}".rstrip("0").rstrip(".")
        return f"{formatted}{suffix}"
    return f"{value}{suffix}"


def _build_rule_alert(current_weather: Dict) -> Dict:
    """consumer/rules.py 기준으로 현재 날씨를 알림 데이터로 변환"""
    try:
        from consumer.consumer import WeatherDataProcessor
        from consumer.rules import AlertGrouping
    except ImportError:
        import sys
        sys.path.insert(0, "/opt/airflow")
        from consumer.consumer import WeatherDataProcessor
        from consumer.rules import AlertGrouping
    
    alert_data = WeatherDataProcessor.process_current_weather(current_weather)
    alert_data["action_groups"] = AlertGrouping.group_alerts(
        alert_data.get("classification_objects", {})
    )
    alert_data["data_warnings"] = current_weather.get("data_warnings", {})
    return alert_data


def _alert_display_rows(alert_data: Dict):
    """알림 데이터의 표시 행 생성"""
    indices = alert_data.get("indices", {})
    levels = alert_data.get("levels", {})
    recommendations = alert_data.get("recommendations", {})
    emojis = alert_data.get("emojis", {})
    display = [
        ("oak_pollen", "꽃가루 참나무", ""),
        ("pine_pollen", "꽃가루 소나무", ""),
        ("pm10", "미세먼지 PM10", "㎍/㎥"),
        ("pm25", "초미세먼지 PM2.5", "㎍/㎥"),
        ("dust", "황사", "㎍/㎥"),
        ("feels_like_temp", "체감온도", "℃"),
        ("other_special_notice", "기타특보", ""),
        ("precipitation_probability", "강수확률", "%"),
        ("uv_index", "자외선지수", ""),
    ]
    
    rows = []
    for key, label, suffix in display:
        value = indices.get(key)
        rows.append({
            "key": key,
            "label": label,
            "value": _format_weather_value(value, suffix),
            "level": levels.get(f"{key}_level", "정보없음"),
            "recommendation": recommendations.get(f"{key}_rec", ""),
            "emoji": emojis.get(f"{key}_emoji", ""),
        })
    return rows


def _build_weather_email(current_weather: Dict) -> Dict[str, str]:
    """규칙 판정 결과를 이메일 제목/본문으로 변환"""
    alert_data = _build_rule_alert(current_weather)
    region = alert_data.get("region", "서울")
    timestamp = alert_data.get("timestamp", datetime.now().isoformat())
    data_warnings = alert_data.get("data_warnings", {})
    action_groups = alert_data.get("action_groups", {})
    rows = _alert_display_rows(alert_data)
    
    if action_groups and action_groups.get("정상") is None:
        action_lines = []
        for group_name, group_info in action_groups.items():
            reasons = ", ".join(group_info.get("reasons", []))
            action_lines.append(f"- {group_name}: {group_info.get('action', '')} ({reasons})")
    else:
        action_lines = ["- 모든 지수가 정상범위입니다."]
    
    detail_lines = [
        f"- {row['label']}: {row['value']} / {row['level']} / {row['recommendation']}"
        for row in rows
    ]
    
    text_lines = [
        f"{region} 기상 알림",
        f"수집시각: {timestamp}",
        "",
        "필요한 행동",
        *action_lines,
        "",
        "상세 판정",
        *detail_lines,
    ]
    if data_warnings:
        text_lines.extend(["", "참고"])
        text_lines.extend([f"- {key}: {value}" for key, value in data_warnings.items()])
    
    row_html = "\n".join(
        "<tr>"
        f"<th>{row['label']}</th>"
        f"<td>{row['value']}</td>"
        f"<td>{row['level']}</td>"
        f"<td>{row['recommendation']}</td>"
        "</tr>"
        for row in rows
    )
    if action_groups and action_groups.get("정상") is None:
        action_html = "".join(
            f"<li><strong>{group_name}</strong>: {group_info.get('action', '')}"
            f"<br><small>{', '.join(group_info.get('reasons', []))}</small></li>"
            for group_name, group_info in action_groups.items()
        )
    else:
        action_html = "<li>모든 지수가 정상범위입니다.</li>"
    
    warning_html = ""
    if data_warnings:
        warning_items = "".join(
            f"<li><strong>{key}</strong>: {value}</li>"
            for key, value in data_warnings.items()
        )
        warning_html = f"<h3>참고</h3><ul>{warning_items}</ul>"
    
    html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #222;">
        <h2>{region} 기상 알림</h2>
        <p>수집시각: {timestamp}</p>
        <h3>필요한 행동</h3>
        <ul>{action_html}</ul>
        <h3>상세 판정</h3>
        <table cellpadding="8" cellspacing="0" border="1" style="border-collapse: collapse;">
          <tr><th>항목</th><th>값</th><th>등급</th><th>권고</th></tr>
          {row_html}
        </table>
        {warning_html}
      </body>
    </html>
    """
    
    return {
        "subject": f"[기상 알림] {region} 행동 권고",
        "text": "\n".join(text_lines),
        "html": html,
    }


def send_weather_email(**context) -> Dict:
    """수집된 서울 현재 날씨 정보를 이메일로 발송"""
    if os.getenv("EMAIL_ENABLED", "false").lower() != "true":
        logger.info("이메일 발송 비활성화: EMAIL_ENABLED=false")
        return {"status": "skipped", "reason": "EMAIL_ENABLED=false"}
    
    recipient_raw = os.getenv("ALERT_EMAIL", "")
    recipients = [email.strip() for email in recipient_raw.split(",") if email.strip()]
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    from_email = os.getenv("SMTP_FROM_EMAIL") or smtp_username
    use_ssl = os.getenv("SMTP_USE_SSL", "false").lower() == "true"
    use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    
    missing = []
    if not recipients:
        missing.append("ALERT_EMAIL")
    if not smtp_host:
        missing.append("SMTP_HOST")
    if not from_email:
        missing.append("SMTP_FROM_EMAIL 또는 SMTP_USERNAME")
    if not smtp_password:
        missing.append("SMTP_PASSWORD")
    
    if missing:
        logger.warning(f"이메일 발송 설정 누락: {', '.join(missing)}")
        return {
            "status": "skipped",
            "reason": f"missing settings: {', '.join(missing)}"
        }
    
    task_instance = context["task_instance"]
    current_weather = task_instance.xcom_pull(
        task_ids="fetch_current_weather",
        key="current_weather_data"
    )
    if not current_weather:
        logger.warning("메일로 발송할 서울 현재 기상 데이터가 없습니다.")
        return {"status": "skipped", "reason": "no current_weather_data"}
    
    email_content = _build_weather_email(current_weather)
    message = MIMEMultipart("alternative")
    message["Subject"] = email_content["subject"]
    message["From"] = from_email
    message["To"] = ", ".join(recipients)
    message.attach(MIMEText(email_content["text"], "plain", "utf-8"))
    message.attach(MIMEText(email_content["html"], "html", "utf-8"))
    
    try:
        if use_ssl:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=15) as server:
                if smtp_username:
                    server.login(smtp_username, smtp_password)
                server.sendmail(from_email, recipients, message.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
                if use_tls:
                    server.starttls()
                if smtp_username:
                    server.login(smtp_username, smtp_password)
                server.sendmail(from_email, recipients, message.as_string())
        
        logger.info(f"날씨 이메일 발송 완료: {', '.join(recipients)}")
        return {"status": "success", "recipients": recipients}
        
    except Exception as e:
        logger.error(f"날씨 이메일 발송 실패: {str(e)}")
        raise AirflowException(f"Failed to send weather email: {str(e)}")


def _build_kakao_text(current_weather: Dict) -> str:
    """카카오톡 나에게 보내기용 규칙 기반 알림 생성"""
    alert_data = _build_rule_alert(current_weather)
    region = alert_data.get("region", "서울")
    collected_at = alert_data.get("timestamp", "")
    time_label = collected_at[11:16] if len(collected_at) >= 16 else "현재"
    action_groups = alert_data.get("action_groups", {})
    rows = _alert_display_rows(alert_data)
    
    if action_groups and action_groups.get("정상") is None:
        actions = ", ".join(
            group_info.get("action", group_name)
            for group_name, group_info in list(action_groups.items())[:2]
        )
    else:
        actions = "특별 조치 없음"
    
    risk_rows = [
        row for row in rows
        if row["level"] in ("나쁨", "매우나쁨")
    ][:3]
    if not risk_rows:
        risk_rows = [
            row for row in rows
            if row["level"] == "보통"
        ][:2]
    risk_text = ", ".join(
        f"{row['label']} {row['level']}({row['value']})"
        for row in risk_rows
    ) or "대부분 좋음"
    
    lines = [
        f"{region} 기상 알림 ({time_label})",
        f"행동: {actions}",
        f"판정: {risk_text}",
    ]
    return "\n".join(lines)[:200]


def _refresh_kakao_access_token() -> Dict:
    """Kakao refresh token으로 access token 갱신"""
    rest_api_key = os.getenv("KAKAO_REST_API_KEY")
    refresh_token = os.getenv("KAKAO_REFRESH_TOKEN")
    client_secret = os.getenv("KAKAO_CLIENT_SECRET")
    
    data = {
        "grant_type": "refresh_token",
        "client_id": rest_api_key,
        "refresh_token": refresh_token,
    }
    if client_secret:
        data["client_secret"] = client_secret
    
    response = requests.post(
        "https://kauth.kakao.com/oauth/token",
        data=data,
        timeout=15
    )
    response.raise_for_status()
    token_data = response.json()
    if token_data.get("refresh_token"):
        logger.info("카카오 refresh token이 새로 발급되었습니다. .env의 KAKAO_REFRESH_TOKEN 갱신이 필요합니다.")
    return token_data


def send_kakao_message(**context) -> Dict:
    """카카오톡 나에게 보내기로 서울 현재 날씨 정보를 발송"""
    if os.getenv("KAKAO_ENABLED", "false").lower() != "true":
        logger.info("카카오톡 발송 비활성화: KAKAO_ENABLED=false")
        return {"status": "skipped", "reason": "KAKAO_ENABLED=false"}
    
    missing = []
    if not os.getenv("KAKAO_REST_API_KEY"):
        missing.append("KAKAO_REST_API_KEY")
    if not os.getenv("KAKAO_REFRESH_TOKEN"):
        missing.append("KAKAO_REFRESH_TOKEN")
    
    if missing:
        logger.warning(f"카카오톡 발송 설정 누락: {', '.join(missing)}")
        return {
            "status": "skipped",
            "reason": f"missing settings: {', '.join(missing)}"
        }
    
    task_instance = context["task_instance"]
    current_weather = task_instance.xcom_pull(
        task_ids="fetch_current_weather",
        key="current_weather_data"
    )
    if not current_weather:
        logger.warning("카카오톡으로 발송할 서울 현재 기상 데이터가 없습니다.")
        return {"status": "skipped", "reason": "no current_weather_data"}
    
    try:
        token_data = _refresh_kakao_access_token()
        access_token = token_data.get("access_token")
        if not access_token:
            raise AirflowException("Kakao access_token was not returned")
        
        template_object = {
            "object_type": "text",
            "text": _build_kakao_text(current_weather),
            "link": {
                "web_url": "https://www.weather.go.kr",
                "mobile_web_url": "https://www.weather.go.kr",
            },
            "button_title": "기상청 보기",
        }
        
        response = requests.post(
            "https://kapi.kakao.com/v2/api/talk/memo/default/send",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
            },
            data={
                "template_object": json.dumps(template_object, ensure_ascii=False)
            },
            timeout=15
        )
        response.raise_for_status()
        
        logger.info("카카오톡 나에게 보내기 완료")
        return {"status": "success", "result": response.json()}
        
    except Exception as e:
        logger.error(f"카카오톡 발송 실패: {str(e)}")
        raise AirflowException(f"Failed to send Kakao message: {str(e)}")


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

# Task 2: 서울 현재 기상 통합 데이터 수집
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

# Task 4: 이메일 발송
task_send_email = PythonOperator(
    task_id="send_weather_email",
    python_callable=send_weather_email,
    provide_context=True,
    dag=dag
)

# Task 5: 카카오톡 발송
task_send_kakao = PythonOperator(
    task_id="send_kakao_message",
    python_callable=send_kakao_message,
    provide_context=True,
    dag=dag
)

# Task 6: 완료 알림
task_notify = PythonOperator(
    task_id="notify_completion",
    python_callable=notify_completion,
    provide_context=True,
    dag=dag
)

# ============================================================================
# DAG 의존성 정의
# ============================================================================

# 워크플로우: validate → fetch_current_weather → publish → email/kakao → notify
task_validate_env >> task_fetch_current >> task_publish_kafka
task_publish_kafka >> [task_send_email, task_send_kakao] >> task_notify
