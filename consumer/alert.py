"""
알림 발송 모듈

다양한 채널(콘솔, Slack, 이메일, 카카오톡, OpenSearch)을 통해
기상지수 기반 알림을 발송하는 기능을 제공합니다.
"""

import os
import json
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional

try:
    from .rules import should_record_signature
    from . import opensearch_setup
except ImportError:  # 직접 실행 시
    from rules import should_record_signature
    import opensearch_setup
from datetime import datetime
import requests
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


# refresh token 잔여 기간이 이 값 아래로 내려가면 경고한다(기본 7일).
KAKAO_REFRESH_EXPIRY_WARN_SECONDS = int(
    os.getenv("KAKAO_REFRESH_EXPIRY_WARN_SECONDS", 7 * 24 * 3600)
)


class ConsoleAlertSender:
    """콘솔로 알림을 출력하는 클래스"""
    
    @staticmethod
    def send(alert_data: Dict) -> bool:
        """
        콘솔에 알림 출력
        
        Args:
            alert_data: {
                "timestamp": "2026-04-28T10:30:00",
                "region": "서울",
                "indices": {
                    "pm10": 120,
                    "pm25": 45,
                    ...
                },
                "levels": {
                    "pm10_level": "나쁨",
                    "pm25_level": "보통",
                    ...
                },
                "recommendations": {
                    "pm10_rec": "마스크 착용 권고",
                    ...
                },
                "action_groups": {
                    "마스크_필수": {...},
                    ...
                }
            }
        
        Returns:
            bool: 발송 성공 여부
        """
        try:
            timestamp = alert_data.get("timestamp", now_kst().isoformat())
            region = alert_data.get("region", "전국")
            
            # 헤더
            print("\n" + "=" * 80)
            print(f"🌍 기상지수 알림 | {timestamp} | {region}")
            print("=" * 80)
            
            # 개별 지수 정보
            indices = alert_data.get("indices", {})
            levels = alert_data.get("levels", {})
            recommendations = alert_data.get("recommendations", {})
            emojis = alert_data.get("emojis", {})
            
            print("\n📊 기상지수 상세 정보:")
            print("-" * 80)
            
            index_display_names = {
                "oak_pollen": "꽃가루 참나무",
                "pine_pollen": "꽃가루 소나무",
                "pm10": "미세먼지 (PM10)",
                "pm25": "초미세먼지 (PM2.5)",
                "dust": "황사",
                "feels_like_temp": "체감온도",
                "other_special_notice": "기타특보",
                "precipitation_probability": "강수확률",
                "uv_index": "자외선 지수",
                "ozone": "오존",
                "cold_risk": "감기위험지수",
                "discomfort": "불쾌지수"
            }
            
            for idx_key, display_name in index_display_names.items():
                value = indices.get(idx_key)
                if value is not None:
                    level_key = f"{idx_key}_level"
                    rec_key = f"{idx_key}_rec"
                    emoji_key = f"{idx_key}_emoji"
                    
                    level = levels.get(level_key, "정보 없음")
                    recommendation = recommendations.get(rec_key, "")
                    emoji = emojis.get(emoji_key, "")
                    
                    # 수치 + 등급 표시
                    if isinstance(value, (int, float)):
                        print(f"\n{emoji} {display_name}: {value}")
                    else:
                        print(f"\n{emoji} {display_name}: {value}")
                    
                    print(f"  └─ 등급: {level}")
                    if recommendation:
                        print(f"  └─ 권고: {recommendation}")
            
            # 행동 그룹
            action_groups = alert_data.get("action_groups", {})
            if action_groups and action_groups.get("정상") is None:
                print("\n" + "-" * 80)
                print("⚡ 필요한 행동 (Action Groups):")
                print("-" * 80)
                
                for group_name, group_info in action_groups.items():
                    if isinstance(group_info, dict):
                        print(f"\n{group_info.get('color', '🟠')} {group_name}")
                        print(f"  └─ 설명: {group_info.get('description', '')}")
                        print(f"  └─ 행동: {group_info.get('action', '')}")
                        print(f"  └─ 원인: {', '.join(group_info.get('reasons', []))}")
            else:
                print("\n" + "-" * 80)
                print("✅ 모든 지수가 정상범위입니다.")
            
            print("\n" + "=" * 80 + "\n")
            
            logger.info(f"콘솔 알림 발송 성공 - {region}")
            return True
            
        except Exception as e:
            logger.error(f"콘솔 알림 발송 실패: {str(e)}")
            return False


class SlackAlertSender:
    """Slack으로 알림을 발송하는 클래스"""
    
    def __init__(self):
        """Slack Webhook URL 초기화"""
        self.webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        self.enabled = os.getenv("SLACK_ENABLED", "false").lower() == "true"
    
    def send(self, alert_data: Dict) -> bool:
        """
        Slack으로 알림 발송
        
        Args:
            alert_data: 알림 데이터
        
        Returns:
            bool: 발송 성공 여부
        """
        if not self.enabled or not self.webhook_url:
            logger.debug("Slack 알림이 비활성화되었습니다.")
            return False
        
        try:
            message = self._build_slack_message(alert_data)
            
            response = requests.post(
                self.webhook_url,
                json=message,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("Slack 알림 발송 성공")
                return True
            else:
                logger.error(f"Slack API 오류: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Slack 알림 발송 실패: {str(e)}")
            return False
    
    @staticmethod
    def _build_slack_message(alert_data: Dict) -> Dict:
        """
        Slack 메시지 형식으로 빌드
        
        Args:
            alert_data: 알림 데이터
        
        Returns:
            Slack Webhook 형식의 메시지
        """
        timestamp = alert_data.get("timestamp", now_kst().isoformat())
        region = alert_data.get("region", "전국")
        indices = alert_data.get("indices", {})
        levels = alert_data.get("levels", {})
        action_groups = alert_data.get("action_groups", {})
        
        # 긴급도에 따른 색상 결정
        color = "#36a64f"  # 녹색 (정상)
        if action_groups.get("외출_자제"):
            color = "#ff0000"  # 빨강 (위험)
        elif action_groups.get("마스크_필수"):
            color = "#ff6600"  # 주황 (주의)
        
        # 필드 구성
        fields = []
        index_names = {
            "oak_pollen": "꽃가루 참나무",
            "pine_pollen": "꽃가루 소나무",
            "pm10": "미세먼지",
            "pm25": "초미세먼지",
            "dust": "황사",
            "feels_like_temp": "체감온도",
            "other_special_notice": "기타특보",
            "precipitation_probability": "강수확률",
            "uv_index": "자외선",
            "ozone": "오존"
        }
        
        for key, name in index_names.items():
            value = indices.get(key)
            level_key = f"{key}_level"
            level = levels.get(level_key, "정보 없음")
            
            if value is not None:
                fields.append({
                    "title": name,
                    "value": f"{value} ({level})",
                    "short": True
                })
        
        # 행동 그룹
        action_text = ""
        if action_groups and action_groups.get("정상") is None:
            action_text = "\n".join([
                f"• {name}: {info.get('action', '')}"
                for name, info in action_groups.items()
                if isinstance(info, dict)
            ])
        else:
            action_text = "✅ 모든 지수가 정상범위입니다."
        
        message = {
            "username": "기상지수 알림봇",
            "icon_emoji": ":weather_cloud:",
            "attachments": [
                {
                    "fallback": f"{region} 기상지수 알림",
                    "color": color,
                    "title": f"🌍 {region} 기상지수 알림",
                    "text": timestamp,
                    "fields": fields,
                    "footer": "기상청/에어코리아 기반 예측",
                    "ts": int(datetime.fromisoformat(timestamp).timestamp())
                },
                {
                    "color": color,
                    "title": "⚡ 필요한 행동",
                    "text": action_text,
                    "mrkdwn_in": ["text"]
                }
            ]
        }
        
        return message


def _format_weather_value(value, suffix: str = "", empty: str = "정보없음") -> str:
    """알림 본문용 값 포맷팅"""
    if value in (None, "", "None"):
        return empty
    if isinstance(value, float):
        formatted = f"{value:.1f}".rstrip("0").rstrip(".")
        return f"{formatted}{suffix}"
    return f"{value}{suffix}"


def _alert_title_label(raw_data: Dict) -> str:
    """수집 모드에 맞는 알림 제목 문구"""
    forecast_type = raw_data.get("forecast_type")
    if forecast_type == "current_conditions":
        return "현재 기상 알림"
    if forecast_type == "morning_mixed":
        return "오늘 아침 기상 알림"
    if forecast_type == "daily_full_forecast":
        return "오늘 전체 예보 알림"
    return "오늘 기상 예보 알림"


def _alert_display_rows(alert_data: Dict) -> List[Dict]:
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


def _build_weather_email(alert_data: Dict) -> Dict[str, str]:
    """규칙 판정 결과를 이메일 제목/본문으로 변환"""
    region = alert_data.get("region", "서울")
    timestamp = alert_data.get("timestamp", now_kst().isoformat())
    data_warnings = alert_data.get("data_warnings", {})
    action_groups = alert_data.get("action_groups", {})
    raw_data = alert_data.get("raw_data", {})
    title_label = _alert_title_label(raw_data)
    rows = _alert_display_rows(alert_data)
    temp_summary = ""
    if raw_data.get("min_temperature") is not None or raw_data.get("max_temperature") is not None:
        temp_summary = (
            f"오늘 기온: 최저 {_format_weather_value(raw_data.get('min_temperature'), '℃')} / "
            f"최고 {_format_weather_value(raw_data.get('max_temperature'), '℃')}"
        )
    
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
        f"{region} {title_label}",
        f"수집시각: {timestamp}",
        "",
        "필요한 행동",
        *action_lines,
        "",
        *([temp_summary, ""] if temp_summary else []),
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
        <h2>{region} {title_label}</h2>
        <p>수집시각: {timestamp}</p>
        <h3>필요한 행동</h3>
        <ul>{action_html}</ul>
        {f"<p><strong>{temp_summary}</strong></p>" if temp_summary else ""}
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
        "subject": f"[{title_label}] {region} 행동 권고",
        "text": "\n".join(text_lines),
        "html": html,
    }


def _build_kakao_text(alert_data: Dict) -> str:
    """카카오톡 나에게 보내기용 규칙 기반 알림 생성"""
    region = alert_data.get("region", "서울")
    collected_at = alert_data.get("timestamp", "")
    time_label = collected_at[11:16] if len(collected_at) >= 16 else "오늘"
    action_groups = alert_data.get("action_groups", {})
    raw_data = alert_data.get("raw_data", {})
    title_label = _alert_title_label(raw_data)
    rows = _alert_display_rows(alert_data)
    
    if action_groups and action_groups.get("정상") is None:
        action_lines = [
            f"- {group_info.get('action', group_name)}"
            for group_name, group_info in list(action_groups.items())[:2]
        ]
    else:
        action_lines = ["- 특별 조치 없음"]
    
    risk_rows = [
        row for row in rows
        if row["level"] in ("나쁨", "매우나쁨")
    ][:3]
    if not risk_rows:
        risk_rows = [
            row for row in rows
            if row["level"] == "보통"
        ][:3]
    risk_lines = [
        f"- {row['label']} {row['value']}: {row['level']}"
        for row in risk_rows
    ] or ["- 대부분 좋음"]
    
    row_by_key = {row["key"]: row for row in rows}
    
    def level_for(key: str) -> str:
        return row_by_key.get(key, {}).get("level", "정보없음")
    
    def value_for(key: str) -> str:
        return row_by_key.get(key, {}).get("value", "정보없음")
    
    summary_lines = []
    if raw_data.get("min_temperature") is not None or raw_data.get("max_temperature") is not None:
        summary_lines.append(
            f"기온 최저 {_format_weather_value(raw_data.get('min_temperature'), '℃')}, "
            f"최고 {_format_weather_value(raw_data.get('max_temperature'), '℃')}"
        )
    summary_lines.extend([
        f"미세먼지 {level_for('pm10')}, 초미세먼지 {level_for('pm25')}",
        (
            f"황사 {level_for('dust')}, 강수확률 {value_for('precipitation_probability')}, "
            f"자외선 {value_for('uv_index')}"
        ),
        f"특보 {value_for('other_special_notice')}",
    ])
    
    lines = [
        f"[{region} {title_label}] {time_label}",
        "",
        "행동",
        *action_lines,
        "",
        "위험/주의",
        *risk_lines,
        "",
        "나머지",
        *summary_lines,
    ]
    return "\n".join(lines)[:1000]


class EmailAlertSender:
    """이메일로 알림을 발송하는 클래스"""
    
    def __init__(self):
        """이메일 설정 초기화"""
        self.enabled = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
        recipient_raw = os.getenv("ALERT_EMAIL", "")
        self.recipients = [email.strip() for email in recipient_raw.split(",") if email.strip()]
        self.smtp_host = os.getenv("SMTP_HOST")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_username = os.getenv("SMTP_USERNAME")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.from_email = os.getenv("SMTP_FROM_EMAIL") or self.smtp_username
        self.use_ssl = os.getenv("SMTP_USE_SSL", "false").lower() == "true"
        self.use_tls = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    
    def send(self, alert_data: Dict) -> bool:
        """
        이메일로 알림 발송
        
        Args:
            alert_data: 알림 데이터
        
        Returns:
            bool: 발송 성공 여부
        """
        if not self.enabled:
            logger.debug("이메일 알림이 비활성화되었습니다.")
            return False
        
        missing = []
        if not self.recipients:
            missing.append("ALERT_EMAIL")
        if not self.smtp_host:
            missing.append("SMTP_HOST")
        if not self.from_email:
            missing.append("SMTP_FROM_EMAIL 또는 SMTP_USERNAME")
        if not self.smtp_password:
            missing.append("SMTP_PASSWORD")
        if missing:
            logger.warning(f"이메일 알림 설정 누락: {', '.join(missing)}")
            return False
        
        try:
            email_content = _build_weather_email(alert_data)
            message = MIMEMultipart("alternative")
            message["Subject"] = email_content["subject"]
            message["From"] = self.from_email
            message["To"] = ", ".join(self.recipients)
            message.attach(MIMEText(email_content["text"], "plain", "utf-8"))
            message.attach(MIMEText(email_content["html"], "html", "utf-8"))
            
            if self.use_ssl:
                with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, timeout=15) as server:
                    if self.smtp_username:
                        server.login(self.smtp_username, self.smtp_password)
                    server.sendmail(self.from_email, self.recipients, message.as_string())
            else:
                with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=15) as server:
                    if self.use_tls:
                        server.starttls()
                    if self.smtp_username:
                        server.login(self.smtp_username, self.smtp_password)
                    server.sendmail(self.from_email, self.recipients, message.as_string())
            
            logger.info(f"이메일 알림 발송 성공: {', '.join(self.recipients)}")
            return True
            
        except smtplib.SMTPAuthenticationError as e:
            logger.error(
                "이메일 인증 실패: Gmail 주소 또는 앱 비밀번호를 확인하세요. "
                f"SMTP 응답={e.smtp_code}"
            )
            return False
        except Exception as e:
            logger.error(f"이메일 알림 발송 실패: {str(e)}")
            return False


class KakaoAlertSender:
    """카카오톡 나에게 보내기로 알림을 발송하는 클래스"""
    
    # refresh token 회전 값을 담아 두는 상태 파일. 컨슈머는 매시간 재시작하므로
    # 메모리에만 두면 회전된 토큰이 그때마다 사라진다.
    DEFAULT_STATE_PATH = ".kakao_token.json"

    def __init__(self):
        """카카오 API 설정 초기화"""
        self.enabled = os.getenv("KAKAO_ENABLED", "false").lower() == "true"
        self.rest_api_key = os.getenv("KAKAO_REST_API_KEY")
        self.client_secret = os.getenv("KAKAO_CLIENT_SECRET")
        self.state_path = Path(
            os.getenv("KAKAO_TOKEN_STATE_PATH", self.DEFAULT_STATE_PATH)
        )
        # 회전으로 저장해 둔 토큰이 있으면 그쪽이 최신이다.
        self.refresh_token = self._load_refresh_token() or os.getenv("KAKAO_REFRESH_TOKEN")

    def _load_refresh_token(self) -> Optional[str]:
        """상태 파일에 저장된 refresh token을 읽는다. 실패는 무시하고 env로 폴백."""
        try:
            if not self.state_path.exists():
                return None
            saved = json.loads(self.state_path.read_text(encoding="utf-8"))
            return saved.get("refresh_token") or None
        except Exception as e:
            logger.warning(f"카카오 토큰 상태 파일 읽기 실패(환경변수로 폴백): {str(e)}")
            return None

    def _store_refresh_token(self, refresh_token: str, expires_in=None) -> None:
        """회전된 refresh token을 상태 파일에 저장한다."""
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(
                json.dumps(
                    {"refresh_token": refresh_token,
                     "refresh_token_expires_in": expires_in},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            os.chmod(self.state_path, 0o600)
            self.refresh_token = refresh_token
            logger.info(f"카카오 refresh token 회전 값을 저장했습니다: {self.state_path}")
        except Exception as e:
            # 저장에 실패해도 이번 발송은 진행한다. 다만 다음 만료 때 끊기므로 크게 남긴다.
            logger.error(
                f"카카오 refresh token 저장 실패 — 토큰 만료 시 알림이 중단됩니다: {str(e)}"
            )

    
    def send(self, alert_data: Dict) -> bool:
        """
        카카오톡 나에게 보내기 발송
        
        Args:
            alert_data: 알림 데이터
        
        Returns:
            bool: 발송 성공 여부
        """
        if not self.enabled:
            logger.debug("카카오톡 알림이 비활성화되었습니다.")
            return False
        
        missing = []
        if not self.rest_api_key:
            missing.append("KAKAO_REST_API_KEY")
        if not self.refresh_token:
            missing.append("KAKAO_REFRESH_TOKEN")
        if missing:
            logger.warning(f"카카오톡 알림 설정 누락: {', '.join(missing)}")
            return False
        
        try:
            access_token = self._refresh_access_token()
            template_object = {
                "object_type": "text",
                "text": _build_kakao_text(alert_data),
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
            
            logger.info("카카오톡 알림 발송 성공")
            return True
            
        except Exception as e:
            logger.error(f"카카오톡 알림 발송 실패: {str(e)}")
            return False
    
    def _refresh_access_token(self) -> str:
        """refresh token으로 access token 갱신"""
        data = {
            "grant_type": "refresh_token",
            "client_id": self.rest_api_key,
            "refresh_token": self.refresh_token,
        }
        if self.client_secret:
            data["client_secret"] = self.client_secret
        
        response = requests.post(
            "https://kauth.kakao.com/oauth/token",
            data=data,
            timeout=15
        )
        response.raise_for_status()
        token_data = response.json()

        # 카카오는 refresh token의 잔여 유효기간이 짧아지면 갱신 응답에 새 토큰을
        # 함께 준다. 이걸 버리면 기존 토큰 만료 시점에 알림이 영구 중단된다.
        rotated = token_data.get("refresh_token")
        if rotated:
            self._store_refresh_token(
                rotated, token_data.get("refresh_token_expires_in")
            )

        remaining = token_data.get("refresh_token_expires_in")
        if isinstance(remaining, (int, float)) and remaining < KAKAO_REFRESH_EXPIRY_WARN_SECONDS:
            logger.warning(
                f"카카오 refresh token 만료 임박: 약 {int(remaining) // 86400}일 남음. "
                "scripts/kakao_get_refresh_token.py로 재발급이 필요합니다."
            )

        access_token = token_data.get("access_token")
        if not access_token:
            raise RuntimeError("Kakao access_token was not returned")
        return access_token


class OpenSearchAlertSender:
    """OpenSearch에 알림 데이터를 저장하는 클래스"""
    
    # OpenSearch가 죽어 있을 때도 쿨다운이 동작하도록 마지막 시그니처를 남긴다.
    DEFAULT_STATE_PATH = ".signature_state.json"

    # 지역별 쿨다운 상태 문서를 담는 전용 인덱스. 문서 ID = 지역.
    # 문서 GET은 refresh와 무관하게 실시간이라, 이력 인덱스를 검색하던
    # 방식의 refresh 지연 레이스가 사라진다.
    STATE_INDEX = opensearch_setup.STATE_INDEX

    def __init__(self, opensearch_client=None):
        """
        OpenSearch 클라이언트 초기화
        
        Args:
            opensearch_client: OpenSearch 클라이언트 (None이면 별도 생성)
        """
        self.client = opensearch_client
        self.index_prefix = os.getenv("OPENSEARCH_INDEX_PREFIX", "weather-alert")
        self.enabled = True if opensearch_client else False
        self.state_path = Path(
            os.getenv("SIGNATURE_STATE_PATH", self.DEFAULT_STATE_PATH)
        )

    def attach(self, client) -> None:
        """재연결로 새로 얻은 클라이언트를 반영한다."""
        if client is not None and client is not self.client:
            logger.info("OpenSearch 클라이언트 재연결 반영")
        self.client = client
        self.enabled = client is not None

    def _record_state(self, alert_data: Dict) -> None:
        """지역별 쿨다운 상태 문서를 갱신하고 로컬 캐시에도 남긴다.

        last_external_send_at은 실제로 외부 채널에 전달됐을 때만 갱신한다.
        쿨다운으로 생략된 회차까지 갱신하면 최대 무발송 간격이 영영 차지 않아
        하트비트 재발송이 동작하지 않는다.
        """
        region = alert_data.get("region", "전국")
        signature = alert_data.get("grade_signature", "")
        delivered = bool(alert_data.get("delivered_channels"))
        now_iso = now_kst().isoformat()

        doc = {"region": region, "grade_signature": signature, "updated_at": now_iso}
        if delivered:
            doc["last_external_send_at"] = now_iso

        try:
            # doc_as_upsert: 문서가 없으면 생성, 있으면 있는 필드만 덮어쓴다.
            # 미발송 회차에는 last_external_send_at이 doc에 없어 기존 값이 유지된다.
            self.client.update(
                index=self.STATE_INDEX,
                id=region,
                body={"doc": doc, "doc_as_upsert": True},
            )
        except Exception as e:
            logger.warning(f"쿨다운 상태 문서 갱신 실패: {str(e)}")

        self._write_cache(region, signature, doc.get("last_external_send_at"))

    def _read_cache(self) -> Dict[str, str]:
        try:
            if self.state_path.exists():
                return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"시그니처 캐시 읽기 실패: {str(e)}")
        return {}

    def _write_cache(
        self, region: str, signature: str, last_sent_at: Optional[str] = None
    ) -> None:
        """마지막으로 알린 등급을 로컬에 남긴다.

        OpenSearch 미가용 시 조회가 항상 None을 돌려주면 쿨다운이 무력화돼
        매 메시지가 전 채널로 나간다. 캐시가 있으면 그 사이에도 중복을 막는다.
        """
        try:
            cache = self._read_cache()
            entry = cache.get(region)
            entry = dict(entry) if isinstance(entry, dict) else {}
            entry["grade_signature"] = signature
            if last_sent_at:
                entry["last_external_send_at"] = last_sent_at
            cache[region] = entry
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(
                json.dumps(cache, ensure_ascii=False), encoding="utf-8"
            )
        except Exception as e:
            logger.warning(f"시그니처 캐시 저장 실패: {str(e)}")
    
    def send(self, alert_data: Dict, record_signature: bool = True) -> bool:
        """
        OpenSearch에 알림 데이터 저장

        Args:
            alert_data: 알림 데이터
            record_signature: False면 등급 시그니처를 비워 저장한다. 외부 발송이
                전부 실패했을 때 "알렸다"고 기록하지 않기 위한 장치다.
                이력 자체는 남겨야 사후 조사가 가능하므로 문서는 그대로 쓴다.

        Returns:
            bool: 저장 성공 여부
        """
        if not self.enabled or not self.client:
            logger.debug("OpenSearch 저장이 비활성화되었습니다.")
            return False
        
        try:
            timestamp = alert_data.get("timestamp", now_kst().isoformat())
            
            # 인덱스 이름: weather-alert-2026.04 (월 단위)
            # 일 단위면 1년에 365개 인덱스가 쌓인다. 시간당 1건 발행하는
            # 워크로드에서 감당할 이유가 없는 샤드 수다. 조회는 와일드카드라
            # 단위가 바뀌어도 그대로 동작한다.
            date_str = datetime.fromisoformat(timestamp).strftime("%Y.%m")
            index_name = f"{self.index_prefix}-{date_str}"
            
            # 문서 생성
            doc = {
                "timestamp": timestamp,
                "region": alert_data.get("region", "전국"),
                "indices": alert_data.get("indices", {}),
                "levels": alert_data.get("levels", {}),
                "recommendations": alert_data.get("recommendations", {}),
                "action_groups": list(alert_data.get("action_groups", {}).keys()),
                "alert_severity": self._calculate_severity(alert_data),
                # 다음 메시지의 쿨다운 비교 기준(등급 시그니처).
                # 외부 발송이 전부 실패했다면 비워 둬서 다음 회차에 재시도하게 한다.
                "grade_signature": alert_data.get("grade_signature", "") if record_signature else "",
                "event_id": alert_data.get("event_id"),
                "delivered_channels": alert_data.get("delivered_channels", []),
                "signature_recorded": record_signature,
            }
            
            # event_id가 있으면 문서 ID로 써서 upsert 한다. 없으면 자동 생성이라
            # 같은 이벤트를 재처리할 때마다 문서가 새로 쌓인다.
            response = self.client.index(
                index=index_name,
                body=doc,
                id=alert_data.get("event_id") or None,
            )
            
            if record_signature:
                self._record_state(alert_data)
            logger.info(f"OpenSearch 저장 성공: {index_name} (ID: {response['_id']})")
            return True
            
        except Exception as e:
            logger.error(f"OpenSearch 저장 실패: {str(e)}")
            return False

    def _cached_state(self, region: str) -> Dict:
        """로컬 캐시 항목을 dict로 정규화한다(옛 str 형식 호환)."""
        entry = self._read_cache().get(region)
        if isinstance(entry, dict):
            return entry
        if isinstance(entry, str):
            return {"grade_signature": entry}
        return {}

    def cooldown_state(self, region: str) -> Dict:
        """해당 region의 쿨다운 상태(직전 시그니처 + 마지막 외부 발송 시각).

        지역별 상태 문서를 ID로 GET 한다. 문서 GET은 refresh와 무관하게
        실시간이라 이력 인덱스를 검색하던 방식의 레이스가 없다.
        상태 문서가 아직 없으면(기존 배포에서 이행 직후) 이력 인덱스에서
        한 번 읽어오고, 미연결/실패 시 로컬 캐시를 쓴다. 최종적으로 아무것도
        없으면 빈 dict — 호출측이 발송하도록(fail-open) 둔다.
        """
        if not self.enabled or not self.client:
            state = self._cached_state(region)
            if state:
                logger.info("OpenSearch 미연결 → 로컬 캐시의 쿨다운 상태 사용")
            return state
        try:
            doc = self.client.get(index=self.STATE_INDEX, id=region)
            source = doc.get("_source", {})
            return {
                "grade_signature": source.get("grade_signature"),
                "last_external_send_at": source.get("last_external_send_at"),
            }
        except Exception as e:
            if getattr(e, "status_code", None) == 404 or "NotFoundError" in type(e).__name__:
                return self._legacy_signature_lookup(region)
            logger.warning(f"쿨다운 상태 조회 실패 → 로컬 캐시 확인: {str(e)}")
            return self._cached_state(region)

    def _legacy_signature_lookup(self, region: str) -> Dict:
        """상태 문서 도입 전 데이터에서 직전 시그니처를 읽는 이행 경로."""
        try:
            response = self.client.search(
                index=f"{self.index_prefix}-*",
                body={
                    "size": 1,
                    # region은 인덱스 템플릿에서 keyword다. match는 분석기를 타서
                    # 지역명이 늘어나면 오매칭한다.
                    "query": {"term": {"region": region}},
                    "sort": [{"timestamp": {"order": "desc"}}],
                    "_source": ["grade_signature"],
                },
            )
            hits = response.get("hits", {}).get("hits", [])
            if hits:
                return {"grade_signature": hits[0].get("_source", {}).get("grade_signature")}
            return {}
        except Exception as e:
            logger.warning(f"이력 시그니처 조회 실패 → 로컬 캐시 확인: {str(e)}")
            return self._cached_state(region)

    def latest_signature(self, region: str) -> Optional[str]:
        """직전 등급 시그니처만 필요할 때의 축약 조회."""
        return self.cooldown_state(region).get("grade_signature")

    @staticmethod
    def _calculate_severity(alert_data: Dict) -> str:
        """
        알림의 심각도 계산
        
        Args:
            alert_data: 알림 데이터
        
        Returns:
            "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
        """
        action_groups = alert_data.get("action_groups", {})
        
        # 그룹의 존재 여부로 판정한다. .get()의 truthy 검사는 값이 빈 dict일 때
        # 활성화된 그룹을 없는 것으로 취급하는 잠재 결함이 있었다.
        if "외출_자제" in action_groups:
            return "CRITICAL"
        if "마스크_필수" in action_groups or "특보_확인" in action_groups:
            return "HIGH"
        if any(g in action_groups for g in
               ("자외선_차단", "보온_필수", "알레르기_주의", "우산_준비")):
            return "MEDIUM"
        if "정상" in action_groups:
            return "LOW"
        return "MEDIUM"


class AlertManager:
    """여러 채널을 통합 관리하는 클래스"""
    
    def __init__(self, opensearch_client=None):
        """
        알림 관리자 초기화
        
        Args:
            opensearch_client: OpenSearch 클라이언트
        """
        self.console_sender = ConsoleAlertSender()
        self.slack_sender = SlackAlertSender()
        self.email_sender = EmailAlertSender()
        self.kakao_sender = KakaoAlertSender()
        self.opensearch_sender = OpenSearchAlertSender(opensearch_client)
    
    def send_all(self, alert_data: Dict, send_external: bool = True) -> Dict[str, bool]:
        """
        모든 활성화된 채널로 알림 발송

        Args:
            alert_data: 알림 데이터
            send_external: False면 외부 채널(Slack/이메일/카카오) 발송을 생략한다.
                등급 무변경(쿨다운) 시 스팸을 막되, 콘솔 출력과 OpenSearch
                이력 저장은 감사/최신 상태 추적을 위해 항상 유지한다.

        Returns:
            {
                "console": True,
                "slack": True,
                "email": False,
                "kakao": False,
                "opensearch": True
            }
        """
        console_ok = self.console_sender.send(alert_data)

        external = {
            "slack": self.slack_sender.send(alert_data) if send_external else False,
            "email": self.email_sender.send(alert_data) if send_external else False,
            "kakao": self.kakao_sender.send(alert_data) if send_external else False,
        }
        delivered = [name for name, ok in external.items() if ok]
        alert_data["delivered_channels"] = delivered

        external_enabled = any(
            sender.enabled
            for sender in (self.slack_sender, self.email_sender, self.kakao_sender)
        )
        record = should_record_signature(send_external, external_enabled, delivered)
        if not record:
            enabled_names = [
                name for name, sender in (
                    ("slack", self.slack_sender),
                    ("email", self.email_sender),
                    ("kakao", self.kakao_sender),
                ) if sender.enabled
            ]
            logger.error(
                "외부 채널 전량 발송 실패 → 등급 시그니처를 기록하지 않는다(다음 회차 재시도). "
                f"활성 채널={enabled_names}"
            )

        results = {
            "console": console_ok,
            **external,
            "opensearch": self.opensearch_sender.send(alert_data, record_signature=record),
        }

        if send_external:
            logger.info(f"외부 채널 발송 결과: {len(delivered)}/{len(external)} 성공 {delivered}")
        else:
            logger.info("외부 채널 발송 생략(쿨다운 또는 판정 근거 부족)")

        return results
