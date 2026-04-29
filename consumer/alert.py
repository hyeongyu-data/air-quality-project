"""
알림 발송 모듈

다양한 채널(콘솔, Slack, 이메일, OpenSearch)을 통해
기상지수 기반 알림을 발송하는 기능을 제공합니다.
"""

import os
import json
import logging
from typing import Dict, List, Optional
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
            timestamp = alert_data.get("timestamp", datetime.now().isoformat())
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
        timestamp = alert_data.get("timestamp", datetime.now().isoformat())
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


class EmailAlertSender:
    """이메일로 알림을 발송하는 클래스"""
    
    def __init__(self):
        """이메일 설정 초기화"""
        self.enabled = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
        self.recipient = os.getenv("ALERT_EMAIL")
        # 실제 구현 시 SMTP 설정 필요
    
    def send(self, alert_data: Dict) -> bool:
        """
        이메일로 알림 발송
        
        Args:
            alert_data: 알림 데이터
        
        Returns:
            bool: 발송 성공 여부
        """
        if not self.enabled or not self.recipient:
            logger.debug("이메일 알림이 비활성화되었습니다.")
            return False
        
        try:
            # TODO: SMTP 설정 필요
            logger.info(f"이메일 알림 발송: {self.recipient}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 알림 발송 실패: {str(e)}")
            return False


class OpenSearchAlertSender:
    """OpenSearch에 알림 데이터를 저장하는 클래스"""
    
    def __init__(self, opensearch_client=None):
        """
        OpenSearch 클라이언트 초기화
        
        Args:
            opensearch_client: OpenSearch 클라이언트 (None이면 별도 생성)
        """
        self.client = opensearch_client
        self.index_prefix = os.getenv("OPENSEARCH_INDEX_PREFIX", "weather-alert")
        self.enabled = True if opensearch_client else False
    
    def send(self, alert_data: Dict) -> bool:
        """
        OpenSearch에 알림 데이터 저장
        
        Args:
            alert_data: 알림 데이터
        
        Returns:
            bool: 저장 성공 여부
        """
        if not self.enabled or not self.client:
            logger.debug("OpenSearch 저장이 비활성화되었습니다.")
            return False
        
        try:
            timestamp = alert_data.get("timestamp", datetime.now().isoformat())
            
            # 인덱스 이름: weather-alert-2026.04.28
            date_str = datetime.fromisoformat(timestamp).strftime("%Y.%m.%d")
            index_name = f"{self.index_prefix}-{date_str}"
            
            # 문서 생성
            doc = {
                "timestamp": timestamp,
                "region": alert_data.get("region", "전국"),
                "indices": alert_data.get("indices", {}),
                "levels": alert_data.get("levels", {}),
                "recommendations": alert_data.get("recommendations", {}),
                "action_groups": list(alert_data.get("action_groups", {}).keys()),
                "alert_severity": self._calculate_severity(alert_data)
            }
            
            # OpenSearch에 저장
            response = self.client.index(
                index=index_name,
                body=doc
            )
            
            logger.info(f"OpenSearch 저장 성공: {index_name} (ID: {response['_id']})")
            return True
            
        except Exception as e:
            logger.error(f"OpenSearch 저장 실패: {str(e)}")
            return False
    
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
        
        if action_groups.get("외출_자제"):
            return "CRITICAL"
        elif action_groups.get("마스크_필수") or action_groups.get("특보_확인"):
            return "HIGH"
        elif (action_groups.get("자외선_차단") or action_groups.get("보온_필수") or
              action_groups.get("알레르기_주의") or action_groups.get("우산_준비")):
            return "MEDIUM"
        elif action_groups.get("정상"):
            return "LOW"
        else:
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
        self.opensearch_sender = OpenSearchAlertSender(opensearch_client)
    
    def send_all(self, alert_data: Dict) -> Dict[str, bool]:
        """
        모든 활성화된 채널로 알림 발송
        
        Args:
            alert_data: 알림 데이터
        
        Returns:
            {
                "console": True,
                "slack": True,
                "email": False,
                "opensearch": True
            }
        """
        results = {
            "console": self.console_sender.send(alert_data),
            "slack": self.slack_sender.send(alert_data),
            "email": self.email_sender.send(alert_data),
            "opensearch": self.opensearch_sender.send(alert_data)
        }
        
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"알림 발송 결과: {success_count}/{len(results)} 채널 성공")
        
        return results
    
    def send_console_only(self, alert_data: Dict) -> bool:
        """콘솔로만 알림 발송 (개발/테스트용)"""
        return self.console_sender.send(alert_data)
    
    def send_slack_only(self, alert_data: Dict) -> bool:
        """Slack으로만 알림 발송"""
        return self.slack_sender.send(alert_data)


class AlertFormatter:
    """알림 데이터를 포맷팅하는 유틸리티"""
    
    @staticmethod
    def format_for_alert(
        timestamp: str,
        region: str,
        indices: Dict,
        levels: Dict,
        recommendations: Dict,
        emojis: Dict,
        action_groups: Dict
    ) -> Dict:
        """
        알림 데이터를 표준 형식으로 포맷팅
        
        Args:
            timestamp: 타임스탬프
            region: 지역
            indices: 기상지수 값
            levels: 등급
            recommendations: 권고사항
            emojis: 이모지
            action_groups: 행동 그룹
        
        Returns:
            표준화된 알림 데이터
        """
        return {
            "timestamp": timestamp,
            "region": region,
            "indices": indices,
            "levels": levels,
            "recommendations": recommendations,
            "emojis": emojis,
            "action_groups": action_groups
        }


if __name__ == "__main__":
    # 테스트 코드
    print("=" * 80)
    print("알림 발송 모듈 테스트")
    print("=" * 80)
    
    # 테스트 데이터
    test_alert_data = {
        "timestamp": datetime.now().isoformat(),
        "region": "서울",
        "indices": {
            "pm10": 120,
            "pm25": 45,
            "uv_index": 8,
            "ozone": 65,
            "dust": 200,
            "cold_risk": 3,
            "discomfort": 58,
            "feels_like_temp": 15
        },
        "levels": {
            "pm10_level": "나쁨",
            "pm25_level": "보통",
            "uv_index_level": "높음",
            "ozone_level": "보통",
            "dust_level": "보통",
            "cold_risk_level": "주의",
            "discomfort_level": "약간불쾌",
            "feels_like_temp_level": "쾌적"
        },
        "recommendations": {
            "pm10_rec": "마스크 착용 권고",
            "pm25_rec": "민감군 주의",
            "uv_index_rec": "자외선 차단 필수",
            "ozone_rec": "민감군 주의",
            "dust_rec": "창문 닫기 권고",
            "cold_risk_rec": "손씻기 강화",
            "discomfort_rec": "수분 섭취",
            "feels_like_temp_rec": "가벼운 옷 착용"
        },
        "emojis": {
            "pm10_emoji": "😷",
            "pm25_emoji": "😐",
            "uv_index_emoji": "😷",
            "ozone_emoji": "😐",
            "dust_emoji": "😐",
            "cold_risk_emoji": "😐",
            "discomfort_emoji": "😐",
            "feels_like_temp_emoji": "😊"
        },
        "action_groups": {
            "마스크_필수": {
                "color": "🔴",
                "description": "마스크 착용이 필수인 상황",
                "action": "KF94/KF99 마스크 착용 필수",
                "reasons": ["미세먼지 나쁨", "자외선 높음"]
            },
            "자외선_차단": {
                "color": "🟡",
                "description": "자외선 차단이 필요한 상황",
                "action": "선크림 SPF50+ 필수, 자외선 차단 의류 착용",
                "reasons": ["자외선 높음"]
            }
        }
    }
    
    # 알림 매니저 생성 및 발송 테스트
    alert_manager = AlertManager()
    results = alert_manager.send_all(test_alert_data)
    
    print("\n" + "=" * 80)
    print("발송 결과:")
    print("=" * 80)
    for channel, success in results.items():
        status = "✅ 성공" if success else "❌ 실패/비활성화"
        print(f"{channel}: {status}")
