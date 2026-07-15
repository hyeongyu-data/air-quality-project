"""
기상지수 임계값 및 알림 규칙 정의

각 기상지수별로:
- 임계값 범위 정의
- 등급 분류 (좋음/보통/나쁨/매우나쁨)
- 권고 행동 그룹화
"""

from typing import Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum


class AlertLevel(Enum):
    """알림 등급"""
    GOOD = "좋음"
    NORMAL = "보통"
    BAD = "나쁨"
    VERY_BAD = "매우나쁨"


@dataclass
class AlertRule:
    """단일 알림 규칙"""
    max_value: float
    level: AlertLevel
    recommendation: str
    emoji: str  # 시각적 표현


class WeatherIndexRules:
    """모든 기상지수의 임계값 및 규칙 정의"""
    
    # ============ 미세먼지 (PM10) ============
    PM10_RULES: List[AlertRule] = [
        AlertRule(
            max_value=30,
            level=AlertLevel.GOOD,
            recommendation="외출 자유 | 창문 개방 좋음",
            emoji="😊"
        ),
        AlertRule(
            max_value=80,
            level=AlertLevel.NORMAL,
            recommendation="민감군(어린이, 노약자) 실외활동 제한 권고",
            emoji="😐"
        ),
        AlertRule(
            max_value=150,
            level=AlertLevel.BAD,
            recommendation="마스크 착용 권고 | 실외활동 최소화",
            emoji="😷"
        ),
        AlertRule(
            max_value=500,
            level=AlertLevel.VERY_BAD,
            recommendation="외출 자제 필수 | KF94 마스크 필수 | 외출 최소화",
            emoji="⚠️"
        ),
    ]
    
    # ============ 초미세먼지 (PM2.5) ============
    PM25_RULES: List[AlertRule] = [
        AlertRule(
            max_value=15,
            level=AlertLevel.GOOD,
            recommendation="외출 자유 | 창문 개방 좋음",
            emoji="😊"
        ),
        AlertRule(
            max_value=35,
            level=AlertLevel.NORMAL,
            recommendation="민감군 주의 | 마스크 착용 권고",
            emoji="😐"
        ),
        AlertRule(
            max_value=75,
            level=AlertLevel.BAD,
            recommendation="마스크 착용 필수 | 실외활동 최소화",
            emoji="😷"
        ),
        AlertRule(
            max_value=500,
            level=AlertLevel.VERY_BAD,
            recommendation="외출 자제 | KF94/KF99 마스크 필수 | 외출 금지",
            emoji="⚠️"
        ),
    ]
    
    # ============ 자외선 지수 ============
    UV_INDEX_RULES: List[AlertRule] = [
        AlertRule(
            max_value=2,
            level=AlertLevel.GOOD,
            recommendation="자외선 차단 불필요 | 야외활동 자유",
            emoji="😊"
        ),
        AlertRule(
            max_value=5,
            level=AlertLevel.NORMAL,
            recommendation="선크림 SPF30+ 권장 | 모자 쓰기 좋음",
            emoji="😐"
        ),
        AlertRule(
            max_value=7,
            level=AlertLevel.BAD,
            recommendation="선크림 SPF50+ 필수 | 자외선 차단 의류 착용 | 야외 활동 최소화",
            emoji="😷"
        ),
        AlertRule(
            max_value=20,
            level=AlertLevel.VERY_BAD,
            recommendation="정오(10시-3시) 외출 자제 | 선크림 2시간마다 재도포 | 자외선 차단복 필수",
            emoji="⚠️"
        ),
    ]
    
    # ============ 황사 ============
    DUST_RULES: List[AlertRule] = [
        AlertRule(
            max_value=150,
            level=AlertLevel.GOOD,
            recommendation="외출 자유",
            emoji="😊"
        ),
        AlertRule(
            max_value=300,
            level=AlertLevel.NORMAL,
            recommendation="민감군 마스크 착용 | 창문 닫기 권고",
            emoji="😐"
        ),
        AlertRule(
            max_value=500,
            level=AlertLevel.BAD,
            recommendation="마스크 착용 필수 | 외출 최소화 | 창문 닫기",
            emoji="😷"
        ),
        AlertRule(
            max_value=10000,
            level=AlertLevel.VERY_BAD,
            recommendation="외출 금지 | 실내 활동만 | 공기청정기 가동 | 환기 금지",
            emoji="⚠️"
        ),
    ]
    
    # ============ 꽃가루농도위험지수 ============
    POLLEN_RULES: List[AlertRule] = [
        AlertRule(
            max_value=1,
            level=AlertLevel.GOOD,
            recommendation="꽃가루 위험 낮음 | 일상활동 가능",
            emoji="😊"
        ),
        AlertRule(
            max_value=2,
            level=AlertLevel.NORMAL,
            recommendation="알레르기 민감군 주의 | 외출 후 세안 권고",
            emoji="😐"
        ),
        AlertRule(
            max_value=3,
            level=AlertLevel.BAD,
            recommendation="마스크 착용 권고 | 창문 닫기 | 외출 후 의류 털기",
            emoji="😷"
        ),
        AlertRule(
            max_value=4,
            level=AlertLevel.VERY_BAD,
            recommendation="알레르기 민감군 외출 자제 | 실내 공기관리 권고",
            emoji="⚠️"
        ),
    ]
    
    # ============ 강수확률 ============
    PRECIPITATION_RULES: List[AlertRule] = [
        AlertRule(
            max_value=30,
            level=AlertLevel.GOOD,
            recommendation="우산 없이 활동 가능",
            emoji="😊"
        ),
        AlertRule(
            max_value=60,
            level=AlertLevel.NORMAL,
            recommendation="우산 준비 권고",
            emoji="😐"
        ),
        AlertRule(
            max_value=80,
            level=AlertLevel.BAD,
            recommendation="우산 필요 | 이동 시 비 대비",
            emoji="☔"
        ),
        AlertRule(
            max_value=100,
            level=AlertLevel.VERY_BAD,
            recommendation="강수 가능성 높음 | 외출 계획 조정 권고",
            emoji="⚠️"
        ),
    ]
    
    # ============ 생활기상지수 - 체감온도 ============
    FEELS_LIKE_TEMP_RULES: List[AlertRule] = [
        AlertRule(
            max_value=0,
            level=AlertLevel.VERY_BAD,
            recommendation="매우 추운 날 | 동상 위험 | 내복 + 외투 + 장갑 필수",
            emoji="❄️"
        ),
        AlertRule(
            max_value=5,
            level=AlertLevel.BAD,
            recommendation="추운 날 | 두터운 외투 필수 | 모자, 장갑, 목도리 필수",
            emoji="😷"
        ),
        AlertRule(
            max_value=10,
            level=AlertLevel.NORMAL,
            recommendation="쌀쌀함 | 얇은 외투 + 내복 권고",
            emoji="😐"
        ),
        AlertRule(
            max_value=20,
            level=AlertLevel.GOOD,
            recommendation="쾌적한 온도 | 편한 복장",
            emoji="😊"
        ),
        AlertRule(
            max_value=100,
            level=AlertLevel.BAD,
            recommendation="더운 날 | 얇은 복장 | 수분 섭취 | 외출 최소화",
            emoji="🌡️"
        ),
    ]


class AlertRuleEngine:
    """알림 규칙 판정 엔진"""
    
    @staticmethod
    def classify_pm10(value: float) -> Tuple[AlertLevel, str, str]:
        """
        미세먼지 판정
        
        Args:
            value: PM10 수치 (μg/m³)
        
        Returns:
            (등급, 권고사항, 이모지)
        """
        return AlertRuleEngine._classify(value, WeatherIndexRules.PM10_RULES)
    
    @staticmethod
    def classify_pm25(value: float) -> Tuple[AlertLevel, str, str]:
        """초미세먼지 판정"""
        return AlertRuleEngine._classify(value, WeatherIndexRules.PM25_RULES)
    
    @staticmethod
    def classify_uv_index(value: float) -> Tuple[AlertLevel, str, str]:
        """자외선 지수 판정"""
        return AlertRuleEngine._classify(value, WeatherIndexRules.UV_INDEX_RULES)
    
    @staticmethod
    def classify_dust(value: float) -> Tuple[AlertLevel, str, str]:
        """황사 판정"""
        return AlertRuleEngine._classify(value, WeatherIndexRules.DUST_RULES)
    
    @staticmethod
    def classify_pollen(value: float) -> Tuple[AlertLevel, str, str]:
        """꽃가루농도위험지수 판정"""
        return AlertRuleEngine._classify(value, WeatherIndexRules.POLLEN_RULES)
    
    @staticmethod
    def classify_precipitation_probability(value: float) -> Tuple[AlertLevel, str, str]:
        """강수확률 판정"""
        return AlertRuleEngine._classify(value, WeatherIndexRules.PRECIPITATION_RULES)
    
    @staticmethod
    def classify_special_notice(value: str) -> Tuple[AlertLevel, str, str]:
        """기타특보성 신호 판정"""
        if value and value not in ["없음", "정보없음"]:
            return AlertLevel.BAD, "특보성 신호 확인 | 최신 기상정보 확인 권고", "⚠️"
        return AlertLevel.GOOD, "특보성 신호 없음", "😊"
    
    @staticmethod
    def classify_feels_like_temp(value: float) -> Tuple[AlertLevel, str, str]:
        """체감온도 판정"""
        return AlertRuleEngine._classify(value, WeatherIndexRules.FEELS_LIKE_TEMP_RULES)
    
    @staticmethod
    def _classify(
        value: float, 
        rules: List[AlertRule]
    ) -> Tuple[AlertLevel, str, str]:
        """
        범용 분류 함수
        
        Args:
            value: 측정값
            rules: 규칙 리스트 (max_value 기준으로 오름차순 정렬되어야 함)
        
        Returns:
            (등급, 권고사항, 이모지)
        """
        for rule in rules:
            if value <= rule.max_value:
                return rule.level, rule.recommendation, rule.emoji
        
        # 범위를 벗어난 경우 마지막 규칙 적용
        last_rule = rules[-1]
        return last_rule.level, last_rule.recommendation, last_rule.emoji


class AlertGrouping:
    """알림을 행동 그룹으로 분류"""
    
    # 행동 그룹 정의
    ACTION_GROUPS = {
        "마스크_필수": {
            "description": "마스크 착용이 필수인 상황",
            "conditions": ["미세먼지 나쁨 이상", "황사 중간 이상", "오존 높음 이상"],
            "action": "KF94/KF99 마스크 착용 필수",
            "color": "🔴"
        },
        "외출_자제": {
            "description": "외출을 자제해야 하는 상황",
            "conditions": ["미세먼지 매우나쁨", "황사 매우나쁨", "감기 고위험"],
            "action": "가급적 외출 자제, 부득이한 경우만 외출",
            "color": "🔴"
        },
        "자외선_차단": {
            "description": "자외선 차단이 필요한 상황",
            "conditions": ["자외선 높음 이상"],
            "action": "선크림 SPF50+ 필수, 자외선 차단 의류 착용",
            "color": "🟡"
        },
        "알레르기_주의": {
            "description": "꽃가루 알레르기 주의가 필요한 상황",
            "conditions": ["꽃가루농도위험지수 나쁨 이상"],
            "action": "마스크 착용, 외출 후 세안, 창문 닫기 권고",
            "color": "🟡"
        },
        "우산_준비": {
            "description": "강수 가능성이 높은 상황",
            "conditions": ["강수확률 60% 이상"],
            "action": "우산 또는 우비 준비",
            "color": "🔵"
        },
        "특보_확인": {
            "description": "특보성 기상 신호가 있는 상황",
            "conditions": ["낙뢰/강풍/강수 가능 신호"],
            "action": "최신 기상특보와 외출 안전 확인",
            "color": "🔴"
        },
        "보온_필수": {
            "description": "보온이 필요한 상황",
            "conditions": ["체감온도 0도 이하"],
            "action": "내복, 외투, 장갑, 목도리 필수",
            "color": "🔵"
        },
        "수분_섭취": {
            "description": "수분 섭취가 필요한 상황",
            "conditions": ["불쾌지수 높음 이상", "체감온도 25도 이상"],
            "action": "충분한 수분 섭취, 가볍고 통풍 좋은 옷 착용",
            "color": "🟡"
        },
        "위생_강화": {
            "description": "개인위생 강화가 필요한 상황",
            "conditions": ["감기 주의 이상"],
            "action": "손씻기, 손소독, 사람 많은 곳 피하기",
            "color": "🟡"
        },
    }
    
    @staticmethod
    def group_alerts(classification_results: Dict) -> Dict[str, Dict]:
        """
        여러 기상지수 판정 결과를 행동 그룹으로 묶기
        
        Args:
            classification_results: {
                "pm10": AlertLevel,
                "pm25": AlertLevel,
                ...
            }
        
        Returns:
            {
                "마스크_필수": {
                    "description": "...",
                    "action": "...",
                    "reasons": ["미세먼지: 나쁨", ...]
                },
                ...
            }
        """
        activated_groups = {}
        
        def format_level(level: AlertLevel) -> str:
            return level.value if isinstance(level, AlertLevel) else "정보없음"
        
        def activate_group(group_name: str, reasons: List[str]) -> None:
            group_info = AlertGrouping.ACTION_GROUPS[group_name].copy()
            group_info["reasons"] = reasons
            activated_groups[group_name] = group_info
        
        # 미세먼지/황사 기반 마스크 필요
        if (classification_results.get("pm10") in [AlertLevel.BAD, AlertLevel.VERY_BAD] or
            classification_results.get("pm25") in [AlertLevel.BAD, AlertLevel.VERY_BAD] or
            classification_results.get("dust") in [AlertLevel.NORMAL, AlertLevel.BAD, AlertLevel.VERY_BAD]):
            activate_group("마스크_필수", [
                f"미세먼지: {format_level(classification_results.get('pm10'))}",
                f"초미세먼지: {format_level(classification_results.get('pm25'))}",
                f"황사: {format_level(classification_results.get('dust'))}"
            ])
        
        # 외출 자제
        if (classification_results.get("pm10") == AlertLevel.VERY_BAD or
            classification_results.get("dust") == AlertLevel.VERY_BAD or
            classification_results.get("cold_risk") == AlertLevel.VERY_BAD):
            activate_group("외출_자제", [
                f"미세먼지: {format_level(classification_results.get('pm10'))}",
                f"황사: {format_level(classification_results.get('dust'))}",
                f"감기위험: {format_level(classification_results.get('cold_risk'))}"
            ])
        
        # 자외선 차단
        if classification_results.get("uv_index") in [AlertLevel.BAD, AlertLevel.VERY_BAD]:
            activate_group("자외선_차단", [
                f"자외선 지수: {format_level(classification_results.get('uv_index'))}"
            ])
        
        # 꽃가루 알레르기 주의
        if (classification_results.get("oak_pollen") in [AlertLevel.BAD, AlertLevel.VERY_BAD] or
            classification_results.get("pine_pollen") in [AlertLevel.BAD, AlertLevel.VERY_BAD]):
            activate_group("알레르기_주의", [
                f"꽃가루 참나무: {format_level(classification_results.get('oak_pollen'))}",
                f"꽃가루 소나무: {format_level(classification_results.get('pine_pollen'))}"
            ])
        
        # 우산 준비
        if classification_results.get("precipitation_probability") in [AlertLevel.BAD, AlertLevel.VERY_BAD]:
            activate_group("우산_준비", [
                f"강수확률: {format_level(classification_results.get('precipitation_probability'))}"
            ])
        
        # 기타 특보 확인
        if classification_results.get("other_special_notice") in [AlertLevel.BAD, AlertLevel.VERY_BAD]:
            activate_group("특보_확인", [
                f"기타특보: {format_level(classification_results.get('other_special_notice'))}"
            ])
        
        # 보온 필수 (체감온도 0도 이하)
        if classification_results.get("feels_like_temp") == AlertLevel.VERY_BAD:
            activate_group("보온_필수", [
                f"체감온도: {format_level(classification_results.get('feels_like_temp'))}"
            ])
        
        # 수분 섭취
        if (
            classification_results.get("discomfort") in [AlertLevel.BAD, AlertLevel.VERY_BAD] or
            classification_results.get("feels_like_temp") in [AlertLevel.BAD, AlertLevel.VERY_BAD]
        ):
            activate_group("수분_섭취", [
                f"불쾌지수: {format_level(classification_results.get('discomfort'))}",
                f"체감온도: {format_level(classification_results.get('feels_like_temp'))}"
            ])
        
        # 위생 강화
        if classification_results.get("cold_risk") in [AlertLevel.NORMAL, AlertLevel.BAD]:
            activate_group("위생_강화", [
                f"감기위험: {format_level(classification_results.get('cold_risk'))}"
            ])
        
        if activated_groups:
            return activated_groups
        
        normal_info = {
            "description": "모든 지수가 정상범위",
            "conditions": [],
            "action": "특별 조치 없음",
            "color": "✅",
            "reasons": ["모든 지수가 정상범위"],
        }
        return {"정상": normal_info}


if __name__ == "__main__":
    # 테스트 코드
    print("=" * 60)
    print("기상지수 알림 규칙 테스트")
    print("=" * 60)
    
    # 미세먼지 테스트
    pm10_value = 120
    level, recommendation, emoji = AlertRuleEngine.classify_pm10(pm10_value)
    print(f"\n미세먼지: {pm10_value} μg/m³")
    print(f"등급: {emoji} {level.value}")
    print(f"권고: {recommendation}")
    
    # 자외선 테스트
    uv_value = 8
    level, recommendation, emoji = AlertRuleEngine.classify_uv_index(uv_value)
    print(f"\n자외선 지수: {uv_value}")
    print(f"등급: {emoji} {level.value}")
    print(f"권고: {recommendation}")
    
    # 알림 그룹화 테스트
    test_results = {
        "pm10": AlertLevel.BAD,
        "pm25": AlertLevel.NORMAL,
        "uv_index": AlertLevel.BAD,
        "dust": AlertLevel.NORMAL,
        "cold_risk": AlertLevel.NORMAL,
        "discomfort": AlertLevel.NORMAL,
        "feels_like_temp": AlertLevel.GOOD,
    }
    
    print("\n" + "=" * 60)
    print("알림 그룹화 결과")
    print("=" * 60)
    grouped = AlertGrouping.group_alerts(test_results)
    for group_name, group_info in grouped.items():
        print(f"\n{group_info.get('color', '')} {group_name}")
        print(f"  설명: {group_info.get('description', '')}")
        print(f"  행동: {group_info.get('action', '')}")
        for reason in group_info.get("reasons", []):
            print(f"  원인: {reason}")
