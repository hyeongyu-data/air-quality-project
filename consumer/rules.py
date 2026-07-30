"""
기상지수 임계값 및 알림 규칙 정의

각 기상지수별로:
- 임계값 범위 정의
- 등급 분류 (좋음/보통/나쁨/매우나쁨)
- 권고 행동 그룹화
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class AlertLevel(Enum):
    """알림 등급"""
    GOOD = "좋음"
    NORMAL = "보통"
    BAD = "나쁨"
    VERY_BAD = "매우나쁨"
    UNKNOWN = "정보없음"


# 수집 실패로 값이 없을 때의 판정 결과.
# 결측을 0으로 치환하면 "체감온도 0℃ → 동상 위험" 같은 오탐이나
# "미세먼지 0 → 외출 자유" 같은 거짓 안전 신호가 나간다. 모른다를
# 괜찮다로도 위험하다로도 바꾸지 않고, 모른다로 그대로 전달한다.
UNKNOWN_RESULT = (AlertLevel.UNKNOWN, "데이터 수집 실패로 판정할 수 없음", "❔")


# 이 둘이 모두 결측이면 알림의 핵심 근거가 사라진다고 본다.
CORE_INDICES = ("pm10", "pm25")


def core_indices_unknown(classification_objects: Dict) -> bool:
    """핵심 지수(미세먼지/초미세먼지)가 전부 결측인지 판단.

    키가 아예 없는 경우도 결측으로 본다. 값을 못 받은 것과 항목이 빠진 것은
    "근거가 없다"는 점에서 같고, 근거 없이 외부 알림을 보내지 않는 쪽이 안전하다.
    """
    return all(
        classification_objects.get(key) in (None, AlertLevel.UNKNOWN)
        for key in CORE_INDICES
    )


def grade_signature(classification_objects: Dict) -> str:
    """각 지수의 등급(AlertLevel)만 뽑아 안정적 문자열 시그니처 생성.

    수치 변동엔 둔감하고 등급 변화에만 민감하다. 키를 정렬하므로
    dict 순서가 달라도 같은 등급 조합이면 같은 시그니처가 나온다.
    쿨다운/중복제거의 "직전 상태" 비교 기준으로 쓴다.
    """
    parts = []
    for key in sorted(classification_objects):
        level = classification_objects[key]
        value = level.value if isinstance(level, AlertLevel) else str(level)
        parts.append(f"{key}={value}")
    return "|".join(parts)


def should_send(
    previous_signature: Optional[str],
    current_signature: str,
    last_sent_at: Optional[str] = None,
    max_silence_seconds: Optional[float] = None,
    now: Optional[datetime] = None,
) -> bool:
    """외부 채널로 발송할지 판정한다.

    기본 규칙: 이전 시그니처가 없거나(최초) 등급이 바뀌었으면 True.

    최대 무발송 간격: 등급이 오래 유지되면 쿨다운이 발송을 계속 생략하는데,
    사용자는 "조용한 게 정상인지 파이프라인이 죽은 건지" 구분할 수 없다.
    마지막 발송에서 max_silence_seconds가 지났으면 등급이 같아도 다시 보낸다.

    last_sent_at 파싱 실패나 미기록은 간격 판정을 건너뛴다(기본 규칙만 적용) —
    상태가 불완전하다고 발송을 늘리는 쪽으로 기울면 스팸이 된다.
    """
    if previous_signature != current_signature:
        return True
    if not max_silence_seconds or not last_sent_at:
        return False
    try:
        sent = datetime.fromisoformat(last_sent_at)
    except (TypeError, ValueError):
        return False
    if sent.tzinfo is None:
        return False
    current = now or datetime.now(sent.tzinfo)
    return (current - sent).total_seconds() >= max_silence_seconds


def should_record_signature(
    send_external: bool,
    external_enabled: bool,
    delivered_channels: List[str],
) -> bool:
    """이번 등급을 "알렸다"고 기록해도 되는지 판단.

    쿨다운은 저장된 시그니처를 "이 등급은 이미 알렸다"는 뜻으로 읽는다.
    그런데 발송 결과와 무관하게 기록하면, SMTP 만료나 카카오 토큰 만료로
    전 채널이 실패한 순간의 등급이 그대로 굳어 등급이 유지되는 동안
    알림이 영구히 사라진다. 채널이 복구돼도 마찬가지다.

    - 발송을 시도하지 않았으면(쿨다운/근거 부족) 상태는 그대로 유지한다.
    - 활성화된 외부 채널이 하나도 없으면 애초에 전달할 대상이 없으므로 기록한다.
    - 시도했고 대상이 있었는데 전부 실패했다면 기록하지 않는다 -> 다음 회차 재시도.
    """
    if not send_external or not external_enabled:
        return True
    return bool(delivered_channels)


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
    def classify_special_notice(value: Optional[str]) -> Tuple[AlertLevel, str, str]:
        """기타특보성 신호 판정.

        프로듀서는 특보 없음을 "없음", 수집 실패를 "정보없음"으로 구분해 보낸다.
        후자를 "좋음"으로 읽으면 확인하지 못한 상태가 안전 신호가 되므로 분리한다.
        """
        if value in (None, "", "정보없음"):
            return UNKNOWN_RESULT
        if value != "없음":
            return AlertLevel.BAD, "특보성 신호 확인 | 최신 기상정보 확인 권고", "⚠️"
        return AlertLevel.GOOD, "특보성 신호 없음", "😊"
    
    @staticmethod
    def classify_feels_like_temp(value: float) -> Tuple[AlertLevel, str, str]:
        """체감온도 판정"""
        return AlertRuleEngine._classify(value, WeatherIndexRules.FEELS_LIKE_TEMP_RULES)
    
    @staticmethod
    def _classify(
        value: Optional[float],
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
        if value is None:
            return UNKNOWN_RESULT

        for rule in rules:
            if value <= rule.max_value:
                return rule.level, rule.recommendation, rule.emoji
        
        # 범위를 벗어난 경우 마지막 규칙 적용
        last_rule = rules[-1]
        return last_rule.level, last_rule.recommendation, last_rule.emoji


class AlertGrouping:
    """알림을 행동 그룹으로 분류"""
    
    # 행동 그룹 정의.
    # 조건은 group_alerts() 코드가 단일 출처다. 예전에는 여기에 conditions 필드로
    # 조건을 한 번 더 적었는데, 아무도 읽지 않는 사이 실제 로직과 어긋났다
    # (존재하지 않는 지수를 가리키고 있었다).
    ACTION_GROUPS = {
        "마스크_필수": {
            "description": "마스크 착용이 필수인 상황",
            "action": "KF94/KF99 마스크 착용 필수",
            "color": "🔴"
        },
        "외출_자제": {
            "description": "외출을 자제해야 하는 상황",
            "action": "가급적 외출 자제, 부득이한 경우만 외출",
            "color": "🔴"
        },
        "자외선_차단": {
            "description": "자외선 차단이 필요한 상황",
            "action": "선크림 SPF50+ 필수, 자외선 차단 의류 착용",
            "color": "🟡"
        },
        "알레르기_주의": {
            "description": "꽃가루 알레르기 주의가 필요한 상황",
            "action": "마스크 착용, 외출 후 세안, 창문 닫기 권고",
            "color": "🟡"
        },
        "우산_준비": {
            "description": "강수 가능성이 높은 상황",
            "action": "우산 또는 우비 준비",
            "color": "🔵"
        },
        "특보_확인": {
            "description": "특보성 기상 신호가 있는 상황",
            "action": "최신 기상특보와 외출 안전 확인",
            "color": "🔴"
        },
        "보온_필수": {
            "description": "보온이 필요한 상황",
            "action": "내복, 외투, 장갑, 목도리 필수",
            "color": "🔵"
        },
        "수분_섭취": {
            "description": "수분 섭취가 필요한 상황",
            "action": "충분한 수분 섭취, 가볍고 통풍 좋은 옷 착용",
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
            classification_results.get("dust") == AlertLevel.VERY_BAD):
            activate_group("외출_자제", [
                f"미세먼지: {format_level(classification_results.get('pm10'))}",
                f"황사: {format_level(classification_results.get('dust'))}"
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
        if classification_results.get("feels_like_temp") in [AlertLevel.BAD, AlertLevel.VERY_BAD]:
            activate_group("수분_섭취", [
                f"체감온도: {format_level(classification_results.get('feels_like_temp'))}"
            ])
        
        if activated_groups:
            return activated_groups
        
        # 활성화된 그룹이 없을 때, 결측이 섞여 있으면 "정상"이라고 말할 수 없다.
        if any(level is AlertLevel.UNKNOWN for level in classification_results.values()):
            unknown_keys = [
                key for key, level in classification_results.items()
                if level is AlertLevel.UNKNOWN
            ]
            return {"정보부족": {
                "description": "일부 지수를 수집하지 못해 판정할 수 없음",
                "action": "수집 실패 항목은 기상청·에어코리아에서 직접 확인 권고",
                "color": "❔",
                "reasons": [f"수집 실패: {', '.join(sorted(unknown_keys))}"],
            }}

        normal_info = {
            "description": "모든 지수가 정상범위",
            "action": "특별 조치 없음",
            "color": "✅",
            "reasons": ["모든 지수가 정상범위"],
        }
        return {"정상": normal_info}
