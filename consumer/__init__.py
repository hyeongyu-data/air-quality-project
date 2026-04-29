"""
기상지수 알림 컨슈머 패키지

Kafka 토픽에서 기상 데이터를 구독하여
실시간 알림을 발송하는 기능을 제공합니다.
"""

from .rules import AlertRuleEngine, AlertGrouping, AlertLevel
from .alert import AlertManager, AlertFormatter
from .consumer import (
    WeatherDataProcessor,
    KafkaWeatherConsumer,
    WeatherAlertConsumer,
    main
)

__all__ = [
    "AlertRuleEngine",
    "AlertGrouping",
    "AlertLevel",
    "AlertManager",
    "AlertFormatter",
    "WeatherDataProcessor",
    "KafkaWeatherConsumer",
    "WeatherAlertConsumer",
    "main"
]