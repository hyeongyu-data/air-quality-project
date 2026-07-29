"""
기상지수 데이터 프로듀서 패키지

기상청/에어코리아 공공API에서 데이터를 수집하여
Kafka로 발행하는 기능을 제공합니다.
"""

from .producer import (
    WeatherAPIClient,
    AirKoreaAPIClient,
    KMAForecastAPIClient,
    KafkaWeatherProducer,
    WeatherDataCollector,
)

__all__ = [
    "WeatherAPIClient",
    "AirKoreaAPIClient",
    "KMAForecastAPIClient",
    "KafkaWeatherProducer",
    "WeatherDataCollector",
]
