"""패키지가 실제로 import 되는지 검증.

compileall은 문법만 본다. `__init__.py`가 존재하지 않는 이름을 export 해도
통과한다. 실제로 producer/__init__.py가 삭제된 main을 계속 export 하는 바람에
DAG의 `from producer.producer import WeatherDataCollector`가 ImportError로
죽었고, CI는 초록색이었다. 그 회귀를 막는 테스트다.

DAG와 Consumer 컨테이너가 실제로 밟는 import 경로를 그대로 따라간다.
"""
import importlib

import pytest

pytest.importorskip("kafka", reason="producer 패키지가 kafka-python에 의존")
pytest.importorskip("opensearchpy", reason="consumer 패키지가 opensearch-py에 의존")
pytest.importorskip("requests")
pytest.importorskip("dotenv")


def test_producer_package_imports():
    """DAG가 밟는 경로: from producer.producer import WeatherDataCollector"""
    module = importlib.import_module("producer.producer")
    assert hasattr(module, "WeatherDataCollector")


def test_consumer_package_imports():
    """Consumer 컨테이너가 밟는 경로."""
    module = importlib.import_module("consumer.consumer")
    assert hasattr(module, "WeatherAlertConsumer")


@pytest.mark.parametrize("package", ["producer", "consumer"])
def test_declared_exports_exist(package):
    """__all__에 적힌 이름이 실제로 존재하는지.

    이 단언이 깨졌던 것이 원래 사고다 — producer가 삭제된 main을 export했다.
    """
    module = importlib.import_module(package)
    missing = [name for name in getattr(module, "__all__", []) if not hasattr(module, name)]
    assert not missing, f"{package}.__all__에 없는 이름: {missing}"


def test_dag_side_helpers_import():
    """DAG가 태스크 안에서 지연 import 하는 모듈들."""
    for name in ("producer.contract", "producer.masking", "producer.timeutil"):
        assert importlib.import_module(name) is not None
