"""공공 API 응답 파싱 계약 테스트 (#43).

공공데이터포털이 스키마·발표시각 규약을 바꾸면 파서가 빈 결과를 돌려주고
상위가 흡수해 전 항목 결측으로 조용히 흘러간다(#35에서 계약 검증으로 태스크
실패까지는 막았지만, 파싱 자체의 회귀는 여기서 잡는다).

픽스처는 실제 응답 형태를 축약한 것이다. 파서가 기대하는 구조가 바뀌면
이 테스트가 CI에서 먼저 깨진다 — 운영에서 결측 경보로 발견하는 것보다 싸다.
"""
import json
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("requests")
pytest.importorskip("dotenv")
pytest.importorskip("kafka")

from producer.producer import (  # noqa: E402
    AirKoreaAPIClient,
    KMAForecastAPIClient,
    WeatherAPIClient,
)
from producer.timeutil import KST  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# ---------- 응답 성공 판정 ----------

def test_success_header_resultcode():
    assert WeatherAPIClient._is_success_response(load("kma_health_index.json")) is True


def test_failure_resultcode_is_not_success():
    bad = load("kma_health_index.json")
    bad["response"]["header"]["resultCode"] = "03"  # NODATA_ERROR
    assert WeatherAPIClient._is_success_response(bad) is False


def test_airkorea_success_variants():
    # 에어코리아는 header.resultCode 또는 response.status 두 형태를 쓴다
    assert AirKoreaAPIClient._is_success_response(load("airkorea_realtime.json")) is True
    assert AirKoreaAPIClient._is_success_response(
        {"response": {"status": "00"}}
    ) is True


# ---------- item 추출: 포털의 세 가지 응답 모양 ----------

def test_extract_items_dict_wrapped_list():
    # {"items": {"item": [...]}} — 기상청 계열
    items = WeatherAPIClient._extract_items(load("kma_health_index.json"))
    assert len(items) == 1
    assert items[0]["code"] == "A07_2"


def test_extract_items_bare_list():
    # {"items": [...]} — 에어코리아 계열
    items = AirKoreaAPIClient._extract_items(load("airkorea_realtime.json"))
    assert len(items) == 3
    assert items[0]["stationName"] == "중구"


def test_extract_items_single_dict_becomes_list():
    # item이 1건이면 dict로 오는 경우가 있다
    data = {"response": {"body": {"items": {"item": {"code": "X"}}}}}
    assert WeatherAPIClient._extract_items(data) == [{"code": "X"}]


def test_extract_items_missing_body_is_empty():
    assert WeatherAPIClient._extract_items({"response": {}}) == []
    assert WeatherAPIClient._extract_items({}) == []


# ---------- XML 폴백: JSON과 같은 구조로 변환되는가 ----------

def test_xml_fallback_matches_json_shape():
    xml_text = (FIXTURES / "kma_health_index.xml").read_text(encoding="utf-8")
    converted = WeatherAPIClient._parse_xml_response(xml_text)

    assert WeatherAPIClient._is_success_response(converted) is True
    items = WeatherAPIClient._extract_items(converted)
    json_items = WeatherAPIClient._extract_items(load("kma_health_index.json"))
    # XML 경로와 JSON 경로가 같은 파싱 결과를 내야 폴백이 의미 있다
    assert items[0]["today"] == json_items[0]["today"]
    assert items[0]["areaNo"] == json_items[0]["areaNo"]


# ---------- 보건지수: 예보 레코드 표준화 ----------

def test_forecast_record_takes_first_available_value():
    client = WeatherAPIClient(api_key="test")
    item = WeatherAPIClient._extract_items(load("kma_health_index.json"))[0]
    record = client._build_forecast_record(item, "서울")
    assert record["value"] == 2.0            # today가 첫 값
    assert record["forecast"]["tomorrow"] == 3.0
    # 빈 문자열 필드는 결측으로 제외된다
    assert "dayaftertomorrow" not in record["forecast"]


# ---------- 자외선: h* 오프셋 → 실제 예보시각 선택 ----------

def test_uv_hourly_selection_skips_blank_markers():
    item = WeatherAPIClient._extract_items(load("kma_uv_index.json"))[0]
    now = datetime(2026, 7, 30, 14, 30, tzinfo=KST)   # base 06시 + 8.5h
    sel = WeatherAPIClient._select_latest_hourly_value(item, "2026073006", now)
    # h12는 "-", h15는 "" — 결측 마커를 건너뛰고 h6(12시 값)을 고른다
    assert sel == {"hour": "h6", "time": "2026073012", "value": 8.0}


def test_uv_before_first_forecast_picks_earliest():
    item = WeatherAPIClient._extract_items(load("kma_uv_index.json"))[0]
    now = datetime(2026, 7, 30, 5, 0, tzinfo=KST)
    assert WeatherAPIClient._select_latest_hourly_value(item, "2026073006", now)["hour"] == "h0"


def test_uv_bad_base_time_returns_none():
    item = {"h0": "1"}
    now = datetime(2026, 7, 30, 12, 0, tzinfo=KST)
    assert WeatherAPIClient._select_latest_hourly_value(item, "not-a-time", now) is None
    assert WeatherAPIClient._select_latest_hourly_value(item, None, now) is None


# ---------- 에어코리아: 측정소 평균 집계 ----------

def test_station_average_ignores_missing_markers():
    items = AirKoreaAPIClient._extract_items(load("airkorea_realtime.json"))
    rep = AirKoreaAPIClient._parse_air_quality_items(items, "서울")

    # pm10: (31 + 45) / 2 — "-"인 용산구는 제외돼야 한다
    assert rep["pm10"] == 38.0
    # pm25: (18 + 22 + 20) / 3
    assert rep["pm25"] == 20.0
    assert rep["station_name"] == "서울 평균"
    assert rep["station_count"] == 3


def test_station_average_all_missing_is_none():
    items = [{"stationName": "A", "pm10Value": "-"}, {"stationName": "B", "pm10Value": ""}]
    rep = AirKoreaAPIClient._parse_air_quality_items(items, "서울")
    assert rep["pm10"] is None      # 0이 아니라 결측 — #33의 계약


# ---------- 발표시각 역산 ----------

def test_latest_index_time_waits_for_delay(monkeypatch):
    """06시 발표 + 지연 30분 규약: 06:10에는 아직 전날 18시 발표분을 쓴다."""
    import producer.clients.kma_index as kma_index

    fixed = datetime(2026, 7, 30, 6, 10, tzinfo=KST)
    monkeypatch.setattr(kma_index, "now_kst", lambda: fixed)
    assert WeatherAPIClient._latest_index_time() == "2026072918"

    fixed2 = datetime(2026, 7, 30, 6, 40, tzinfo=KST)
    monkeypatch.setattr(kma_index, "now_kst", lambda: fixed2)
    assert WeatherAPIClient._latest_index_time() == "2026073006"


def test_latest_base_datetime_respects_delay(monkeypatch):
    import producer.clients.kma_forecast as kma_forecast

    fixed = datetime(2026, 7, 30, 2, 5, tzinfo=KST)   # 02시 발표 + 지연 10분 전
    monkeypatch.setattr(kma_forecast, "now_kst", lambda: fixed)
    client = KMAForecastAPIClient.__new__(KMAForecastAPIClient)
    base = client._latest_base_datetime([2, 5, 8, 11, 14, 17, 20, 23], 10)
    assert base.strftime("%Y%m%d%H") == "2026072923"

    fixed2 = datetime(2026, 7, 30, 2, 15, tzinfo=KST)
    monkeypatch.setattr(kma_forecast, "now_kst", lambda: fixed2)
    assert client._latest_base_datetime(
        [2, 5, 8, 11, 14, 17, 20, 23], 10
    ).strftime("%Y%m%d%H") == "2026073002"
