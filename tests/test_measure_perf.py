"""성능 측정 스크립트의 파서·렌더·비교 로직 검증 (#122)."""
import pytest

pytest.importorskip("opensearchpy")
pytest.importorskip("kafka")

from consumer.measure_perf import (
    _norm_since,
    _since_days,
    build_metrics_query,
    compare,
    parse_api_durations,
    parse_kafka_lag,
    parse_metrics,
    render_md,
)


def test_norm_since_adds_day_unit():
    assert _norm_since("7") == "7d"
    assert _norm_since("24h") == "24h"
    assert _norm_since("2w") == "2w"


def test_since_days():
    assert _since_days("7d") == 7.0
    assert _since_days("24h") == 1.0
    assert _since_days("1w") == 7.0


def test_render_md_synthetic_suppresses_throughput():
    data = {
        "meta": {"measured_at": "x", "since": "1h", "n_note": "synthetic_load: 60",
                 "consumer": "x", "git": "abc"},
        "metrics": {"n": 60, "throughput_per_day": 999999.0,
                    "e2e_latency_seconds": {"p50": None, "p95": None, "p99": None},
                    "process_duration_ms": {"p50": 10.0, "p95": 20.0, "p99": 30.0},
                    "index_success_rate": 1.0, "missing_rate": 1.0},
        "kafka_lag": {}, "regression": ["첫 회차"],
    }
    md = render_md(data)
    assert "999999" not in md
    assert "합성 부하" in md
    assert "— / — / —" in md  # None 은 대시로


def test_build_query_has_percentile_aggs():
    q = build_metrics_query("7d")
    assert q["aggs"]["e2e"]["percentiles"]["field"] == "e2e_latency_seconds"
    assert 95 in q["aggs"]["proc"]["percentiles"]["percents"]
    assert q["query"]["range"]["timestamp"]["gte"] == "now-7d"


def test_parse_metrics_from_es_response():
    resp = {
        "hits": {"total": {"value": 100}},
        "aggregations": {
            "e2e": {"values": {"50.0": 4.4, "95.0": 16.1, "99.0": 22.0}},
            "proc": {"values": {"50.0": 100.6, "95.0": 300.2, "99.0": 480.0}},
            "indexed": {"buckets": [{"key": 1, "key_as_string": "true", "doc_count": 98},
                                    {"key": 0, "key_as_string": "false", "doc_count": 2}]},
            "missing": {"buckets": [{"key": "1.0-*", "doc_count": 7}]},
        },
    }
    m = parse_metrics(resp, since_days=7)
    assert m["n"] == 100
    assert m["throughput_per_day"] == round(100 / 7, 1)
    assert m["e2e_latency_seconds"]["p95"] == 16.1
    assert m["process_duration_ms"]["p50"] == 100.6
    assert m["index_success_rate"] == 0.98
    assert m["missing_rate"] == 0.07


def test_parse_metrics_empty():
    resp = {"hits": {"total": {"value": 0}}, "aggregations": {}}
    m = parse_metrics(resp, since_days=1)
    assert m["n"] == 0 and m["index_success_rate"] is None


def test_parse_kafka_lag():
    committed = {0: 10, 1: 20}
    ends = {0: 13, 1: 20}
    assert parse_kafka_lag(committed, ends) == {0: 3, 1: 0}


def test_parse_kafka_lag_missing_end_and_negative():
    # end 가 없는 파티션은 건너뛰고, 음수 랙은 0 으로 클램프
    assert parse_kafka_lag({0: 10, 1: 5}, {0: 8}) == {0: 0}


def test_parse_api_durations():
    lines = [
        "... INFO ... api_call api=kma status=200 duration_ms=250",
        "... INFO ... api_call api=kma status=200 duration_ms=450",
        "... INFO ... api_call api=airkorea status=200 duration_ms=1200",
        "unrelated log line",
    ]
    d = parse_api_durations(lines)
    assert d["kma"]["n"] == 2
    assert d["kma"]["p50"] == 350.0
    assert d["airkorea"]["n"] == 1 and d["airkorea"]["p95"] == 1200.0


def _report(**over):
    m = {"e2e_latency_seconds": {"p95": 16.0}, "process_duration_ms": {"p95": 300.0},
         "index_success_rate": 0.99}
    m.update(over.get("metrics", {}))
    return {"metrics": m, "kafka_lag": over.get("kafka_lag", {0: 1})}


def test_compare_flags_p95_regression():
    prev = _report()
    curr = _report(metrics={"process_duration_ms": {"p95": 480.0}})  # +60%
    out = compare(prev, curr)
    assert any("process_duration_ms p95" in r for r in out)


def test_compare_no_regression_on_small_delta():
    prev = _report()
    curr = _report(metrics={"process_duration_ms": {"p95": 320.0}})  # +6.7%
    assert compare(prev, curr) == ["회귀 없음"]


def test_compare_flags_low_index_rate_and_lag():
    curr = _report(metrics={"index_success_rate": 0.90}, kafka_lag={0: 15})
    out = compare(_report(), curr)
    assert any("색인 성공률" in r for r in out)
    assert any("max lag" in r for r in out)


def test_compare_first_run():
    assert compare(None, _report()) == ["첫 회차 — 비교 대상 없음"]


def test_render_md_has_headers_and_table():
    data = {
        "meta": {"measured_at": "2026-09-11 14:00 KST", "since": "7d",
                 "n_note": "real traffic", "consumer": "x", "git": "abc1234"},
        "metrics": {"n": 42, "throughput_per_day": 6.0,
                    "e2e_latency_seconds": {"p50": 4.4, "p95": 16.1, "p99": 22.0},
                    "process_duration_ms": {"p50": 100.6, "p95": 300.2, "p99": 480.0},
                    "index_success_rate": 0.98, "missing_rate": 0.1},
        "kafka_lag": {0: 2}, "api_durations": {}, "regression": ["회귀 없음"],
    }
    md = render_md(data)
    assert "# 성능 측정 2026-09-11 14:00 KST" in md
    assert "표본 수" in md and "42" in md
    assert "회귀 없음" in md
    assert "방향성 참고용" in md
