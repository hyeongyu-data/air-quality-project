"""성능 수치를 한 번에 수집해 리포트로 남긴다 (#122).

Consumer 이미지에 포함되고, alert_watch(#121)의 OpenSearch 연결 헬퍼를 재사용한다.
컨테이너 안에서 도므로 opensearchpy·kafka-python 만 쓴다 — docker·git 은 없다.

호스트 cron:

    COMPOSE_FILE=docker-compose.yaml:docker-compose.prod.yaml \\
      docker compose run --rm --no-deps -T -e GIT_SHA=$(git rev-parse --short HEAD) \\
      consumer python consumer/measure_perf.py --since 7d --out /app/state/perf --compare
    docker compose cp consumer:/app/state/perf/. docs/perf/

리포트는 `<out>/<타임스탬프>.json`(--compare 의 소스)와 `.md`(사람용).
측정과 판정을 안 섞는다 — 회귀는 리포트에 표시만, exit code 는 항상 0(수집 실패만 2).
#121 알람과 분리.
"""
import argparse
import json
import logging
import os
import sys
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, List, Optional

try:
    from .alert_watch import _os_client
    from .timeutil import now_kst
except ImportError:  # 직접 실행 시
    from alert_watch import _os_client
    from timeutil import now_kst

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger("measure_perf")

KAFKA_GROUP = "weather-alert-group"
TOPIC = os.getenv("KAFKA_TOPIC", "seoul-weather")

REGRESS_P95_PCT = float(os.getenv("PERF_REGRESS_P95_PCT", "50"))
MIN_INDEX_RATE = float(os.getenv("PERF_MIN_INDEX_RATE", "0.98"))
MAX_LAG = int(os.getenv("PERF_MAX_LAG", "10"))


# ---------- 순수 함수 (테스트 대상) ----------

def build_metrics_query(since: str) -> Dict:
    return {
        "size": 0,
        "query": {"range": {"timestamp": {"gte": f"now-{since}"}}},
        "aggs": {
            "e2e": {"percentiles": {"field": "e2e_latency_seconds", "percents": [50, 95, 99]}},
            "proc": {"percentiles": {"field": "process_duration_ms", "percents": [50, 95, 99]}},
            "indexed": {"terms": {"field": "opensearch_indexed"}},
            "missing": {"range": {"field": "missing_count", "ranges": [{"from": 1}]}},
        },
    }


def _pct(agg: Dict) -> Dict:
    v = agg.get("values", {})

    def r(x):
        return round(x, 1) if isinstance(x, (int, float)) else None

    return {"p50": r(v.get("50.0")), "p95": r(v.get("95.0")), "p99": r(v.get("99.0"))}


def parse_metrics(resp: Dict, since_days: float) -> Dict:
    total = resp["hits"]["total"]
    n = total["value"] if isinstance(total, dict) else total
    aggs = resp.get("aggregations", {})
    buckets = {}
    for b in aggs.get("indexed", {}).get("buckets", []):
        key = b.get("key_as_string", str(b["key"])).lower()
        buckets[key] = b["doc_count"]
    indexed_true = buckets.get("true", 0)
    missing_n = sum(b["doc_count"] for b in aggs.get("missing", {}).get("buckets", []))
    return {
        "n": n,
        "throughput_per_day": round(n / since_days, 1) if since_days else None,
        "e2e_latency_seconds": _pct(aggs.get("e2e", {})),
        "process_duration_ms": _pct(aggs.get("proc", {})),
        "index_success_rate": round(indexed_true / n, 4) if n else None,
        "missing_rate": round(missing_n / n, 4) if n else None,
    }


def parse_kafka_lag(committed: Dict, ends: Dict) -> Dict[int, int]:
    """{partition: committed_offset} + {partition: end_offset} → {partition: lag}."""
    return {p: max(0, ends[p] - committed[p]) for p in committed if p in ends}


def compare(prev: Optional[Dict], curr: Dict) -> List[str]:
    if not prev:
        return ["첫 회차 — 비교 대상 없음"]
    out: List[str] = []
    for key in ("e2e_latency_seconds", "process_duration_ms"):
        p = prev.get("metrics", {}).get(key, {}).get("p95")
        c = curr.get("metrics", {}).get(key, {}).get("p95")
        if p and c and p > 0 and (c - p) / p * 100 >= REGRESS_P95_PCT:
            out.append(f"⚠️ {key} p95 {p} → {c} (+{(c - p) / p * 100:.0f}%)")
    rate = curr.get("metrics", {}).get("index_success_rate")
    if rate is not None and rate < MIN_INDEX_RATE:
        out.append(f"⚠️ 색인 성공률 {rate} < {MIN_INDEX_RATE}")
    max_lag = max(curr.get("kafka_lag", {}).values(), default=0)
    if max_lag > MAX_LAG:
        out.append(f"⚠️ Kafka max lag {max_lag} > {MAX_LAG}")
    return out or ["회귀 없음"]


def _v(x) -> str:
    return "—" if x is None else str(x)


def render_md(data: Dict) -> str:
    m = data["metrics"]
    meta = data["meta"]
    e2e, proc = m["e2e_latency_seconds"], m["process_duration_ms"]
    lag = data.get("kafka_lag") or {}
    synthetic = meta["n_note"].startswith("synthetic_load")
    # 합성 부하 버스트를 하루로 환산한 처리량은 의미가 없다.
    tput = "— (합성 부하 — 무의미)" if synthetic else f"{_v(m['throughput_per_day'])} 건/일"
    lines = [
        f"# 성능 측정 {meta['measured_at']}",
        "",
        f"- period: now-{meta['since']}  ({meta['n_note']})",
        f"- consumer: {meta['consumer']}",
        f"- git: {meta['git']}",
        "",
        "| 지표 | 값 |",
        "| --- | --- |",
        f"| 표본 수 (weather-metrics 문서) | {m['n']} |",
        f"| 처리량 | {tput} |",
        f"| e2e 지연 p50 / p95 / p99 (초) | {_v(e2e['p50'])} / {_v(e2e['p95'])} / {_v(e2e['p99'])} |",
        f"| 처리 시간 p50 / p95 / p99 (ms) | {_v(proc['p50'])} / {_v(proc['p95'])} / {_v(proc['p99'])} |",
        f"| 색인 성공률 | {_v(m['index_success_rate'])} |",
        f"| 결측률 | {_v(m['missing_rate'])} |",
        f"| Kafka lag (파티션별) | {lag or '측정 불가'} |",
        "",
        "> 외부 API 응답시간은 Airflow 태스크 로그에 있다(컨테이너 stdout 아님) — "
        "`docker compose exec airflow sh -c \"grep -rh api_call /opt/airflow/logs\"` 로 수동 확인.",
        "",
        "## 회귀",
        "",
    ]
    lines += [f"- {r}" for r in data.get("regression", ["미실행"])]
    lines += ["", "> 실트래픽 퍼센타일은 표본 수(n)가 작으면 방향성 참고용이다. "
              "`--load` 로 잡은 값은 합성 부하 1회 버스트이며 실운영 수치와 구분해 읽는다. "
              "더미 시크릿 스택에서는 채널 미설정으로 색인·지연과 무관하게 delivery_failed 가 뜬다."]
    return "\n".join(lines) + "\n"


# ---------- I/O ----------

def _percentiles(values: List[float]) -> Dict:
    if not values:
        return {"p50": None, "p95": None, "n": 0}
    s = sorted(values)
    return {"p50": round(median(s), 1),
            "p95": round(s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))], 1),
            "n": len(s)}


def parse_api_durations(log_lines: Iterable[str]) -> Dict[str, Dict]:
    """`api_call api=X ... duration_ms=Z` 라인 집계. 수동으로 뽑은 로그를 넘길 때만."""
    import re
    pat = re.compile(r"api_call api=(\S+).*?duration_ms=([\d.]+)")
    by_api: Dict[str, List[float]] = {}
    for line in log_lines:
        m = pat.search(line)
        if m:
            by_api.setdefault(m.group(1), []).append(float(m.group(2)))
    return {api: _percentiles(v) for api, v in by_api.items()}


def _kafka_lag() -> Dict[int, int]:
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin = KafkaAdminClient(bootstrap_servers=servers)
    consumer = KafkaConsumer(bootstrap_servers=servers, group_id=None)
    try:
        raw = admin.list_group_offsets({KAFKA_GROUP: None}).get(KAFKA_GROUP, {})
        if not raw:
            return {}
        ends = consumer.end_offsets(list(raw.keys()))
        committed = {tp.partition: raw[tp].offset for tp in raw}
        end_by_part = {tp.partition: ends[tp] for tp in raw if tp in ends}
        return parse_kafka_lag(committed, end_by_part)
    finally:
        admin.close()
        consumer.close()


def _inject_load(n: int, wait: int) -> None:
    import time
    from kafka import KafkaProducer
    # 주의: 실제 토픽에 쓴다. 합성 메시지는 pm10 만 있어 전 지수가 결측이므로
    # alert_watch(#121)의 조건 c(결측)를 유발하고 weather-metrics-* 를 오염시킨다.
    # dev·격리 스택 전용.
    logger.warning("합성 부하 %d건을 실제 토픽 %s 에 주입 (결측 알람 유발) — dev 전용", n, TOPIC)
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaProducer(bootstrap_servers=servers)
    ts = now_kst().replace(microsecond=0).isoformat()
    for i in range(n):
        doc = {"schema_version": 1, "event_id": f"perf-load-{i}",
               "timestamp": ts, "region": "서울", "pm10": 20.0 + (i % 120)}
        producer.send(TOPIC, json.dumps(doc, ensure_ascii=False).encode("utf-8"))
    producer.flush()
    producer.close()
    time.sleep(wait)


def _norm_since(since: str) -> str:
    return f"{since}d" if since and since[-1].isdigit() else since


def _since_days(since: str) -> float:
    num = float(since[:-1])
    return {"d": num, "h": num / 24, "m": num / 1440, "w": num * 7}.get(since[-1], num)


def _latest_prev_json(out_dir: str) -> Optional[Dict]:
    p = Path(out_dir)
    files = sorted(p.glob("*.json")) if p.is_dir() else []
    return json.loads(files[-1].read_text(encoding="utf-8")) if files else None


def collect(args) -> Dict:
    if args.load:
        _inject_load(args.load, args.load_wait)

    since = _norm_since(args.since)   # "7" → "7d"
    client = _os_client()
    resp = client.search(index="weather-metrics-*", body=build_metrics_query(since))
    try:
        lag = _kafka_lag()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Kafka lag 수집 실패: %s", type(exc).__name__)
        lag = {}

    return {
        "meta": {
            "measured_at": now_kst().strftime("%Y-%m-%d %H:%M KST"),
            "since": since,
            "n_note": f"synthetic_load: {args.load}" if args.load else "real traffic",
            "consumer": "max_poll_records=10, poll 10s, LocalExecutor",
            "git": os.getenv("GIT_SHA", "unknown"),
        },
        "metrics": parse_metrics(resp, _since_days(since)),
        "kafka_lag": lag,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="성능 수치 측정·기록 (#122)")
    ap.add_argument("--since", default="24h")
    ap.add_argument("--out", default="/app/state/perf")
    ap.add_argument("--load", type=int, default=0)
    ap.add_argument("--load-wait", type=int, default=30)
    ap.add_argument("--compare", action="store_true")
    ap.add_argument("--stdout", action="store_true")
    args = ap.parse_args()

    try:
        data = collect(args)
    except Exception as exc:  # noqa: BLE001
        logger.error("수집 실패: %s: %s", type(exc).__name__, exc)
        return 2

    prev = _latest_prev_json(args.out) if args.compare else None
    data["regression"] = compare(prev, data) if args.compare else ["미실행 (--compare 아님)"]

    md = render_md(data)
    if not args.stdout:
        Path(args.out).mkdir(parents=True, exist_ok=True)
        stamp = now_kst().strftime("%Y%m%d-%H%M%S")
        (Path(args.out) / f"{stamp}.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        (Path(args.out) / f"{stamp}.md").write_text(md, encoding="utf-8")
        logger.info("리포트: %s/%s.{json,md}", args.out, stamp)
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
