"""관측 알람 기준(a·b·c·d·g)을 주기적으로 평가해 Slack으로 발송한다 (#121).

`docs/observability.md`의 알람 기준을 자동 발송으로 배선한다. e(DAG 실패, Slack
콜백 #35)·h(카카오 토큰 만료 임박, 경고 로그 #50)는 이미 배선돼 여기서 다루지
않는다. f(디스크)는 `scripts/check_storage_health.sh`(#112)가 호스트에서 잰다 —
컨테이너 안에서는 디스크 압박을 제대로 못 잰다.

실행 (호스트 cron):
    */15 * * * * cd /path/to/repo && flock -n /tmp/aq-alert.lock \
      docker compose run --rm --no-deps -T consumer python consumer/alert_watch.py \
      >> /var/log/aq-alert.log 2>&1

exit code: 0 이상 없음 / 1 하나 이상 발화 / 2 수집 실패.
"""
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

try:
    from .secretstore import read_secret
    from .timeutil import now_kst
except ImportError:  # 직접 실행 시
    from secretstore import read_secret
    from timeutil import now_kst

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
# kafka-python은 연결·메타데이터 갱신마다 INFO를 쏟아 cron 로그를 채운다.
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger("alert_watch")

STATE_PATH = Path(os.getenv("ALERT_WATCH_STATE_PATH", "/app/state/alert_watch_cooldown.json"))
KAFKA_GROUP = "weather-alert-group"

# Slack 메시지에 넣어도 되는 필드 — 이 목록에 없는 값은 절대 넣지 않는다.
# weather-metrics-* 문서 원문(_source)은 안 읽고, weather-alert-*는 count만 본다.
_ALLOWED_FIELDS = {"event_id", "region", "missing_indices", "missing_count", "cluster_status"}


class Config:
    def __init__(self) -> None:
        g = os.getenv
        self.slack_enabled = g("ALERT_WATCH_SLACK_ENABLED", "false").lower() == "true"
        self.lookback_minutes = int(g("ALERT_WATCH_LOOKBACK_MINUTES", "70"))
        self.cooldown_minutes = int(g("ALERT_WATCH_COOLDOWN_MINUTES", "60"))
        self.lag_threshold = int(g("ALERT_WATCH_LAG_THRESHOLD", "4"))
        self.no_history_hours = int(g("ALERT_WATCH_NO_HISTORY_HOURS", "7"))
        self.alert_prefix = g("OPENSEARCH_INDEX_PREFIX", "weather-alert")


class Snapshot:
    """수집 결과. evaluate()는 이것만 본다 — 테스트가 여기에 값을 채워 넣는다."""

    def __init__(
        self,
        alert_docs_recent: int,
        delivery_failed_count: int,
        missing_hits: List[Dict],
        kafka_max_lag: Optional[int],
        cluster_status: Optional[str],
    ) -> None:
        self.alert_docs_recent = alert_docs_recent
        self.delivery_failed_count = delivery_failed_count
        self.missing_hits = missing_hits
        self.kafka_max_lag = kafka_max_lag
        self.cluster_status = cluster_status


def evaluate(snap: Snapshot, cfg: Config) -> List[Dict]:
    """스냅샷 → 발화된 알람 목록 [{code, title, detail}]. 순수 함수."""
    alerts: List[Dict] = []

    if snap.alert_docs_recent == 0:
        alerts.append({
            "code": "a",
            "title": f"신규 이력 없음 ({cfg.no_history_hours}시간)",
            "detail": "weather-alert-* 에 최근 문서 0건 — 파이프라인 정지 의심",
        })

    if snap.delivery_failed_count > 0:
        alerts.append({
            "code": "b",
            "title": f"전달 실패 {snap.delivery_failed_count}건",
            "detail": "delivery_failed:true — 채널 인증 만료 등 (트러블슈팅 3편)",
        })

    if snap.missing_hits:
        idx = sorted({i for h in snap.missing_hits for i in h.get("missing_indices", [])})
        eids = [h.get("event_id") for h in snap.missing_hits[:5] if h.get("event_id")]
        alerts.append({
            "code": "c",
            "title": f"결측 발생 {len(snap.missing_hits)}건",
            "detail": f"missing_indices={idx} event_id={eids}",
        })

    if snap.kafka_max_lag is not None and snap.kafka_max_lag > cfg.lag_threshold:
        alerts.append({
            "code": "d",
            "title": f"컨슈머 랙 {snap.kafka_max_lag} (> {cfg.lag_threshold})",
            "detail": "소비가 발행을 못 따라감 — consumer 로그 확인",
        })

    if snap.cluster_status == "red":
        alerts.append({
            "code": "g",
            "title": "OpenSearch 클러스터 red",
            "detail": "데이터 인덱스는 replica 0 — red = 실제 장애",
        })

    return alerts


# ---------- 쿨다운 ----------

def _load_state() -> Dict[str, str]:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _save_state(state: Dict[str, str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{STATE_PATH.name}.", dir=STATE_PATH.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(state, ensure_ascii=False))
        os.chmod(tmp, 0o600)
        os.replace(tmp, STATE_PATH)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def filter_cooldown(alerts: List[Dict], state: Dict[str, str], cfg: Config, now=None) -> List[Dict]:
    """쿨다운 안에 이미 보낸 조건은 뺀다."""
    from datetime import timedelta
    current = now or now_kst()
    window = timedelta(minutes=cfg.cooldown_minutes)
    fresh = []
    for a in alerts:
        last = state.get(a["code"])
        if last:
            try:
                from datetime import datetime
                if current - datetime.fromisoformat(last) < window:
                    logger.info("쿨다운 억제: %s (마지막 %s)", a["code"], last)
                    continue
            except ValueError:
                pass
        fresh.append(a)
    return fresh


# ---------- 수집 (I/O) ----------

def _os_client():
    from opensearchpy import OpenSearch
    user = os.getenv("OPENSEARCH_USER")
    password = read_secret("OPENSEARCH_PASSWORD")
    return OpenSearch(
        hosts=[{"host": os.getenv("OPENSEARCH_HOST", "localhost"),
                "port": int(os.getenv("OPENSEARCH_PORT", "9200"))}],
        http_auth=(user, password) if user and password else None,
        use_ssl=os.getenv("OPENSEARCH_USE_SSL", "false").lower() == "true",
        verify_certs=os.getenv("OPENSEARCH_VERIFY_CERTS", "false").lower() == "true",
        ca_certs=os.getenv("OPENSEARCH_CA_CERTS") or None,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )


def _kafka_max_lag() -> Optional[int]:
    """weather-alert-group 의 파티션별 (end - committed) 최댓값. read-only."""
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin = KafkaAdminClient(bootstrap_servers=servers)
    consumer = KafkaConsumer(bootstrap_servers=servers, group_id=None)
    try:
        committed = admin.list_group_offsets({KAFKA_GROUP: None}).get(KAFKA_GROUP, {})
        if not committed:
            return None
        tps = list(committed.keys())
        ends = consumer.end_offsets(tps)
        return max(ends[tp] - committed[tp].offset for tp in tps)
    finally:
        admin.close()
        consumer.close()


def collect(cfg: Config) -> Snapshot:
    client = _os_client()
    since = f"now-{cfg.lookback_minutes}m"

    alert_recent = client.count(index=f"{cfg.alert_prefix}-*", body={
        "query": {"range": {"timestamp": {"gte": f"now-{cfg.no_history_hours}h"}}}
    })["count"]

    failed = client.count(index="weather-metrics-*", body={
        "query": {"bool": {"filter": [
            {"term": {"delivery_failed": True}},
            {"range": {"timestamp": {"gte": since}}},
        ]}}
    })["count"]

    missing = client.search(index="weather-metrics-*", body={
        "size": 20, "_source": ["event_id", "missing_indices", "missing_count"],
        "query": {"bool": {"filter": [
            {"range": {"missing_count": {"gt": 0}}},
            {"range": {"timestamp": {"gte": since}}},
        ]}},
    })
    missing_hits = [
        {k: v for k, v in h["_source"].items() if k in _ALLOWED_FIELDS}
        for h in missing["hits"]["hits"]
    ]

    status = client.cluster.health()["status"]

    try:
        lag = _kafka_max_lag()
    except Exception as exc:  # noqa: BLE001 — 수집 실패는 로그만, 나머지 조건은 계속
        logger.warning("Kafka lag 수집 실패: %s", type(exc).__name__)
        lag = None

    return Snapshot(alert_recent, failed, missing_hits, lag, status)


# ---------- Slack ----------

def _post_slack(webhook: str, text: str) -> bool:
    import requests
    try:
        r = requests.post(webhook, json={"text": text}, timeout=10)
        return r.status_code == 200
    except Exception as exc:  # noqa: BLE001 — traceback 이 webhook URL 을 노출할 수 있다
        logger.error("Slack 발송 실패: %s", type(exc).__name__)
        return False


def notify(alerts: List[Dict], cfg: Config) -> None:
    lines = ["*[관측 알람]* " + now_kst().strftime("%Y-%m-%d %H:%M KST")]
    for a in alerts:
        lines.append(f"• ({a['code']}) {a['title']} — {a['detail']}")
    text = "\n".join(lines)

    webhook = read_secret("SLACK_WEBHOOK_URL")
    if not cfg.slack_enabled or not webhook:
        logger.info("[dry-run] %s", text.replace("\n", " | "))
        return
    if _post_slack(webhook, text):
        logger.info("Slack 발송 성공 (%d건)", len(alerts))


def main() -> int:
    cfg = Config()
    try:
        snap = collect(cfg)
    except Exception as exc:  # noqa: BLE001
        logger.error("수집 실패: %s: %s", type(exc).__name__, exc)
        return 2

    alerts = evaluate(snap, cfg)
    if not alerts:
        logger.info("이상 없음")
        return 0

    state = _load_state()
    fresh = filter_cooldown(alerts, state, cfg)
    if fresh:
        notify(fresh, cfg)
        now_iso = now_kst().isoformat()
        for a in fresh:
            state[a["code"]] = now_iso
        _save_state(state)
    else:
        logger.info("발화했으나 전부 쿨다운 중: %s", [a["code"] for a in alerts])
    return 1


if __name__ == "__main__":
    sys.exit(main())
