# 성능 측정 기록 (#122)

`consumer/measure_perf.py` 가 성능 수치를 한 번에 수집해 이 디렉터리에
날짜별로 `<타임스탬프>.json`(기계용, `--compare` 소스)와 `.md`(사람용)를 남긴다.

## 측정 대상 (`measure_perf.py` 가 리포트에 채우는 것)

| 지표 | 소스 |
| --- | --- |
| 처리량 (건/일) | `weather-metrics-*` 문서 수 ÷ 창 일수 (합성 부하 리포트에선 무의미 표시) |
| e2e 지연 p50/p95/p99 | `e2e_latency_seconds` percentiles agg |
| 처리 시간 p50/p95/p99 | `process_duration_ms` percentiles agg (판정 + 색인 + 발송 시도 포함) |
| 색인 성공률 | `opensearch_indexed` true 비율 |
| 결측률 | `missing_count > 0` 문서 비율 |
| Kafka lag (파티션별) | `KafkaAdminClient.list_group_offsets` + `end_offsets` |

**리포트에 없는 것 (수동 확인):**

- **외부 API 응답시간** — `producer/api_common.py` 의 `api_call ... duration_ms` 는
  Airflow 태스크 로그 **파일**(`/opt/airflow/logs`)에 있고 컨테이너 stdout 이
  아니라, 컨테이너 안에서 도는 이 스크립트가 못 읽는다.
  `docker compose exec -T airflow sh -c "grep -rh api_call /opt/airflow/logs"` 로
  뽑아 필요하면 `measure_perf.parse_api_durations` 에 파이프한다.
- **`_nodes/stats` 색인 지연** — `weather_writer`(최소 권한, #118)가 못 읽고
  (`cluster:monitor/nodes/stats` 403), `process_duration_ms` 가 색인 시간을 포함한다.

## 실행

```bash
# 개발
docker compose run --rm --no-deps -T consumer python consumer/measure_perf.py --since 24h --stdout

# 운영 (주 1회 권장) — prod 오버레이 필수
COMPOSE_FILE=docker-compose.yaml:docker-compose.prod.yaml \
  docker compose run --rm --no-deps -T consumer \
  python consumer/measure_perf.py --since 7d --out /app/state/perf --compare
docker compose cp consumer:/app/state/perf/. docs/perf/   # 리포트 회수
git add docs/perf && git commit   # 사람이 검토 후 커밋 (cron 자동 커밋 안 함)
```

`--load N` 은 유효 스키마 메시지 N건을 **실제 토픽에** 주입하고 `--load-wait` 초 뒤
측정한다 — 초기 베이스라인용. **dev·격리 스택 전용**: 합성 메시지는 전 지수가
결측이라 `alert_watch`(#121) 조건 c(결측)를 유발하고 `weather-metrics-*` 를
오염시킨다. 리포트 헤더에 `synthetic_load: N` 이 붙고 처리량은 무의미 표시된다.

## 이 디렉터리

`README.md` 만 커밋돼 있고, 리포트는 첫 정기 실행부터 쌓인다. 리포트가 늘면
(주 1회 ≈ 연 100개) 최근 ~12개만 남기고 오래된 건 정리한다 — 추이는 `--compare`
델타로 충분하다.

## 회귀 기준 (`--compare`)

직전 `.json` 대비:

- `e2e_latency_seconds` 또는 `process_duration_ms` 의 **p95 가 +50% 이상** (`PERF_REGRESS_P95_PCT`)
- 색인 성공률 `< 0.98` (`PERF_MIN_INDEX_RATE`)
- Kafka max lag `> 10` (`PERF_MAX_LAG`)

→ 리포트 "회귀" 절에 `⚠️` 라인. 측정 스크립트 exit code 는 **항상 0**(수집 실패만 2) —
회귀 판정과 알람(#121)은 분리한다.

## 읽는 법

- **실트래픽 퍼센타일은 표본 수(`n`)가 작으면 방향성 참고용**이다. 하루 4건
  워크로드에서 7일치는 ~28건 — p95 를 정밀 수치로 신뢰하지 않는다.
- `--load` 로 잡은 값은 **합성 부하 1회 버스트**다. 실운영 수치와 구분해 읽는다.
- 더미 시크릿으로 띄운 스택에서는 모든 메시지가 `delivery_failed:true` 가 된다
  (채널 미설정) — 색인·지연 수치와는 무관하다.
