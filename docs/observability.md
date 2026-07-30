# 관측성

무엇이 보이고, 무엇이 울리는가. 측정값의 원천은 두 가지입니다 — Consumer가 OpenSearch에 남기는 **메트릭 문서**(`weather-metrics-*`)와 **구조화 로그**(`LOG_FORMAT=json`).

## 메트릭 문서 (`weather-metrics-*`, 메시지당 1건)

| 필드 | 의미 |
| --- | --- |
| `event_id` | 상관관계 ID — 발행·처리·이력·로그를 한 ID로 잇는다 |
| `e2e_latency_seconds` | 발행 타임스탬프 → 처리 완료 |
| `process_duration_ms` | 판정 + 저장 + 발송 소요 |
| `external_attempted` / `delivered_channels` | 발송 시도 여부와 실제 전달된 채널 |
| `delivery_failed` | **시도했는데 전 채널 실패** — 억제(suppressed)와 구분된다 |
| `suppressed` | 쿨다운·근거 부족으로 발송 생략 |
| `missing_indices` / `missing_count` | 결측('정보없음')이었던 지수 |
| `alert_severity` | LOW / MEDIUM / HIGH / CRITICAL |
| `opensearch_indexed` | 이력 색인 성공 여부 |

보존 90일(ISM), 월 단위 인덱스, 색인은 best-effort — 메트릭 실패가 알림 처리를 막지 않습니다.

## 구조화 로그

- Consumer는 `LOG_FORMAT=json`으로 한 줄 JSON을 남깁니다. 메시지 처리 중의 모든 로그에 `event_id`가 자동으로 붙습니다(contextvar).
- Producer(Airflow 태스크 로그)는 API 호출마다 `api_call api=<이름> status=<코드> duration_ms=<n>`을 남깁니다. 수집기 없이 grep으로 API별 응답시간·실패율을 셀 수 있습니다.

```bash
# 채널별 전달 실패 최근 10건
curl -s "localhost:9200/weather-metrics-*/_search?q=delivery_failed:true&size=10&sort=timestamp:desc"

# API별 호출 소요 (Airflow 로그에서)
docker compose exec airflow sh -c "grep -h api_call /opt/airflow/logs -r" | tail -20
```

## 알람 기준

판정 쿼리는 전부 위 메트릭 문서로 계산됩니다. 알림 발송 도구(예: OpenSearch Dashboards 알림, cron+curl)는 #24에서 결정합니다 — 여기는 **기준의 단일 출처**입니다.

| # | 조건 | 판정 | 의미 |
| --- | --- | --- | --- |
| a | 신규 이력 없음 | `weather-alert-*`에 최근 7시간 문서 0건 | 파이프라인 정지 (6시간 주기 + 1시간 여유) |
| b | 전달 실패 | `delivery_failed:true` 발생 | 채널 인증 만료 등 — [트러블슈팅 3편](troubleshooting.md) 참고 |
| c | 결측 발생 | `missing_count > 0` | API 부분 장애. `missing_indices`로 어느 API인지 특정 |
| d | 컨슈머 랙 | `kafka-consumer-groups.sh --describe` LAG > 4 | 소비가 발행을 못 따라감 |
| e | DAG 실패 | Airflow 실패 콜백(Slack) — 이미 #35에서 배선 | 수집·발행 실패 |
| f | 디스크 | `docker system df` 볼륨 사용량 | 로컬 환경 수동 점검 |
| g | 클러스터 red | `_cluster/health` status | 데이터 인덱스는 replica 0이라 red = 실제 장애 |
| h | 카카오 토큰 만료 임박 | Consumer 경고 로그(`만료 임박`) — #50에서 배선 | 사전 재발급 |

## 실측 수치 (2026-07-30, 로컬 Docker)

| 항목 | 값 |
| --- | --- |
| end-to-end 지연 (발행→처리 완료) | **16.3초** — 폴링 간격(10초)이 지배. 판정 자체가 아니라 대기 시간 |
| 메시지 처리 소요 | **100.6 ms** (판정 + 이력 색인 + 발송 시도) |
| API 호출 소요 | Airflow 로그의 `api_call duration_ms` — 공공 API별 수백 ms~수 초 |
| 처리량 상한 | 초당 ~1건 (10초 폴링 × 배치 10) — 하루 4건 워크로드 기준 충분 |

같은 실측에서 알람 (b)·(c)의 판정 필드도 실제 상황으로 확인됐습니다 — 만료된 카카오 토큰으로 `delivery_failed: true`, `yellow_dust: null` 입력으로 `missing_indices: ["dust"]`.

수치는 메트릭 문서에서 직접 다시 잴 수 있습니다:

```bash
curl -s "localhost:9200/weather-metrics-*/_search" -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "e2e": {"stats": {"field": "e2e_latency_seconds"}},
    "duration": {"stats": {"field": "process_duration_ms"}}
  }
}'
```
