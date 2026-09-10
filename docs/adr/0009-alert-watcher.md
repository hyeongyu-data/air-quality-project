# ADR-0009. 관측 알람 자동 발송 — 경량 워처

- 상태: 채택 (2026-09-11)
- 관련 이슈: [#121](https://github.com/hyeongyu-data/air-quality-project/issues/121)
- 관련: [#35](../DEVELOPMENT_PLAN.md)(DAG 실패 콜백), [#50](../DEVELOPMENT_PLAN.md)(토큰 만료 로그), [#112](../DEVELOPMENT_PLAN.md)(스토리지 점검), [ADR-0006](0006-secret-management.md)

## 맥락

`docs/observability.md` 에 알람 기준 8종(a~h)이 판정 쿼리와 함께 있지만,
**자동 발송 배선이 없었다**. e(DAG 실패, Slack 콜백)·h(카카오 토큰, 경고 로그)만
배선. a·b·c·d·f·g 는 사람이 직접 확인.

## 결정

**경량 워처 `consumer/alert_watch.py` — Consumer 이미지에 포함, 호스트 cron
(`docker compose run --rm --no-deps`)으로 주기 실행.** f·g 의 디스크 부분은
`scripts/check_storage_health.sh`(#112)에 Slack POST 를 얹어 호스트에서.

### 왜 경량 워처 (OpenSearch Dashboards Alerting 아님)

- #118 에서 OSD 를 운영 프로필과 **비호환**으로 표시했다(OSD 는 http+보안 off 가정).
- OSD Alerting 은 saved-object 관리와 OSD 상시 가동 의존이 생긴다.
- 워처는 `read_secret`·TLS/CA·`weather_writer` 계정(#117·#118)을 그대로 재사용한다.
  새 의존성·새 서비스 없음.

### 왜 `run --rm`, `exec` 아님

Consumer 컨테이너가 죽어 있는 것 자체가 알람 조건(a: 이력 없음, d: 랙)이다.
`exec` 는 죽은 컨테이너에서 안 돌고, "컨테이너 다운(rc 1)" 과 "알람 발화(rc 1)"
를 exit code 로 못 가른다. `run --rm --no-deps` 는 같은 이미지·env·secret·
`consumer_state` 볼륨을 새 컨테이너에 그대로 준다. cron 에 `flock -n` 으로 겹침 방지.

### 왜 cron (compose 상시 서비스 아님)

Compose 에 "N분마다 실행" 모델이 없다. 상시 컨테이너 + `while sleep` 은 재시작·
로그·관측이 더 번거롭다. 호스트 cron 이 단순하고, 워처가 죽어도 다음 tick 에 복구.

### Kafka 랙 측정

`KafkaAdminClient.list_group_offsets({"weather-alert-group": None})`(committed) +
`KafkaConsumer(group_id=None).end_offsets(tps)`(end). 둘 다 read-only
(OffsetFetch/ListOffsets) — JoinGroup·리밸런스 없이 실제 컨슈머 그룹을 안 건드린다.

### 쿨다운

조건별 마지막 발송 시각을 `/app/state/alert_watch_cooldown.json`(consumer_state
볼륨, 원자적 교체)에. 같은 조건은 `COOLDOWN_MINUTES`(기본 60) 안에 재발송 안 함.
장애 지속 시 그 간격마다 다시 울린다 — 조용해지지 않는다.

### Slack 페이로드

`producer/masking.py` 는 Consumer 이미지에 없다. 그래서 명시적 필드 화이트리스트
(`event_id`·`region`·`missing_indices`·`missing_count`·수치·`cluster_status`)만
담고, 문서 `_source` 원문·자유 텍스트(`recommendations`)는 안 읽는다.
`requests.post` 예외는 `type(e).__name__` 만 로그(traceback 이 webhook URL 노출 가능).

## 대안과 기각 이유

| 대안 | 기각 이유 |
| --- | --- |
| OSD Alerting 플러그인 | #118 비호환, saved-object·OSD 상시 가동 의존 |
| Prometheus + Alertmanager | 인프라 규모. `observability.md` 에 설계만 |
| 별도 상시 `alert-watch` 컨테이너 | cron + `run --rm` 으로 충분. 재시작·로그 관리 부담만 늘어남 |
| Airflow 모니터링 DAG | Airflow 가 죽으면 모니터도 죽는다. 호스트 cron 이 독립적 |

## 재검토 조건

- 시계열 대시보드·다중 receiver·복잡한 라우팅이 필요해지면 → Prometheus 스택.
- Slack 외 채널(PagerDuty)·온콜 로테이션이 필요하면 → `notify()` 의 receiver
  추상화 + Events API v2.
- 알람이 잦아 쿨다운/에스컬레이션 튜닝이 필요하면 → 조건별 임계·"N회 후 에스컬레이션".
