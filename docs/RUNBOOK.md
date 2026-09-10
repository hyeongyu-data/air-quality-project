# 실행 가이드 (Runbook)

로컬에서 스택을 띄우고, 검증하고, 문제를 해결하는 절차입니다. 프로젝트 소개와 설계는 [README](../README.md)를 보세요.

## 환경변수

루트에 `.env` 파일을 두고 아래 값을 채웁니다. 실제 키/토큰은 README에 기록하지 않습니다.

```bash
# 공공데이터 API
WEATHER_API_KEY=your_kma_service_key
AIRKOREA_API_KEY=your_airkorea_service_key

# Kafka / OpenSearch
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
OPENSEARCH_HOST=opensearch
OPENSEARCH_PORT=9200
OPENSEARCH_INDEX_PREFIX=weather-alert

# 이메일 알림
EMAIL_ENABLED=true
ALERT_EMAIL=receiver@example.com
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your_gmail_address@gmail.com
SMTP_PASSWORD=your_gmail_app_password
SMTP_FROM_EMAIL=your_gmail_address@gmail.com
SMTP_USE_TLS=true
SMTP_USE_SSL=false

# 카카오톡 나에게 보내기
KAKAO_ENABLED=true
KAKAO_REST_API_KEY=your_kakao_rest_api_key
KAKAO_CLIENT_SECRET=your_kakao_client_secret
KAKAO_REFRESH_TOKEN=your_kakao_refresh_token
KAKAO_REDIRECT_URI=http://localhost:8088/kakao/callback
# 회전된 refresh token을 저장할 경로 (기본 ./.kakao_token.json)
KAKAO_TOKEN_STATE_PATH=./.kakao_token.json

# Slack은 선택
SLACK_ENABLED=false
SLACK_WEBHOOK_URL=
```

Gmail은 계정 비밀번호가 아니라 2단계 인증 후 발급한 앱 비밀번호를 `SMTP_PASSWORD`에 넣어야 합니다.

## 실행

```bash
docker compose up -d --build
docker compose ps
```

`--build`는 `consumer`(`Dockerfile`)와 `airflow`(`Dockerfile.airflow`) 이미지를
빌드합니다. Airflow는 런타임 pip 설치가 없고 의존성이 이미지에 굳어 있습니다
([ADR-0008](adr/0008-airflow-custom-image.md)). 이미지 재빌드가 필요한 때:
`requirements-airflow.txt` / `Dockerfile.airflow` / `requirements-consumer.txt` /
`Dockerfile` 변경 시 — `docker compose build airflow` 또는 `up -d --build`.

접속 주소:

| 서비스 | 주소 | 로그인 |
| --- | --- | --- |
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Kafka UI | http://localhost:8081 | 없음 |
| OpenSearch Dashboards | http://localhost:5601 (`--profile ops`) | 없음 |
| OpenSearch | http://localhost:9200 | 없음 |

Airflow 계정은 컨테이너 시작 시 자동 생성되며, 이미 존재하면 비밀번호를 `airflow`로 재설정합니다.

## DAG 실행

Airflow UI에서 `realtime_weather_alert` DAG를 켜면 KST 00·06·12·18시에 실행됩니다. 00시는 오늘 전체 예보, 06시는 아침 요약, 12·18시는 현재 기준 알림입니다.

이 네 시각은 `producer.collect_scheduled_weather()`가 분기를 정의한 시각과 같습니다. 수동 트리거는 아무 때나 가능하며, 그 경우 현재 기준 알림으로 처리됩니다.

수동 실행:

```bash
docker compose exec airflow airflow dags trigger realtime_weather_alert
```

로그 확인:

```bash
docker compose logs -f airflow
docker compose logs -f consumer
docker compose logs -f kafka
```

## 카카오 refresh token 발급

Kakao Developers 콘솔에서 Redirect URI를 먼저 등록합니다.

```text
http://localhost:8088/kakao/callback
```

그 다음 `.env`에 `KAKAO_REST_API_KEY`와 필요 시 `KAKAO_CLIENT_SECRET`을 넣고 로컬에서 실행합니다.

```bash
python3 scripts/kakao_get_refresh_token.py
```

카카오는 refresh token의 잔여 유효기간이 짧아지면 갱신 응답에 새 토큰을 함께 줍니다. Consumer는 이 회전 값을 `KAKAO_TOKEN_STATE_PATH`(Compose 기본 `/app/state/kakao_token.json`, 권한 600)에 원자적으로 저장하고 다음 기동 시 환경변수보다 우선해 읽습니다. 저장하지 않으면 기존 토큰 만료 시점에 카카오 알림이 영구 중단됩니다. 기본 Compose의 `consumer_state` 명명 볼륨이 `/app/state`를 보존하므로 컨테이너를 재생성해도 토큰과 쿨다운 캐시가 유지됩니다.

브라우저에서 카카오 로그인/동의를 마치면 터미널에 `KAKAO_REFRESH_TOKEN`이 출력됩니다. 이 값을 `.env`에 저장하면 Consumer가 Kafka 메시지를 처리할 때마다 refresh token으로 access token을 새로 받아 카카오톡 나에게 보내기를 수행합니다.

## 검증 명령

문법 확인:

```bash
python3 -m compileall producer consumer dags scripts
```

단위 테스트(169개 — 판정 로직·발송 게이트·API 파싱 계약·수명 관리):

```bash
pip install pytest pytest-cov requests python-dotenv kafka-python opensearch-py
pytest -q --cov=consumer --cov=producer
```

공공 API 응답 형태는 `tests/fixtures/`의 픽스처로 고정돼 있어, 포털이 스키마를 바꾸면 운영 결측 경보 전에 CI가 먼저 깨집니다. 커버리지 바닥(50%)은 목표가 아니라 후퇴 방지선입니다.

`master`로 향하는 PR·push는 GitHub Actions(`.github/workflows/ci.yml`)가 위 compile + pytest를 자동 실행하며, 통과가 머지 필수 조건이다.

컨테이너 안에서 DAG import 확인:

```bash
docker compose exec airflow python -m py_compile /opt/airflow/dags/air_pipeline.py
```

06시 알림 데이터 생성 확인:

```bash
docker compose exec airflow python -c "from producer.producer import WeatherDataCollector; c=WeatherDataCollector(); print(c.collect_scheduled_weather('서울', run_hour=6)); c.close()"
```

Kafka 토픽 확인:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

OpenSearch 인덱스 확인:

```bash
curl http://localhost:9200/_cat/indices?v
```

## 문제 해결

빠른 해결법은 아래 항목을, 문제를 어떻게 찾고 고쳤는지의 전체 과정(가설 → 검증 → 원인 → 재발 방지)은 [트러블슈팅 기록](docs/troubleshooting.md)을 보세요.

### Airflow 로그인이 안 될 때

현재 기본 계정은 `airflow` / `airflow`입니다. 그래도 안 되면 계정을 직접 재설정합니다.

```bash
docker compose exec airflow airflow users reset-password --username airflow --password airflow
```

### Gmail SMTP 535 오류

`SMTP_USERNAME`은 전체 Gmail 주소여야 하고, `SMTP_PASSWORD`는 일반 비밀번호가 아니라 Gmail 앱 비밀번호여야 합니다. Gmail 계정의 2단계 인증을 켠 뒤 앱 비밀번호를 새로 발급해 넣습니다.

### 카카오톡 발송 실패

`KAKAO_REFRESH_TOKEN`이 없거나 만료되면 `scripts/kakao_get_refresh_token.py`를 다시 실행합니다. Kakao Developers 콘솔의 Redirect URI가 `.env`의 `KAKAO_REDIRECT_URI`와 정확히 같아야 합니다.

### 자외선지수가 포털 검색값과 다를 때

이 프로젝트는 기상청 UV API의 시간별 값 중 현재 시각 이하의 가장 최근 발표값을 사용합니다. 포털은 관측소, 발표 지연, 보정 모델이 다를 수 있어 일시적으로 차이가 날 수 있습니다.

### 에어코리아 황사 권한 오류

황사 발생정보 API 활용 신청 권한이 없으면 황사는 `정보없음`으로 표시되고 행동 권고를 활성화하지 않습니다. 실제 황사 정보가 필요하면 공공데이터포털에서 해당 에어코리아 API 활용 권한을 신청해야 합니다.

예전에는 권한이 없을 때 PM10 평균을 황사 대체값으로 썼습니다. 그런데 황사 판정의 "좋음" 임계가 150㎍/㎥라 서울 PM10 평균으로는 사실상 항상 "좋음"이 나왔습니다. **감시되는 것처럼 보이지만 실제로는 아무것도 감시하지 않는 지표**였기 때문에 제거했습니다. 모른다를 괜찮다로 바꾸지 않는다는 원칙은 다른 지수와 같습니다.

### DLQ 메시지와 Consumer 오프셋 복구

깨진 JSON, 지원하지 않는 스키마, 필수 키 누락 또는 처리 예외는
`seoul-weather-dlq`(환경변수 `KAFKA_DLQ_TOPIC`으로 변경 가능)로 격리됩니다.
DLQ 발행까지 실패하면 해당 파티션의 읽기 위치를 실패 오프셋으로 되돌리고
후속 레코드를 처리하지 않습니다. 다른 파티션은 계속 처리하며, 파티션별
연속 완료 위치만 커밋합니다. Consumer 재시작 시 브로커에 저장된 마지막
커밋 위치부터 다시 읽습니다.

```bash
# DLQ 원인·원본 파티션·오프셋 확인
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${KAFKA_DLQ_TOPIC:-seoul-weather-dlq}" \
  --from-beginning

# 컨슈머 그룹의 파티션별 현재/끝 오프셋 확인
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group weather-alert-group --describe
```

원인을 수정한 뒤에는 DLQ의 `raw` payload를 검토하고, 운영 토픽에 직접
재발행하기 전에 테스트 토픽에서 `event_id`와 외부 발송 결과를 확인합니다.
현재 자동 DLQ 재처리 명령은 제공하지 않으며, 재처리 시 중복 외부 발송
가능성을 검토해야 합니다. DLQ 발행 실패·커밋 실패·브로커 재시작 복구는
실제 Kafka 통합 테스트가 필요합니다.

### Airflow 메타DB (PostgreSQL, #119)

메타DB는 `postgres:16-alpine` 서비스의 전용 볼륨 `airflow_pg_data`에 있습니다.
컨테이너(airflow·postgres)를 재생성해도 DAG on/off 상태와 실행 이력이 유지되며,
executor는 `LocalExecutor`(태스크를 별도 프로세스로 병렬 실행)입니다. 전체
초기화는 `docker compose down -v`. 설계 근거는 [ADR-0007](adr/0007-airflow-postgres-localexecutor.md).

`airflow_home` 볼륨은 로그·생성된 `airflow.cfg`·`webserver_config.py`·
`standalone_admin_password.txt`를 보존합니다(메타DB 아님).

**SQLite → Postgres 전환 시**: 기존 `airflow_home`의 `airflow.db`는 버려집니다
(Postgres에 새 메타DB). DAG는 자동 재등록되지만 실행 이력은 유실됩니다. 전환 전
`airflow_home/airflow.db`를 복사해 두세요.

### 메타DB 백업·복구

```bash
# 백업 (cron 권장, 예: 매일 1회)
scripts/airflow_db_backup.sh                        # airflow-db-YYYYMMDD-HHMMSS.sql.gz

# 복구 — airflow를 먼저 멈춘다 (--clean이 살아 있는 테이블을 DROP)
docker compose stop airflow
gunzip -c airflow-db-XXXX.sql.gz | docker compose exec -T postgres psql -U airflow -d airflow
docker compose start airflow
```

복구 리허설(검증됨): 백업 → `stop airflow` → `airflow_pg_data` 볼륨 삭제 →
`up postgres` → 복구 → `dag_run` 행 수·DAG on/off 확인 → `start airflow`.

### Airflow가 postgres에 못 붙을 때

`depends_on: postgres condition: service_healthy`로 순서를 보장합니다. 그래도
`FATAL: password authentication failed`가 나면: 운영 프로필은 `secrets/postgres_password`와
`secrets/airflow_db_conn`이 **같은 비밀번호**여야 합니다(secrets/README.md 스니펫).
기존 `airflow_pg_data` 볼륨이 다른 비밀번호로 초기화돼 있으면 `down -v` 후 재생성.

### 디스크·OpenSearch 상태 점검

운영 점검 또는 cron에서 아래 명령을 실행합니다. 기본 디스크 경고 기준은 80%이며
`DISK_USAGE_THRESHOLD`로 조정할 수 있습니다. 점검 스크립트는 인증정보를 인자나
출력으로 받지 않으며, 인증이 필요한 운영 OpenSearch는 `OPENSEARCH_HEALTH_URL`을
인증 프록시 주소로 제공합니다.

```bash
DISK_USAGE_THRESHOLD=80 ./scripts/check_storage_health.sh
```

종료 코드가 0이 아니면 디스크 사용률이 기준 이상이거나 OpenSearch가 yellow에
도달하지 못한 상태입니다. 먼저 `docker system df -v`와 `docker compose ps`를
확인하고, OpenSearch red인 경우 인덱스·노드 상태를 확인한 뒤 데이터 삭제나
볼륨 초기화는 승인 없이 수행하지 않습니다.

### 자동 알람 (#121)

관측 알람 기준(a·b·c·d·g)을 `consumer/alert_watch.py`가, f·g의 디스크 부분을
`check_storage_health.sh`가 담당합니다. 기준의 단일 출처와 환경변수 표는
[observability.md "자동 발송"](observability.md)에 있습니다.

**cron** (호스트, 저장소 루트). 전체 예시·설명은 [observability.md "자동 발송"](observability.md).

```cron
COMPOSE_FILE=docker-compose.yaml:docker-compose.prod.yaml
KAFKA_UI_PASSWORD=...
*/15 * * * *  cd /path/to/repo && flock -n /tmp/aq-alert.lock docker compose run --rm --no-deps -T consumer python consumer/alert_watch.py >> /var/log/aq-alert.log 2>&1
*/15 * * * *  cd /path/to/repo && OPENSEARCH_HEALTH_URL='https://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s' scripts/check_storage_health.sh >> /var/log/aq-storage.log 2>&1
```

- **운영은 `COMPOSE_FILE`로 오버레이를 함께 걸어야** 합니다 — 안 걸면 base로 붙어
  (평문 opensearch, `.env`) `collect()`가 실패하고 `.env.prod`의
  `ALERT_WATCH_SLACK_ENABLED=true`도 안 읽혀 dry-run에 머뭅니다.
- `SLACK_WEBHOOK_URL`은 `secrets/slack_webhook_url`(오버레이가 `_FILE`로 주입).
- `run --rm`이지 `exec`가 아닙니다 — Consumer가 죽어 있어도(그 자체가 알람) 돌아야 합니다.
  SIGKILL 시 멈춘 컨테이너가 남을 수 있으니 가끔 `docker compose rm -f`로 정리합니다.

**알람 수신 시**:

| 알람 | 먼저 확인 |
| --- | --- |
| a 신규 이력 없음 | `docker compose ps`, Airflow UI에서 DAG 실행 상태, Kafka 토픽 오프셋 |
| b 전달 실패 | [알림 채널 인증 만료](#알림-채널-인증-만료) — 어느 채널인지 consumer 로그 |
| c 결측 발생 | `missing_indices`로 어느 공공 API인지 → [공공 API 장애·결측](#공공-api-장애결측) |
| d 컨슈머 랙 | consumer 로그(예외·재시작 루프), OpenSearch 연결 상태 |
| f 디스크 | `docker system df -v`, 오래된 인덱스 ISM 확인 |
| g 클러스터 red | [OpenSearch red](#opensearch-red-또는-kafka-백로그dlq) |

## 장애 대응 절차

장애 대응 중에는 `.env`, 토큰, API 키, 비밀번호를 로그·Issue·PR에 복사하지 않습니다. 아래 명령은 상태 확인용이며, 볼륨 삭제·토픽 삭제·운영 토픽 재발행은 승인 없이 실행하지 않습니다.

### 공공 API 장애·결측

1. Airflow UI 또는 로그에서 실패한 DAG와 `api_call` 이벤트를 확인합니다.
2. `docker compose logs --since 30m airflow`로 HTTP 상태·타임아웃·응답시간을 확인합니다.
3. `weather-metrics-*`에서 `missing_count`와 `missing_indices`를 확인합니다.
4. API 복구 후 실패한 DAG 실행만 재시도하고 결측 메시지를 운영 토픽에 직접 재발행하지 않습니다.

### 알림 채널 인증 만료

1. `weather-metrics-*`에서 `delivery_failed:true`와 `delivered_channels`를 확인합니다.
2. 카카오는 `scripts/kakao_get_refresh_token.py`로 새 토큰을 발급하고 안전한 환경 주입 경로를 갱신합니다.
3. SMTP 앱 비밀번호와 Slack Webhook을 교체한 뒤 Consumer를 재시작하고 테스트 메시지로 전달 결과를 확인합니다.

### OpenSearch red 또는 Kafka 백로그·DLQ

1. OpenSearch는 `curl -sS 'http://localhost:9200/_cluster/health?pretty'`와 `docker compose logs --since 30m opensearch`를 확인합니다.
2. Kafka는 `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group weather-alert-group --describe`로 LAG를 확인합니다.
3. DLQ의 원인·source_partition·source_offset·raw payload를 확인합니다.
4. 원인 수정 전 운영 토픽 재발행·인덱스 삭제·볼륨 초기화를 실행하지 않습니다. 재처리하면 외부 알림이 중복될 수 있습니다.

### 복구 후 공통 검증

- `docker compose ps`의 healthy 상태, Consumer heartbeat, 최근 `weather-metrics-*` 문서를 확인합니다.
- 전달 실패·결측·DLQ·LAG가 기준 이하인지 확인하고 수행 명령과 영향 범위를 운영 기록에 남깁니다.
