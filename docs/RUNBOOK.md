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

카카오는 refresh token의 잔여 유효기간이 짧아지면 갱신 응답에 새 토큰을 함께 줍니다. Consumer는 이 회전 값을 `KAKAO_TOKEN_STATE_PATH`(기본 `./.kakao_token.json`, 권한 600)에 저장하고 다음 기동 시 환경변수보다 우선해 읽습니다. 저장하지 않으면 기존 토큰 만료 시점에 카카오 알림이 영구 중단됩니다. 컨테이너를 재생성해도 유지하려면 이 경로를 볼륨에 올려야 합니다.

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

### Airflow 메타DB

메타DB(SQLite)는 홈 볼륨 `airflow_home`에 있어 컨테이너를 재생성해도 DAG on/off 상태와 실행 이력이 유지됩니다. 초기화하려면 `docker compose down -v`로 볼륨까지 지웁니다.

이 규모(하루 4회, 선형 4태스크)에서는 SQLite + SequentialExecutor로 충분합니다. DAG 수가 늘거나 병렬 실행이 필요해지면 PostgreSQL + LocalExecutor로 전환합니다 — compose에 postgres 서비스를 추가하고 `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`을 교체하면 됩니다.

### Airflow LocalExecutor/SQLite 오류

로컬 compose는 SQLite와 호환되는 `SequentialExecutor`를 사용합니다. `LocalExecutor`로 바꾸려면 Airflow 메타DB를 PostgreSQL 등으로 교체해야 합니다.

