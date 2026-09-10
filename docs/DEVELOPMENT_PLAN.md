# 개발 계획 및 작업 기록

## 운영 절차

모든 작업은 계획 작성·승인 → GitHub Issue → 이슈 연결 브랜치 → 코드와 관련 Markdown 수정 → 검증 → Draft PR → 셀프/에이전트 리뷰 → 승인 → Squash merge 순서로 진행한다. 원격 Issue·브랜치·push·PR·merge는 사용자 승인 없이 실행하지 않는다. 시크릿은 코드·로그·문서·테스트에 넣지 않는다.

## P0-2 승인 계획: 토큰과 쿨다운 상태의 컨테이너 영속화

- 문제: Consumer의 카카오 refresh token과 쿨다운 캐시가 컨테이너 재생성 후 사라질 수 있었다.
- 구현: `/app/state` 명명 볼륨, 명시적 경로 환경변수, 원자적 JSON 저장, 파일 권한 600.
- 범위 외: AWS Secrets Manager, 외부 채널 재시도, 운영 자격증명 변경.
- 완료 기준: 재생성 보존 경로 문서화, OpenSearch 장애 fallback 유지, 손상·쓰기 실패 테스트, 시크릿 검사, 테스트·린트·Compose 검증.

## 2026-09-10 작업 기록

- Issue: `#102 feat: 토큰과 쿨다운 상태의 컨테이너 영속화`
- 브랜치: `feat/102-token-cooldown-persistence`
- 변경: `consumer/alert.py`, `docker-compose.yaml`, `.env.example`, `tests/test_kakao_token.py`, `README.md`, `docs/RUNBOOK.md`, `docs/operational-risks.md`
- 검증: pytest 196개, 커버리지 56.63%, ruff `E9,F`, compileall, `docker compose config -q`, `git diff --check` 통과.
- 보류: 실제 컨테이너 재생성 검증은 Docker 엔진 오류로 미실행. PR에 제한 사항으로 명시한다.
- 다음 게이트: 시크릿·diff 자체 검토 → Conventional Commit → Draft PR 템플릿 작성. PR 생성 전 원격 작업 승인을 다시 확인한다.

## 다음 승인 요청: P0-1 Kafka 오프셋 복구의 정식 반영

P0-1의 애플리케이션 변경을 Issue #104와 `fix/104-kafka-offset-recovery` 브랜치에서 정식 반영한다.

- 문제: DLQ 발행 실패 시 poll 위치만 되돌리지 않으면 뒤 레코드가 커밋되어 실패 메시지가 유실될 수 있다.
- 제안: 파티션별 연속 완료 오프셋만 커밋하고, DLQ 실패 파티션은 실패 위치로 되감아 후속 레코드 처리를 중단한다. 커밋·seek 실패 시 연결을 폐기한다.
- 범위: `consumer/consumer.py`, `tests/test_message_schema.py`, `README.md`, `docs/RUNBOOK.md`, 이 개발 계획 문서.
- 비범위: 일시적 downstream 장애와 영구적인 poison message 분류, DLQ 자동 재처리 UI, 외부 알림 채널 재시도.
- 완료 기준:
  - [x] 파티션별 실패·후속 레코드·다중 파티션 시나리오 테스트
  - [x] 커밋 실패·seek 실패·Consumer 재시작 시나리오 테스트
  - [x] 실제 Kafka 통합 검증: malformed JSON 1건을 `seoul-weather`에 주입하고 DLQ 발행 성공과 격리를 확인
  - [x] DLQ 복구 절차와 중복 발송 한계 문서화
  - [x] pytest 201개, ruff, compileall, Compose 설정 검증 통과
  - [x] 시크릿 검사 및 Second Brain 기록 갱신

승인 후 생성할 이슈 제목은 `fix: DLQ 실패 파티션의 Kafka 오프셋 복구`이며, 기존 Bug Report 템플릿의 현상·기대 동작·재현 절차·환경·보안 확인 항목을 작성한다. 승인 전에는 stash 적용, 원격 Issue 생성, 브랜치·push·PR을 실행하지 않는다.

## 2026-09-10 P0-1 구현 기록

- Issue #104와 이슈 번호 브랜치를 사용했다. P0-1 변경은 P0-2와 별도 커밋·PR로 유지한다.
- 파티션별 완료 오프셋, DLQ 실패 위치 되감기, 실패 파티션 후속 처리 중단, 커밋·seek 실패 시 연결 폐기를 구현했다.
- 검증: pytest 201개 통과, 커버리지 57.41%, ruff `E9,F`, compileall, Compose 설정, diff check 통과.
- Docker가 복구되어 `airquality` 격리 Compose로 Kafka·OpenSearch·Airflow·Consumer를 기동했다. 외부 알림 없이 malformed JSON 1건을 `seoul-weather`에 주입했고 `seoul-weather-dlq`에서 원인·파티션·오프셋·raw payload를 확인했다.
- 리뷰 반영: 완료 오프셋을 rewind보다 먼저 커밋, DLQ Producer 멱등성 활성화, 배치 처리량 주석 정정, 순서·커밋 실패 회귀 테스트 추가(`7aa170b`).
- 검증 범위 제한: 실제 Kafka 통합 검증은 malformed JSON에서 DLQ 발행 성공까지다. DLQ 발행 실패, 다중 파티션 공백, Consumer 재시작 복구는 단위 모델 테스트 범위이며 실제 Kafka 환경에서 검증했다고 주장하지 않는다.
- 비차단 후속 과제: `commit()`의 모든 예외를 연결 초기화로 처리하는 동작은 단일 Consumer 운영 범위에서 허용한다. 확장 전에는 `CommitFailedError`와 일시적 타임아웃을 구분한다.
- 다음 게이트: CI 재실행과 리뷰 확인 후 Ready/merge 승인.

## P0-3 Airflow 메타DB 영속성 확인 결과

### 확인 결과

기존 PR #78에서 이미 `airflow_home:/opt/airflow` named volume으로 해결되어 있었다. `docker-compose.yaml`, `docs/RUNBOOK.md`, README와 Compose 설정을 재확인했고 `docker compose config -q`가 통과했다.

### 현재 구현

- `airflow_home` named volume이 `/opt/airflow` 전체를 보존한다.
- RUNBOOK에 재생성 보존과 `docker compose down -v` 초기화 절차가 문서화되어 있다.
- SQLite + SequentialExecutor 운영 한계와 PostgreSQL 전환 조건이 문서화되어 있다.

### 범위

- 대상: `docker-compose.yaml`, `.env.example`, `README.md`, `docs/RUNBOOK.md`, `docs/operational-risks.md`, 관련 테스트·개발 계획 문서.
- 비범위: PostgreSQL 전환, executor 교체, 외부 배포 환경 구성, Airflow 인증 체계 개편.

### 처리 결과

- [x] 기존 구현(PR #78)과 문서 확인
- [x] `docker compose config -q` 통과
- [x] 중복 Issue #106 종료
- [x] second-brain 기록 갱신

추가 코드 변경은 없으며, 실제 컨테이너 재생성 검증은 Docker 환경에서 별도 운영 검증 과제로 남긴다.

## 관측성 기반 최소 메트릭 확인 결과

### 확인 결과

기존 구현에서 Consumer 메트릭, 구조화 로그, event_id 상관관계와 관측성 문서가 이미 반영되어 있음을 확인했다. `consumer/metrics.py`, `consumer/logutil.py`, `tests/test_observability.py`, `docs/observability.md`가 해당 범위를 다룬다.

### 현재 제공 기능

- `weather-metrics-*` OpenSearch 문서에 처리 지연·전달 결과·결측·억제 상태를 기록한다.
- `LOG_FORMAT=json`과 `event_id` contextvar로 구조화 로그와 상관관계를 제공한다.
- 메트릭·로그의 민감정보 비노출 및 best-effort 색인 동작을 테스트한다.
- `docs/observability.md`에 필드, 알람 기준, 실측 수치를 문서화한다.

### 범위 외

Prometheus/Grafana 배포, 외부 알림 시스템 연동, 전체 파이프라인 분산 추적은 후속 작업으로 둔다.

### 처리 결과

- [x] 기존 구현과 관련 테스트·문서 확인
- [x] `docs/observability.md`와 README 연결 확인
- [x] 중복 Issue #108 종료
- [ ] Prometheus/Grafana 또는 외부 알람 연동은 후속 과제

## 남은 작업 기준 목록

완료된 P0 작업과 기존 구현 확인 항목을 제외한 후속 작업은 아래 순서로 진행한다.

1. 공공 API 응답 픽스처 기반 파싱 테스트를 CI에 추가한다.
2. 디스크 사용량·OpenSearch 상태 알람 기준과 실행 방법을 추가한다.
3. API 장애·채널 인증 만료·OpenSearch red·Kafka 백로그 대응 런북을 보강한다.
4. 처리량 상한과 p50/p95 지연을 측정하고 문서화한다.
5. Airflow SQLite/SequentialExecutor를 PostgreSQL/LocalExecutor로 전환할 계획을 수립한다.
6. 런타임 패키지 설치를 제거하고 이미지·의존성 버전을 고정한다.
7. Secret Manager, TLS, 관리 포트 제한, non-root 컨테이너를 적용한다.

각 항목은 독립 Issue와 이슈 번호 브랜치로 진행하며, 코드·Markdown 수정과 검증 후 PR로 병합한다.

### 1번 항목 확인 결과: 공공 API 픽스처 테스트

`tests/fixtures/`와 `tests/test_api_contract.py`에 KMA·AirKorea JSON/XML 픽스처, 성공 응답·실패 응답·필수 필드 검증이 이미 구현되어 있다. 중복 Issue #110은 종료하며 추가 코드는 작성하지 않는다.

## 운영 안정성·보안 이슈 (2026-09-11 발행)

위험도 순: `#117` 시크릿 이관 → `#118` OpenSearch·Kafka 인증·non-root → `#119`
Airflow PostgreSQL·LocalExecutor → `#120` 런타임 pip 제거 → `#121` 알람 자동 발송
→ `#122` 성능 정기 측정. 각 항목은 계획 작성 → 별도 세션 검증 → 구현·docker
검증 → PR → 별도 세션 리뷰 → 수정·머지 순서.

### #117 시크릿을 .env 평문에서 Docker secrets로 이관

- Issue: `#117` / 브랜치: `feat/117-secret-file-loader`
- 문제: `consumer`가 `env_file: .env`로 모든 시크릿을 컨테이너 env에 주입 →
  `docker inspect`·`/proc/1/environ` 평문 노출. `.env` 한 파일 유출 = 전체 노출.
- 구현:
  - `_FILE` 관례 로더(`read_secret`) — `FOO_FILE` 파일 우선, 없으면 `FOO` env.
    두 런타임이 코드를 공유 못 해 `consumer/secretstore.py`·`producer/secretstore.py`
    쌍둥이(12줄 순수 함수, `ponytail:` 교차 주석).
  - consumer 시크릿 읽기(SMTP·Slack·카카오·OpenSearch 비번)를 `read_secret`로.
    producer API 키도 전방 호환으로 교체(대응 Docker secret은 이번 범위 밖).
  - `docker-compose.prod.yaml`: top-level `secrets:` + `/run/secrets/*` + `*_FILE`
    env(경로만). Fernet 키는 base compose의 `FERNET_KEY: ''`가 `_CMD`를 막으므로
    `command:` 안에서 `export ...="$(cat /run/secrets/airflow_fernet_key)"`.
    admin 비번도 `cat` 방식. consumer `env_file`을 `!override ["./.env.prod"]`로
    교체 — 운영 호스트에 dev `.env`가 남아도 시크릿이 주입되지 않게.
  - `.env.example` 3블록 재구성, `.env.prod.example`, `secrets/README.md`,
    `.gitignore`(`/secrets/*` + README 예외, `.env.prod`).
  - `docs/adr/0006-secret-management.md`(Docker secrets + AWS Secrets Manager 설계),
    `SECURITY.md`(회전·만료 절, `_FILE` 경로), CI에 오버레이 `config -q` 스텝.
- 범위 외: OpenSearch 정식 인증서·`internal_users`·Kafka SASL·non-root(#118),
  Fernet 키 회전 실행 절차(#119), Airflow 앱 시크릿 매니저 이관(#119/#120),
  Kafka UI 비번(Spring `_FILE` 미지원, 127.0.0.1 전용).
- 검증: pytest 207개, 커버리지 57.75%, ruff `E9,F`, compileall, base·오버레이
  `docker compose config -q`, `git diff --check`, gitleaks(142 커밋) 통과. 격리
  Compose(prod 오버레이)로 kafka·opensearch·consumer 기동 →
  `docker inspect`/`/proc/1/environ`에 더미 시크릿 값 부재(`*_FILE` 경로만),
  로더가 secret 파일 정상 읽음, dev `.env` 누출값 미주입, consumer가 파일 비번으로
  OpenSearch 인증 시도(더미라 401 — 값 사용 확인), Kafka 컨슈머 정상 조인.
- PR #123, squash 머지 `7a91a72`. 리뷰(APPROVE WITH NITS) 반영: 빈 Fernet 키
  `test -s` 가드, secretstore 엣지 케이스 테스트 3건.

### #118 OpenSearch·Kafka 인증 강화와 컨테이너 non-root 실행

- Issue: `#118` / 브랜치: `feat/118-service-auth-nonroot`
- 범위 판단(localhost Docker 포트폴리오): 검증 가능한 것 먼저. non-root ·
  weather_writer 최소 권한 계정 · 포트 정리 · TLS 체인 검증 · Fernet 회전 절차.
  자체 서명 CA와 Kafka SASL은 **설계+재검토 조건**으로 defer(관리 포트가 이미
  127.0.0.1 전용, 데모 CA 개인키 공개, KRaft 단일 브로커 SASL 리스크).
- 구현:
  - `Dockerfile`: `app` 사용자(uid 10001), `/app/state` 소유권, `USER app`.
    명명 볼륨이 이미지 mountpoint 소유권을 복사받아 non-root 쓰기 가능.
  - `config/opensearch-security/` — 이미지의 데모 security 설정 10개 전체를
    커밋(부분 마운트 시 `config.yml` 없어 부팅 실패). `internal_users.yml`에
    `weather_writer`(해시는 `scripts/opensearch_hash.sh`), `roles.yml`에
    `weather_manager`(weather-* CRUD + 템플릿 + ISM + monitor. 그 외 전부 불가).
    `admin`은 데모 해시 유지 → healthcheck·break-glass.
  - `consumer/consumer.py` `_build_client`에 `ca_certs`·`ssl_assert_hostname=False`
    (데모 노드 인증서 SAN에 compose 서비스명 없음).
  - `docker-compose.prod.yaml`: security 설정 디렉터리 마운트, 핀된 데모
    `config/opensearch-root-ca.pem` 마운트, `OPENSEARCH_USER: weather_writer`,
    `OPENSEARCH_VERIFY_CERTS/CA_CERTS`. OSD·Kafka UI `ports: !override []`.
    Fernet·admin 비번 `command:`에 `test -s` 가드(빈 파일도 기동 중단).
  - base `docker-compose.yaml`: 9093(controller)·9600(perf) 호스트 매핑 제거.
  - `SECURITY.md`: 운영 프로필 표 갱신, 컨테이너 실행 사용자 표, 기존 볼륨 주의,
    Fernet 키 생성·회전 절차, 남은 한계(CA·SASL) 설계와 재검토 조건.
  - `tests/test_opensearch_security_config.py`: 권한 확장 회귀 가드(텍스트 검사,
    YAML 의존성 미추가).
- 검증: pytest 214개, 커버리지 57.69%, ruff `E9,F`, compileall, hadolint,
  base·오버레이(`--profile ops`) `docker compose config -q`, `git diff --check`.
  격리 Compose(prod 오버레이, 새 볼륨)로 kafka·opensearch·consumer 기동 →
  consumer `uid=10001(app)`, `/app/state` 쓰기 OK, kafka·opensearch non-root,
  weather_writer: health 200 / weather-* 색인 201 / `_cluster/settings` 403 /
  `.opendistro_security` 403 / 비-weather 인덱스 403. consumer가 weather_writer로
  TLS+CA 체인 검증 하에 연결 성공, bootstrap(템플릿 3 + ISM 정책) 성공,
  유효 메시지 1건이 `weather-alert-2026.09`·`weather-metrics-*`에 색인, 그룹 lag 0.
- 범위 외: 자체 서명 CA(재검토: 9200 외부 노출), Kafka SASL(재검토: 브로커 외부
  노출), Airflow 앱 시크릿 매니저(#119/#120), 클라우드 관리형.
- PR #124, squash 머지 `8566ec8`. 리뷰(REQUEST CHANGES) 반영: 안 쓰는 데모 계정
  5개 제거, `weather_manager`에서 `cluster_composite_ops`·aliases 제거, healthcheck를
  `weather_writer`로, OSD-prod 비호환 명시, CA 만료·볼륨 blast radius 문서화.

### #119 Airflow 메타DB PostgreSQL·LocalExecutor 전환

- Issue: `#119` / 브랜치: `feat/119-airflow-postgres`
- 구현:
  - `postgres:16-alpine` 서비스 + 전용 볼륨 `airflow_pg_data` + `pg_isready`
    healthcheck. 이미지에 `psycopg2 2.9.9` 포함 — 의존성 추가 없음.
  - airflow: `SequentialExecutor`→`LocalExecutor`, `SQL_ALCHEMY_CONN`을 postgres로,
    `PARALLELISM=8`·`MAX_ACTIVE_TASKS_PER_DAG=4`·`MAX_ACTIVE_RUNS_PER_DAG=1`,
    `depends_on: postgres healthy`. `airflow standalone` 유지(LocalExecutor는
    별도 워커 없음). dev·prod 모두 postgres(이중 백엔드 안 함).
  - prod 오버레이: 이미지 엔트리포인트가 `command:`보다 먼저 DB에 붙으므로
    `command:` export 불가 → `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: !reset null` +
    `..._CMD: "cat /run/secrets/airflow_db_conn"`(전체 DSN, `_CMD`는 shlex.split
    되므로 명령치환 불가). postgres `POSTGRES_PASSWORD: !reset null` +
    `POSTGRES_PASSWORD_FILE`(평문+`_FILE` 동시 설정 거부). `postgres_password`와
    `airflow_db_conn`은 같은 비밀번호.
  - `scripts/airflow_db_backup.sh`(`pg_dump --clean --no-owner`), CI 오버레이 스텝에
    새 더미 시크릿 2개 추가, `tests/test_compose_airflow_db.py`(텍스트 가드).
  - ADR-0007, RUNBOOK(백업·복구 절차·마이그레이션 주의), README 아키텍처 표.
- 검증: pytest 220개, ruff `E9,F`, compileall, base·오버레이 `config -q`,
  `git diff --check`. 격리 Compose:
  - dev: postgres·airflow healthy, executor=LocalExecutor, conn=postgres,
    `database is locked` 0건, DAG 2회 run `success`, airflow 컨테이너 재생성 후
    실행 이력·on/off 보존(pg 볼륨), 백업→볼륨 삭제→복구 리허설(dag_run 2행 복원).
  - prod 오버레이: postgres·airflow healthy, conn이 secret `_CMD`에서 옴,
    `docker inspect`에 평문 DSN·비밀번호 부재(경로만), auth 실패 0건.
- 범위 외: CeleryExecutor/KubernetesExecutor, webserver/scheduler 컨테이너 분리,
  MWAA, SQLite→Postgres 데이터 마이그레이션(실행 이력 유실 감수).
- PR #125, squash 머지 `92eba6d`. 리뷰(APPROVE WITH NITS) 반영: 백업 스크립트
  bash+pipefail+temp file, postgres 리소스 제한, `_AIRFLOW_WWW_USER_CREATE` 제거,
  주석 정정, 테스트 앵커.

### #120 Airflow 런타임 pip 설치 제거 (커스텀 이미지)

- Issue: `#120` / 브랜치: `feat/120-airflow-image`
- 구현:
  - `Dockerfile.airflow`(`FROM apache/airflow:2.10.0` + `pip install -r
    requirements-airflow.txt`). `--constraint`는 안 건다 — Airflow 2.10 제약
    파일(2024-08)이 `requests==2.32.3` 등으로 고정해 최신 핀과 충돌(빌드 실패
    확인). `requirements-airflow.txt` = requests·kafka-python·python-dotenv 3개
    (opensearch-py는 consumer 전용이라 제외 — grpcio 등 회피).
  - compose `airflow`: `image: apache/airflow:2.10.0` → `build: Dockerfile.airflow`
    + `image: air-quality-airflow:local`, `_PIP_ADDITIONAL_REQUIREMENTS` 제거.
  - CI: 이미지 빌드 + `import requests,kafka,dotenv,airflow` + `pip check` +
    DagBag 파싱(`list-import-errors`는 DB 필요 → `python -c`로), hadolint에
    `Dockerfile.airflow` 추가. `.dockerignore`에 `.claude`·`.github`·`secrets/`.
  - `tests/test_requirements_consistency.py`(공유 패키지 버전 일치 강제, #22 연장).
  - ADR-0008(코드 마운트 유지 근거·봉인 이미지 후속 조건), RUNBOOK·README 빌드 절차.
- 검증: pytest 224개, ruff `E9,F`, compileall, hadolint(양쪽), base·오버레이
  `config -q`, `git diff --check`. 격리 Docker:
  - `docker build -f Dockerfile.airflow` 성공, `pip check` clean, deps 굳음
    (requests 2.34.2·kafka-python 3.0.9).
  - `--network none`에서 `airflow db migrate` 완주 — 런타임에 PyPI 안 침.
  - DagBag `import_errors: {}`, dev 부팅 로그에 pip 설치 라인 0, healthy 50s.
  - 파이프라인 회귀: 메시지 1건 → OpenSearch 색인 + 카카오 발송, DAG import OK.
- 범위 외: 프라이빗 PyPI 미러, 멀티스테이지·크기 최적화, `producer/`·`consumer/`
  이미지 COPY(봉인 이미지), GHA 레이어 캐시.
