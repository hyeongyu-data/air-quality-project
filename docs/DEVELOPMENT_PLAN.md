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
