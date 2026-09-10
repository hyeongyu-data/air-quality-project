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
  - [ ] 실제 Kafka 통합 검증 또는 Docker 상태 기록
  - [x] DLQ 복구 절차와 중복 발송 한계 문서화
  - [x] pytest 201개, ruff, compileall, Compose 설정 검증 통과
  - [x] 시크릿 검사 및 Second Brain 기록 갱신

승인 후 생성할 이슈 제목은 `fix: DLQ 실패 파티션의 Kafka 오프셋 복구`이며, 기존 Bug Report 템플릿의 현상·기대 동작·재현 절차·환경·보안 확인 항목을 작성한다. 승인 전에는 stash 적용, 원격 Issue 생성, 브랜치·push·PR을 실행하지 않는다.

## 2026-09-10 P0-1 구현 기록

- Issue #104와 이슈 번호 브랜치를 사용했다. P0-1 변경은 P0-2와 별도 커밋·PR로 유지한다.
- 파티션별 완료 오프셋, DLQ 실패 위치 되감기, 실패 파티션 후속 처리 중단, 커밋·seek 실패 시 연결 폐기를 구현했다.
- 검증: pytest 201개 통과, 커버리지 57.41%, ruff `E9,F`, compileall, Compose 설정, diff check 통과.
- Docker 명령은 현재 출력 없이 응답하지 않아 실제 Kafka 통합 검증은 미완료로 기록한다. 단위 테스트를 통합 검증으로 간주하지 않는다.
- 다음 게이트: 자체 보안·diff 검토 후 Conventional Commit과 Draft PR 작성. 원격 push/PR은 별도 확인 후 수행한다.
