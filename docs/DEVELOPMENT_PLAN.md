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
