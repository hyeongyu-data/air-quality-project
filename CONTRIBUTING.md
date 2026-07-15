# 기여 가이드

이 문서는 실제 회사 운영 방식에 준하는 협업 흐름을 정의합니다. 모든 변경은 아래 절차를 따릅니다.

## 전체 워크플로우

1. **Issue 등록** — 버그/기능 템플릿으로 작업을 먼저 이슈화한다.
2. **작업 브랜치 생성** — 아래 네이밍 규칙을 따른다.
3. **작업 및 검증** — 로컬에서 검증 명령을 통과시킨다. 관련 `.md` 문서도 함께 갱신한다.
4. **Draft PR 생성** — 작업 중임을 알리고 초안으로 올린다.
5. **셀프 리뷰 및 설명 보강** — 본인이 먼저 diff를 리뷰하고 PR 설명을 채운다.
6. **Ready for review 전환** — 리뷰 준비 완료를 표시한다.
7. **에이전트 리뷰** — 자동 코드 리뷰를 실행하고 지적을 반영한다.
8. **이해도 체크 답변** — 리뷰어의 inline 코멘트에 근거를 들어 답변한다.
9. **리뷰 요청 및 승인** — 아래 "리뷰·승인 규칙" 참고.
10. **Squash merge** — 승인 후 squash로 병합하고 브랜치를 삭제한다.

## 브랜치 네이밍

`<type>/<간단한-설명>` 형식.

| type | 용도 |
| --- | --- |
| `feat` | 새 기능 |
| `fix` | 버그 수정 |
| `chore` | 설정·도구·프로세스 |
| `docs` | 문서만 변경 |
| `refactor` | 동작 불변 리팩터링 |

예: `feat/kakao-retry`, `fix/uv-index-timezone`

기본 브랜치(`master`)에 직접 push하지 않는다.

## 커밋 컨벤션

[Conventional Commits](https://www.conventionalcommits.org/) 기반. 제목은 한글로 간결하게.

```
<type>: <요약>

<본문 — 선택, 왜 바꿨는지>
```

type: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `perf`

예:
```
feat: 카카오 알림 전송 실패 시 1회 재시도 추가
fix: 자외선지수 KST 기준 시간 계산 오류 수정
```

## PR 규칙

- 제목은 커밋 컨벤션을 따른다.
- PR 템플릿의 체크리스트와 **보안 체크리스트**를 모두 채운다.
- 관련 이슈를 `Closes #N`으로 연결한다.
- 하나의 PR은 하나의 목적만 담는다(리뷰 가능한 크기 유지).

## 리뷰·승인 규칙

- 회사 표준은 **최소 2명 승인 후 Squash merge**다.
- 본 리포는 현재 collaborator가 1명이라 2인 승인을 강제할 수 없다. 그동안은:
  - **에이전트 코드 리뷰를 필수 게이트**로 사용한다(사람 리뷰 대체).
  - CODEOWNERS 소유자가 최종 확인 후 머지한다.
- collaborator가 늘면 브랜치 보호 규칙에서 `required approving reviews`를 2로 올린다.

### 현재 활성 브랜치 보호 (`master`)

| 규칙 | 상태 |
| --- | --- |
| PR 없이 직접 push | 차단 (PR 필수) |
| 필수 승인 수 | 0 (솔로 락 방지 — 팀원 증가 시 2로 상향) |
| 오래된 리뷰 자동 dismiss | 켜짐 |
| 대화(코멘트) 해결 필수 | 켜짐 |
| linear history (squash 정합) | 켜짐 |
| force push / 브랜치 삭제 | 차단 |
| admin 예외(`enforce_admins`) | 꺼짐 (소유자 잠김 방지) |

승인 수를 2로 올리려면(팀원 확보 후):

```bash
gh api -X PATCH repos/{owner}/{repo}/branches/master/protection/required_pull_request_reviews \
  -F required_approving_review_count=2
```

## 로컬 검증

```bash
# 문법 확인
python3 -m compileall producer consumer dags scripts

# 컨테이너 안 DAG import 확인
docker compose exec airflow python -m py_compile /opt/airflow/dags/air_pipeline.py
```

자세한 검증 명령은 [README](README.md#검증-명령) 참고.

## 보안 원칙

비밀정보 취급과 취약점 제보는 [SECURITY.md](SECURITY.md)를 따른다. 핵심:

- API 키·토큰·비밀번호는 **절대 커밋하지 않는다**. `.env`로만 주입한다.
- 로그·스크린샷·PR 설명에 비밀정보를 넣지 않는다.
- 새 의존성은 출처와 필요성을 확인하고 최소한으로 추가한다.
