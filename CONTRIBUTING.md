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
| admin도 규칙 강제(`enforce_admins`) | 켜짐 (admin도 PR 경유 — 직접 push 차단) |

승인 수를 2로 올리려면(팀원 확보 후):

```bash
gh api -X PATCH repos/{owner}/{repo}/branches/master/protection/required_pull_request_reviews \
  -F required_approving_review_count=2
```

## 로컬 검증

```bash
# 문법 확인
python3 -m compileall producer consumer dags scripts

# 단위 테스트 (순수 판정 로직)
pip install pytest && pytest -q

# 컨테이너 안 DAG import 확인
docker compose exec airflow python -m py_compile /opt/airflow/dags/air_pipeline.py
```

`master`로 향하는 PR·push는 GitHub Actions(`.github/workflows/ci.yml`)가 compile + pytest를 자동 실행한다. **CI 통과는 머지 필수 게이트**다(브랜치 보호의 required status check). 자세한 검증 명령은 [README](README.md#검증-명령) 참고.

## 보안 원칙

비밀정보 취급과 취약점 제보는 [SECURITY.md](SECURITY.md)를 따른다. 핵심:

- API 키·토큰·비밀번호는 **절대 커밋하지 않는다**. `.env`로만 주입한다.
- 로그·스크린샷·PR 설명에 비밀정보를 넣지 않는다.
- 새 의존성은 출처와 필요성을 확인하고 최소한으로 추가한다.

## 버전 정합성 원칙

선언과 실행이 어긋나면 재현성이 깨진다. 버전이 사는 곳은 네 군데이고, 올릴 때는 짝을 함께 올린다.

| 선언 | 실행 주체 | 짝 |
| --- | --- | --- |
| `requirements-consumer.txt` | Consumer 이미지 빌드 · CI 테스트 | compose의 `_PIP_ADDITIONAL_REQUIREMENTS`와 동일 버전 유지 |
| compose `_PIP_ADDITIONAL_REQUIREMENTS` | Airflow 컨테이너 기동 시 설치 | 위와 동일 버전 |
| `requirements.txt`의 airflow 핀 | 없음(IDE/참조용) | compose의 `apache/airflow` 이미지 태그 |
| compose 이미지 태그 | 각 컨테이너 | `latest` 금지 — 검증된 태그로 고정 |

갱신 절차: 핀 수정 → `pip install -r requirements-consumer.txt`로 해석 확인(CI가 자동 수행) → `docker compose up -d --build` 기동 확인 → 짝 파일 동기화 여부를 PR 셀프 리뷰에서 확인.

## Dependabot PR 처리 기준

Dependabot이 주간으로 올리는 갱신 PR은 **7일 내에 판단**한다. 머지하거나, 보류 사유를 PR 코멘트로 남긴다. 방치는 문서("주간 점검한다")를 거짓말로 만든다.

| 분류 | 기준 | 처리 |
| --- | --- | --- |
| 저위험 | 패치·마이너 버전, CI 전용 액션(`actions/*`) | CI 통과 확인 후 머지 |
| 수동 검토 | 메이저 버전, 런타임 이미지 베이스, 실행 경로에 닿는 라이브러리 | 호환성 확인 후 판단. 보류 시 사유와 재검토 조건을 코멘트로 |

보류 사례: 실제 런타임과 무관한 유령 핀(예: `requirements.txt`의 airflow — 실행은 compose 이미지), CI와 런타임 버전이 갈리게 되는 이미지 bump.

오래된 PR은 base가 앞서가 있으므로 `@dependabot rebase` 코멘트로 리베이스한 뒤 CI를 다시 확인한다.
