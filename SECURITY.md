# 보안 정책

## 취약점 제보

취약점은 **공개 이슈로 올리지 마세요.** GitHub Security Advisory로 비공개 제보합니다.

- 제보 채널: [Security Advisory 생성](https://github.com/hyeongyu-data/air-quality-project/security/advisories/new)
- 포함 내용: 영향 범위, 재현 절차, 가능하면 완화책
- 응답 목표: 영업일 기준 3일 내 접수 확인

## 비밀정보 취급 원칙

이 프로젝트는 공공데이터 API 키, SMTP 앱 비밀번호, 카카오 토큰, AWS 자격증명을 사용합니다. 모두 **비밀정보**로 취급합니다.

- 모든 비밀정보는 `.env`로만 주입한다. `.env`는 `.gitignore`로 차단되어 있으며 절대 커밋하지 않는다.
- 코드, 로그, PR 설명, 이슈, 스크린샷 어디에도 실제 키/토큰/비밀번호를 넣지 않는다.
- README/문서의 예시 값은 항상 `your_xxx` 형태의 플레이스홀더만 사용한다.
- Gmail은 계정 비밀번호가 아니라 앱 비밀번호를 사용한다.
- AWS는 장기 액세스 키 대신 가능한 한 최소 권한 IAM 역할/프로파일을 사용한다.

## 비밀정보 유출 시 대응

키/토큰이 커밋되었거나 노출된 것으로 의심되면:

1. **즉시 해당 자격증명을 회전(rotation)**한다 — 발급처 콘솔에서 재발급/폐기.
   - 공공데이터포털(기상청/에어코리아) 서비스 키
   - Gmail 앱 비밀번호
   - Kakao REST API 키 / refresh token
   - AWS 액세스 키
2. 노출된 값을 `.env`에서 교체한다.
3. 필요 시 git 히스토리에서 제거한다(`git filter-repo` 등). 이미 공개된 값은 회전이 최우선이다.

## 의존성 보안

- `.github/dependabot.yml`로 pip·docker·github-actions 의존성의 알려진 취약점을 주간 점검한다.
- Dependabot PR은 `security` 라벨로 표시되며 우선 검토한다.

## 지원 범위

이 리포는 학습/포트폴리오 성격의 단일 브랜치(`master`) 프로젝트입니다. 보안 패치는 `master`에만 적용합니다.

## 로그·페이로드의 시크릿 취급

공공데이터포털 API는 헤더 인증을 지원하지 않아 서비스 키를 쿼리스트링으로 보냅니다. 그래서 `requests` 예외 문자열에는 키를 포함한 전체 URL이 들어갑니다. 이 값이 수집 오류 메시지(`last_error`)에 담기면 로그뿐 아니라 `data_warnings`를 통해 Kafka 페이로드와 알림 이메일 본문까지 흘러갑니다.

이를 막기 위해 두 겹으로 처리합니다.

1. **값이 만들어지는 지점에서 마스킹** — `producer/masking.py`의 `mask_secrets()`를 모든 `last_error` 대입에 적용합니다. 로그 마스킹만으로는 페이로드로 새는 경로를 막을 수 없습니다.
2. **출력 단계의 안전망** — `install_secret_filter()`가 루트 로거에 `SecretMaskingFilter`를 붙여, 이후 누군가 URL이나 토큰을 그대로 `logger`에 넘겨도 한 번 더 걸립니다.

마스킹 대상: `serviceKey`, `access_token`, `refresh_token`, `client_secret`, `rest_api_key`, `api_key`, `password`, `Authorization: Bearer`.

새로운 시크릿 종류를 다루게 되면 `producer/masking.py`의 `_RULES`에 패턴을 추가하고 `tests/test_masking.py`에 회귀 테스트를 함께 넣습니다.

페이로드 전문 로깅은 하지 않습니다. 수집·수신 로그는 식별자와 결측 항목 요약만 남깁니다.

## CI 보안 게이트

`master`로 향하는 모든 PR은 다음을 통과해야 머지됩니다(단일 필수 체크에 포함).

| 검사 | 도구 | 실패 시 |
| --- | --- | --- |
| 정적 분석 (오류·버그 클래스) | ruff `E9,F` | 지적된 코드를 수정한다. 의도적 미사용 import(패키지 존재 검증 등)는 `# noqa: F401`에 사유 주석을 함께 단다 |
| 의존성 취약점 | pip-audit | 패치 버전으로 올린다. 즉시 불가하면 영향 분석을 PR에 남기고 보류 사유를 기록한다 |
| 비밀정보 (커밋 이력 전체) | gitleaks | **유출로 간주** — 값을 회전(재발급)부터 하고, 히스토리 정리와 무관하게 기존 값은 폐기한다 |
| Dockerfile | hadolint | 수정하거나, 근거가 있으면 `# hadolint ignore=규칙` 위에 사유 주석을 단다 |

오탐 예외는 억제 주석(noqa/hadolint ignore)에 **반드시 사유를 함께** 남기고, PR 리뷰에서 그 사유를 확인합니다.

## 운영 프로필 (docker-compose.prod.yaml)

기본 compose는 로컬 학습용(무인증·평문·기본 계정)입니다. 운영에 가까운 구성이 필요하면 오버레이를 겹칩니다.

```bash
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d --build
```

| 항목 | 기본(dev) | 운영 프로필 |
| --- | --- | --- |
| 관리 포트(9092·8080·9200·8081) | 모든 인터페이스 | **127.0.0.1 전용** |
| Airflow 계정/Fernet 키 | `airflow/airflow`, 빈 키 | **환경변수 필수 — 미설정이면 기동 실패** |
| OpenSearch | 보안 플러그인 off, http | **TLS + 인증** (무인증 401) |
| Kafka UI | 무인증, 동적 설정 허용 | **로그인 강제**, 동적 설정 차단 |

필요 환경변수: `AIRFLOW_ADMIN_PASSWORD` · `AIRFLOW_FERNET_KEY` · `OPENSEARCH_ADMIN_PASSWORD` · `KAFKA_UI_PASSWORD`

### 검증 절차

```bash
curl -sk https://localhost:9200/                  # 401 이어야 함
curl -sk -u admin:$OPENSEARCH_ADMIN_PASSWORD https://localhost:9200/   # 200
curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/api/clusters  # 302 (로그인 리다이렉트)
docker port pj-opensearch 9200                    # 127.0.0.1:9200
```

Consumer가 TLS+인증으로 저장까지 하는지는 메시지 1건을 발행해 `weather-alert-*` 색인을 확인합니다.

### 롤백

오버레이 없이 재기동하면 기본 구성으로 돌아갑니다. 단, OpenSearch 보안 플러그인을 켰다 끄면 인덱스는 유지되지만 상태 전환 중 컨슈머가 백오프 재연결을 수행합니다(자동 복구).

### 알려진 한계 — 실배포 전 필수 처리

- OpenSearch는 이미지의 **데모 인증서와 내장 admin 계정**을 씁니다. 정식 인증서 발급과 `internal_users` 교체가 선행돼야 합니다.
- Kafka는 compose 내부 네트워크의 PLAINTEXT입니다. 포트 바인딩으로 외부 접근은 차단되지만, 브로커를 네트워크 밖에 열려면 SASL/TLS가 필요합니다.
- 시크릿은 여전히 `.env` 평문입니다. 클라우드 배포 시 시크릿 매니저로 이관합니다.
