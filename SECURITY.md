# 보안 정책

## 취약점 제보

취약점은 **공개 이슈로 올리지 마세요.** GitHub Security Advisory로 비공개 제보합니다.

- 제보 채널: [Security Advisory 생성](https://github.com/hyeongyu-data/air-quality-project/security/advisories/new)
- 포함 내용: 영향 범위, 재현 절차, 가능하면 완화책
- 응답 목표: 영업일 기준 3일 내 접수 확인

## 비밀정보 취급 원칙

이 프로젝트는 공공데이터 API 키, SMTP 앱 비밀번호, 카카오 토큰, AWS 자격증명을 사용합니다. 모두 **비밀정보**로 취급합니다.

- 개발 환경은 `.env`로 주입한다. `.env`는 `.gitignore`로 차단되어 있으며 절대 커밋하지 않는다.
- 운영 환경은 `.env` 평문 대신 **Docker secrets**를 쓴다 — `./secrets/<이름>` 파일이
  컨테이너의 `/run/secrets/<이름>`으로 마운트되고, 애플리케이션은 `<VAR>_FILE`
  환경변수(값이 아니라 경로)로 읽는다. 설계 근거는 [ADR-0006](docs/adr/0006-secret-management.md),
  파일 목록·생성법은 `secrets/README.md`, 실행은 아래 "운영 프로필" 절.
- 코드, 로그, PR 설명, 이슈, 스크린샷 어디에도 실제 키/토큰/비밀번호를 넣지 않는다.
- README/문서의 예시 값은 항상 `your_xxx` 형태의 플레이스홀더만 사용한다.
- Gmail은 계정 비밀번호가 아니라 앱 비밀번호를 사용한다.
- AWS는 장기 액세스 키 대신 가능한 한 최소 권한 IAM 역할/프로파일을 사용한다.

## 시크릿 회전·만료

정기 회전을 권장합니다(권고 주기: 분기 1회, 유출 의심 시 즉시).

| 시크릿 | 회전 방법 | 반영 |
| --- | --- | --- |
| 공공데이터 API 키 | 공공데이터포털 콘솔에서 재발급 | dev: `.env` / 운영: `secrets/weather_api_key`·`airkorea_api_key` 교체 후 재기동 |
| SMTP 앱 비밀번호 | Gmail 앱 비밀번호 재발급 | `secrets/smtp_password` 교체 후 consumer 재기동 |
| Slack Webhook | Slack 앱 설정에서 재발급 | `secrets/slack_webhook_url` 교체 후 재기동 |
| 카카오 REST 키 / client secret | 카카오 개발자 콘솔 | `secrets/kakao_*` 교체 |
| 카카오 refresh token | `scripts/kakao_get_refresh_token.py` (재발급 + 발급 즉시 검증, #95). 회전값은 상태 파일에 자동 저장 | 자동 |
| Airflow 관리자 비밀번호 | 임의 문자열 재생성 | `secrets/airflow_admin_password` 교체 후 airflow 재기동(`reset-password` 자동 실행) |
| Airflow Fernet 키 | **주의**: 기존 Connection/Variable이 옛 키로 암호화돼 있어 단순 교체하면 복호화 불가 | export → 키 교체 → import 절차 필요. Airflow DB 전환(#119)과 함께 정식 절차 수립 |

카카오 refresh token 만료 임박은 Consumer가 경고 로그를 남깁니다(#50).
자동 알림 배선은 #121에서 다룹니다.

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
cp .env.prod.example .env.prod          # 비밀 아닌 설정
# ./secrets/ 파일 생성 — secrets/README.md 참고
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d --build
```

| 항목 | 기본(dev) | 운영 프로필 |
| --- | --- | --- |
| 관리 포트(9092·8080·9200·8081) | 모든 인터페이스 | **127.0.0.1 전용** |
| 시크릿 주입 | `.env` 평문(`env_file` → 컨테이너 env) | **Docker secrets** — `./secrets/*` → `/run/secrets/*`, `*_FILE`로 읽음. consumer `env_file`은 `.env.prod`(비밀 아님)로 교체 |
| Airflow 계정/Fernet 키 | `airflow/airflow`, 빈 키 | **secret 파일 필수 — 없으면 기동 실패** |
| OpenSearch | 보안 플러그인 off, http | **TLS + 인증** (무인증 401) |
| Kafka UI | 무인증, 동적 설정 허용 | **로그인 강제**, 동적 설정 차단 |

필요 파일: `./secrets/{slack_webhook_url,smtp_password,kakao_rest_api_key,kakao_client_secret,kakao_refresh_token,opensearch_password,airflow_fernet_key,airflow_admin_password}` · `.env.prod` · compose 보간용 `.env`의 `KAFKA_UI_PASSWORD`

### 검증 절차

```bash
# 시크릿이 컨테이너 환경변수로 노출되지 않는다
docker inspect pj-consumer -f '{{range .Config.Env}}{{println .}}{{end}}' | grep -iE '_FILE='   # 경로만
docker exec pj-consumer sh -c "cat /proc/1/environ | tr \"\\0\" \"\\n\"" | grep -Ei 'password|token|webhook' || echo "값 노출 없음(OK)"

curl -sk https://localhost:9200/                  # 401 이어야 함
curl -sk -u admin:$(cat secrets/opensearch_password) https://localhost:9200/   # 200
curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/api/clusters  # 302 (로그인 리다이렉트)
docker port pj-opensearch 9200                    # 127.0.0.1:9200
```

Consumer가 TLS+인증으로 저장까지 하는지는 메시지 1건을 발행해 `weather-alert-*` 색인을 확인합니다.

> CI는 base compose만 `config -q`로 검증합니다. 오버레이의 top-level `secrets:`는
> `.github/workflows/ci.yml`의 별도 스텝(더미 `secrets/` + `.env.prod` fixture)에서
> 구문만 확인하고, 실제 시크릿 미노출은 위 수동 절차로 검증합니다.

### 롤백

오버레이 없이 재기동하면 기본 구성으로 돌아갑니다. 단, OpenSearch 보안 플러그인을 켰다 끄면 인덱스는 유지되지만 상태 전환 중 컨슈머가 백오프 재연결을 수행합니다(자동 복구).

### 알려진 한계 — 실배포 전 필수 처리

- OpenSearch는 이미지의 **데모 인증서와 내장 admin 계정**을 씁니다. 정식 인증서 발급과 `internal_users` 교체가 선행돼야 합니다. (#118)
- Kafka는 compose 내부 네트워크의 PLAINTEXT입니다. 포트 바인딩으로 외부 접근은 차단되지만, 브로커를 네트워크 밖에 열려면 SASL/TLS가 필요합니다. (#118)
- Airflow가 읽는 앱 시크릿(공공 API 키·Slack 콜백)은 아직 `.env` 바인드 마운트입니다. `docker inspect`엔 안 뜨지만, 시크릿 매니저 이관은 배포 구조 개편(#119/#120)과 함께합니다.
- Kafka UI 비밀번호는 `.env` 환경변수로 남습니다(Spring Boot `_FILE` 미지원, 127.0.0.1 전용).
- 클라우드 배포 시 `./secrets/`를 AWS Secrets Manager로 이관합니다 — 앱의 `_FILE` 관례는 그대로 재사용됩니다([ADR-0006](docs/adr/0006-secret-management.md)).
