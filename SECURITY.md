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
| Airflow Fernet 키 | 아래 "Fernet 키 생성·회전" 절차 | `secrets/airflow_fernet_key` 교체 후 airflow 재기동 |

카카오 refresh token 만료 임박은 Consumer가 경고 로그를 남깁니다(#50).
자동 알림 배선은 #121에서 다룹니다.

### Fernet 키 생성·회전

**생성** (최초 1회):

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" \
  | tr -d '\n' > secrets/airflow_fernet_key
chmod 600 secrets/airflow_fernet_key
```

운영 오버레이는 이 파일이 **비어 있거나 없으면 기동을 중단**합니다
(`docker-compose.prod.yaml`의 `test -s`).

**회전**: 기존 Connection/Variable이 옛 키로 암호화돼 있어 단순 교체하면 복호화가
깨집니다. Airflow는 `AIRFLOW__CORE__FERNET_KEY`에 `새키,옛키` 쉼표 목록을 받아
옛 키로 복호화·새 키로 재암호화합니다.

```bash
OLD=$(cat secrets/airflow_fernet_key)
NEW=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
printf '%s,%s' "$NEW" "$OLD" > secrets/airflow_fernet_key      # 새키,옛키
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d airflow
docker compose exec airflow airflow rotate-fernet-key           # 저장된 값 재암호화
printf '%s' "$NEW" > secrets/airflow_fernet_key                 # 옛키 제거
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d airflow
```

현재는 Connection/Variable을 쓰지 않으므로 실무상 재생성(옛 키 무시)으로 충분합니다.
DB를 PostgreSQL로 옮긴 뒤(#119)에는 회전 전 `airflow_home`/DB 백업을 먼저 합니다.

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
#   secrets/opensearch_password 는 weather_writer 비밀번호 (기본 "weatherwriter",
#   교체 시 scripts/opensearch_hash.sh 로 해시 재생성 → config/opensearch-security/internal_users.yml)
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d --build
```

| 항목 | 기본(dev) | 운영 프로필 |
| --- | --- | --- |
| REST/관리 포트(9092·8080·9200) | 모든 인터페이스 | **127.0.0.1 전용** |
| 관측 UI(Kafka UI·OSD) | 호스트 노출 | **호스트 미노출** — `docker compose exec` 또는 임시 `run --service-ports` / SSH 터널 |
| 불필요 포트(9093 controller·9600 perf) | 호스트 매핑됨 | **매핑 제거** |
| 시크릿 주입 | `.env` 평문(`env_file` → 컨테이너 env) | **Docker secrets** — `./secrets/*` → `/run/secrets/*`, `*_FILE`로 읽음. consumer `env_file`은 `.env.prod`(비밀 아님)로 교체 |
| Airflow 계정/Fernet 키 | `airflow/airflow`, 빈 키 | **secret 파일 필수 — 비었거나 없으면 기동 중단(`test -s`)** |
| OpenSearch 접근 | 보안 플러그인 off, http | **TLS + 핀된 CA 체인 검증** (무인증 401), healthcheck도 `weather_writer` |
| OpenSearch 계정 | (없음) | 안 쓰는 데모 계정 5개 제거. Consumer는 **`weather_writer` 최소 권한** — `weather-*` 데이터·템플릿·ISM만. `admin`은 break-glass(데모 해시 유지 — 회전은 `securityadmin.sh` 필요, 자체 CA 도입과 함께) |
| Kafka UI | 무인증, 동적 설정 허용 | **로그인 강제**, 동적 설정 차단 |
| Consumer 컨테이너 | root | **non-root (uid 10001)** |
| OpenSearch Dashboards | http, 보안 off | **prod 오버레이와 비호환** — `--profile ops` 는 dev 전용 |

필요 파일: `./secrets/{slack_webhook_url,smtp_password,kakao_rest_api_key,kakao_client_secret,kakao_refresh_token,opensearch_password,airflow_fernet_key,airflow_admin_password}` · `.env.prod` · `./config/opensearch-security/`(커밋됨) · compose 보간용 `.env`의 `KAFKA_UI_PASSWORD`

### 컨테이너 실행 사용자

| 서비스 | 사용자 | 비고 |
| --- | --- | --- |
| consumer | `app` (uid 10001) | `Dockerfile`에서 지정 (#118) |
| kafka | `appuser` (uid 1000) | 이미지 기본 |
| opensearch | `opensearch` (uid 1000) | 이미지 기본 |
| airflow | uid 50000 | 이미지 기본 |
| opensearch-dashboards | uid 1000 | 이미지 기본 |
| kafka-ui | root | provectus 이미지 기본. 호스트 포트 미노출로 완화 |

### 기존 볼륨 주의

`weather_writer` 계정과 non-root는 **새 볼륨**에서만 자동 반영된다. `<프로젝트>`는
compose 프로젝트명(기본은 디렉터리명 `air-quality-project`, `-p` 로 지정 가능).

- `<프로젝트>_opensearch_data` 볼륨이 이미 있으면 `.opendistro_security` 인덱스가
  남아 있어 마운트된 `config/opensearch-security/*.yml`이 무시된다 →
  `docker compose ... down -v` 후 재기동, 또는 컨테이너 안에서
  `plugins/opensearch-security/tools/securityadmin.sh -cd config/opensearch-security
  -icl -nhnv -cacert config/root-ca.pem -cert config/kirk.pem -key config/kirk-key.pem`.
- `<프로젝트>_consumer_state` 볼륨이 root 소유로 만들어져 있으면 non-root Consumer가
  `/app/state` 에 못 쓴다. 그러면 **쿨다운 상태가 fail-open** 으로 떨어져 같은
  경보가 여러 채널로 중복 발송되고, **카카오 토큰 회전 저장이 조용히 실패**한다.
  → `down -v`, 또는
  `docker run --rm -v <프로젝트>_consumer_state:/s alpine chown -R 10001:10001 /s`.

### 검증 절차

```bash
# 시크릿이 컨테이너 환경변수로 노출되지 않는다
docker inspect pj-consumer -f '{{range .Config.Env}}{{println .}}{{end}}' | grep -iE '_FILE='   # 경로만
docker exec pj-consumer sh -c "cat /proc/1/environ | tr \"\\0\" \"\\n\"" | grep -Ei 'password|token|webhook' || echo "값 노출 없음(OK)"

# non-root
docker exec pj-consumer id                        # uid=10001(app)
docker exec pj-consumer sh -c 'touch /app/state/probe && echo state-writable'

# OpenSearch 인증·최소 권한 (weather_writer 비밀번호로)
PW=$(cat secrets/opensearch_password)
curl -sk https://localhost:9200/                                              # 401
curl -sk -o /dev/null -w '%{http_code}\n' -u weather_writer:$PW https://localhost:9200/_cluster/health   # 200
curl -sk -o /dev/null -w '%{http_code}\n' -u weather_writer:$PW -XPUT https://localhost:9200/_cluster/settings -H 'Content-Type: application/json' -d '{"persistent":{}}'  # 403
curl -sk -o /dev/null -w '%{http_code}\n' -u weather_writer:$PW https://localhost:9200/.opendistro_security/_search   # 403

# 관측 UI 포트 미노출
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml port kafka-ui 8080   # 출력 없음
```

Consumer가 `weather_writer`로 TLS+체인 검증 하에 저장까지 하는지는 유효 메시지
1건(`schema_version:1`)을 발행해 `weather-alert-*`·`weather-metrics-*` 색인과
컨슈머 그룹 lag 0을 확인합니다.

> CI는 base compose만 `config -q`로 검증합니다. 오버레이의 top-level `secrets:`는
> `.github/workflows/ci.yml`의 별도 스텝(더미 `secrets/` + `.env.prod` fixture)에서
> 구문만 확인하고, 실제 시크릿 미노출은 위 수동 절차로 검증합니다.

### 롤백

오버레이 없이 재기동하면 기본 구성으로 돌아갑니다. 단, OpenSearch 보안 플러그인을 켰다 끄면 인덱스는 유지되지만 상태 전환 중 컨슈머가 백오프 재연결을 수행합니다(자동 복구).

### 남은 한계 — 설계와 재검토 조건

#118에서 non-root·최소 권한 계정·포트 정리·TLS 체인 검증까지 처리했다. 아래는
localhost Docker 범위에서 위험도가 낮아 defer한 항목이다.

- **OpenSearch·Consumer TLS는 데모 인증서**를 쓴다. 체인 검증은 켰지만 데모 CA의
  **개인키가 공개**돼 있어(설치 스크립트에 포함) 진짜 MITM은 못 막는다. 또한
  `config/opensearch-root-ca.pem`(데모 CA)은 **2028-04-19 만료** — 그 전에 교체.
  - 재검토: 9200을 127.0.0.1 밖으로 열 때, 또는 CA 만료 전.
  - 절차: `openssl`로 자체 CA + 노드/admin 인증서 생성 → `config/`에 마운트
    (파일명은 `esnode.pem`·`root-ca.pem`·`kirk.pem` 유지 → `opensearch.yml` 수정 불필요)
    → `securityadmin.sh -cacert ... -cert kirk.pem -key kirk-key.pem`로 초기화
    → `config/opensearch-root-ca.pem`을 새 CA로 교체. 이때 `admin` 비밀번호도 함께 회전.
- **`config/opensearch-security/`는 OpenSearch 2.8.0 이미지의 데모 파일**을 vendoring
  한 것이다(`config.yml`·`audit.yml` 등은 2023년판). 이미지 태그를 올리면 security
  초기화가 드리프트할 수 있으므로 재-sync 한다. `config/opensearch-security/README`
  없이 이 문단이 그 기록이다.
- **Kafka는 compose 내부 네트워크 PLAINTEXT**다. 9092는 127.0.0.1 전용, 9093은
  호스트 미노출, 브로커는 compose 네트워크 밖으로 안 나간다.
  - 재검토: 브로커를 네트워크 밖에 열 때.
  - 절차: prod 오버레이에 `SASL_PLAINTEXT`(또는 `SASL_SSL`) 리스너 추가,
    `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`·`KAFKA_SASL_ENABLED_MECHANISMS`,
    SCRAM 자격증명 secret → Producer/Consumer `sasl_mechanism`·`sasl_plain_*` env.
    KRaft 단일 브로커라 컨트롤러 리스너 설정도 함께 손봐야 한다.
- Airflow가 읽는 앱 시크릿(공공 API 키·Slack 콜백)은 아직 `.env` 바인드 마운트다.
  `docker inspect`엔 안 뜨지만, 시크릿 매니저 이관은 배포 구조 개편(#119/#120)과 함께.
- Kafka UI 비밀번호는 `.env` 환경변수로 남는다(Spring Boot `_FILE` 미지원, 호스트 미노출).
- 클라우드 배포 시 `./secrets/`를 AWS Secrets Manager로 이관한다 — 앱의 `_FILE`
  관례는 그대로 재사용된다([ADR-0006](docs/adr/0006-secret-management.md)).
