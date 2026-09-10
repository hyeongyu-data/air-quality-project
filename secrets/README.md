# 운영 시크릿 파일 (#117)

`docker-compose.prod.yaml`이 이 디렉터리의 파일을 Docker secrets로 컨테이너의
`/run/secrets/<이름>`에 마운트한다. 애플리케이션은 `<VAR>_FILE` 환경변수(경로만
담김 — `docker inspect`에 노출돼도 안전)로 그 파일을 읽는다.

이 디렉터리는 `README.md`만 커밋된다(`.gitignore`의 `/secrets/*` + 예외). 실제
값 파일은 절대 커밋하지 않는다.

## 필요한 파일

| 파일 | 내용 |
| --- | --- |
| `slack_webhook_url` | Slack Incoming Webhook URL |
| `smtp_password` | SMTP 앱 비밀번호 |
| `kakao_rest_api_key` | 카카오 REST API 키 |
| `kakao_client_secret` | 카카오 client secret |
| `kakao_refresh_token` | 카카오 refresh token (없으면 빈 파일 — 상태 파일이 우선) |

> 운영 프로필의 consumer는 6개 채널 시크릿을 **모두** 마운트한다. Slack·이메일을
> 안 쓰더라도 해당 파일이 없으면 `docker compose up`이 실패하므로, 안 쓰는 채널은
> **빈 파일**로 만들어 둔다(`: > secrets/slack_webhook_url`). 로더가 빈 값을
> 미설정과 같게 다룬다.
| `opensearch_password` | Consumer가 접속할 OpenSearch 비밀번호 (데모 구성이면 `admin`) |
| `airflow_fernet_key` | Airflow Fernet 키 |
| `airflow_admin_password` | Airflow 관리자 비밀번호 |

## 생성

값 끝에 개행이 붙지 않도록 `printf %s`를 쓴다(로더가 `strip()` 하지만 습관).

```bash
mkdir -p secrets && chmod 700 secrets
printf %s "$SLACK_WEBHOOK_URL_VALUE"  > secrets/slack_webhook_url
printf %s "$SMTP_PASSWORD_VALUE"      > secrets/smtp_password
printf %s "$KAKAO_REST_API_KEY_VALUE" > secrets/kakao_rest_api_key
printf %s "$KAKAO_CLIENT_SECRET_VALUE" > secrets/kakao_client_secret
printf %s "$KAKAO_REFRESH_TOKEN_VALUE" > secrets/kakao_refresh_token
printf %s "admin"                    > secrets/opensearch_password
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" | tr -d '\n' > secrets/airflow_fernet_key
printf %s "$AIRFLOW_ADMIN_PASSWORD_VALUE" > secrets/airflow_admin_password
chmod 600 secrets/*
```

비밀 아닌 설정(`SMTP_HOST`, `*_ENABLED` 등)은 `.env.prod`(→ `.env.prod.example`).

## 회전

발급처 콘솔에서 재발급 → 해당 파일 교체 → `docker compose ... up -d` 재기동.
자세한 절차는 `SECURITY.md`의 "시크릿 회전·만료" 절.
