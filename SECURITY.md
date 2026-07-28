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
