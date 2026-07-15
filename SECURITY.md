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
