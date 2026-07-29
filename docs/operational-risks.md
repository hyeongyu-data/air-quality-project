# 운영 리스크와 개선 방안

이 시스템을 실제로 운영한다고 가정했을 때 발생 가능한 장애를 코드 근거와 함께 정리한 문서입니다. 각 항목은 해당 GitHub 이슈로 연결됩니다.

> **진행 상황 (2026-07-29 갱신)** — **P0 6건을 전량 해결**했습니다. 1절(데이터 품질) 전체, 2.1·2.2·2.4(전달 보증), 3.1·3.3·3.4(영속성·멱등성), 4.1(시크릿 로그), 5절(시간대). 각 절 머리에 상태와 PR 번호를 표기했습니다. 인프라 변경(3.1·3.3)은 Docker로 실제 기동해 확인한 뒤 머지했습니다.
>
> 그 과정에서 회귀를 하나 잡았습니다 — 죽은 코드 청소 때 `producer/__init__.py`의 export를 함께 정리하지 않아 DAG가 `ImportError`로 죽고 있었는데 CI는 초록색이었습니다. `compileall`은 문법만 보고, 테스트는 패키지 `__init__`을 거치지 않았기 때문입니다. 패키지 import 테스트로 막았습니다([PR #71](https://github.com/hyeongyu-data/air-quality-project/pull/71)).

## 요약

해피 패스는 완성돼 있습니다. 문제는 **실패를 실패로 인지하는 경로가 대부분 끊겨 있다**는 것입니다.

- 외부 API가 죽으면 결측이 `0`으로 치환돼 **거짓 알림이 정상 알림과 구분 없이** 발송된다.
- 알림 채널이 죽어도 쿨다운 시그니처는 "발송됨"으로 전진해, 등급이 유지되는 동안 **알림이 영구히 사라진다**.
- Kafka 발행이 실패해도 DAG는 초록색으로 끝나고, 실패를 사람에게 알릴 경로가 없다.
- 상태 저장소(Kafka, Airflow 메타DB)는 컨테이너 재생성 시 데이터가 통째로 사라지는 구성이다.
- 관측 수단은 `docker logs`와 사람의 눈이 전부다.

판정 로직과 협업 프로세스는 이 규모 프로젝트로서 충실하지만, **운영 성숙도는 "로컬 데모" 단계**입니다.

---

## 1. 데이터 품질 — 결측이 정상값으로 둔갑한다

### 1.1 결측을 0으로 치환 — **해결됨 (PR #58)** ([#33](https://github.com/hyeongyu-data/air-quality-project/issues/33))

`consumer/consumer.py:76-82`의 `_safe_float(value, default: float = 0)`가 9개 지표 전부에 적용된다.

| 트리거 | 현재 동작 | 사용자 영향 |
| --- | --- | --- |
| 생활기상지수 + 초단기실황 동시 실패 | `feels_like_temp = None → 0.0` → `rules.py:225`의 `max_value=0` 규칙 매칭 | **한여름에 "매우나쁨 · 동상 위험 · 내복 필수"** 발송 |
| 에어코리아 장애 | `pm10 = None → 0.0` → `rules.py:57` GOOD | **"좋음 · 외출 자유"라는 거짓 안전 신호** |

`producer/producer.py:1107-1125`가 `data_warnings`에 결측 사유를 담고 `consumer/alert.py:369-395`가 본문 하단에 출력하지만, **판정에는 전혀 반영되지 않는다.** 경고를 만들어 놓고 쓰지 않는다.

**개선**: `AlertLevel.UNKNOWN` 도입, 결측 지표를 판정·시그니처·발송에서 제외. 핵심 지표 전량 결측이면 사용자 알림 대신 운영 경보로 전환.

### 1.2 응답 스키마·발표시각 변경 시 조용한 전면 결측 — **부분 해결 (PR #60)** ([#35](https://github.com/hyeongyu-data/air-quality-project/issues/35))

`_is_success_response`가 False면 빈 리스트를 반환하고(`producer/producer.py:144-149, 540-546`) 상위가 `or {}`로 흡수한다(`:1093-1096`). 결과는 1.1과 동일한 전 항목 0이며, Airflow 태스크는 성공으로 끝난다.

**개선**: 수집 단계에 **필수 지수 최소 개수 계약**을 두고 미달 시 `AirflowException`. 고정 픽스처 기반 파싱 계약 테스트를 CI에 추가([#43](https://github.com/hyeongyu-data/air-quality-project/issues/43)).

### 1.3 죽은 지표 — **해결됨 (PR #66)** ([#44](https://github.com/hyeongyu-data/air-quality-project/issues/44))

황사 권한이 없으면 PM10 평균을 그대로 복사하는데(`producer/producer.py:666-667`), 황사 GOOD 임계가 150(`rules.py:141`)이라 서울 PM10 평균으로는 사실상 항상 "좋음"이다. 또한 `pm10` 수치는 전 측정소 평균인데 `pm10_grade`는 첫 측정소 등급을 그대로 쓴다(`:696-708`).

---

## 2. 전달 보증 — 실패가 성공으로 기록된다

### 2.1 발송 실패인데 쿨다운은 전진 — **해결됨 (PR #59)** ([#34](https://github.com/hyeongyu-data/air-quality-project/issues/34)) — 가장 치명적

```
send_all()          → 채널별 실패를 dict에 담기만 함        (alert.py:812-818)
OpenSearch 저장     → grade_signature를 무조건 기록         (alert.py:699-715)
다음 회차            → should_send() = False → 외부 발송 생략 (consumer.py:310-313)
```

SMTP 앱 비밀번호 만료(535), 카카오 refresh token 만료, Slack webhook 폐기 중 **하나만 발생해도** 등급이 유지되는 동안 알림이 오지 않는다. 채널이 복구돼도 마찬가지다.

탐지도 불가능하다. `consumer.py:321`의 `any(results.values())`는 콘솔 채널이 항상 True(`alert.py:813`)라 외부 발송이 전멸해도 카운터가 증가한다.

**개선**: "발송됨"의 정의를 **외부 채널 최소 1개 성공**으로 바꾸고, 실패 시 시그니처를 전진시키지 않는다. `delivered_channels`를 함께 저장해 채널별로 판정한다.

### 2.2 DAG가 실패를 성공으로 보고 — **해결됨 (PR #60)** ([#35](https://github.com/hyeongyu-data/air-quality-project/issues/35))

- `dags/air_pipeline.py:150-156` — 수집 실패 시 예외 없이 반환. 태스크는 성공.
- `:228-239` — XCom이 비어도 `{"status": "success"}`.
- `:213-226` — 발행 실패해도 `published_count`만 0이고 반환은 success.
- `:45` — `email_on_failure: False`, `on_failure_callback` 없음.

`DEFAULT_ARGS`의 `retries: 2`(`:43`)는 실패가 실패로 표현돼야 의미를 갖는다. 지금은 발동할 일이 없다.

### 2.3 at-most-once + DLQ 부재 ([#21](https://github.com/hyeongyu-data/air-quality-project/issues/21))

`enable_auto_commit=True`(`consumer.py:207`)에서 오프셋은 처리 성공과 무관하게 커밋된다. 처리 중 예외는 `except`가 삼키므로(`:327-330`) **해당 메시지는 재처리되지 않는다.** 파싱 불가 메시지 1건 = 그 시간대 알림 영구 소실.

### 2.4 카카오 토큰 회전 폐기 — **해결됨 (PR #62)** ([#50](https://github.com/hyeongyu-data/air-quality-project/issues/50))

갱신 응답의 신규 refresh token을 로그만 남기고 버린다(`alert.py:655-656`). 기존 토큰 만료 시점에 카카오 알림이 영구 중단되고, 2.1과 겹치면 복구 후에도 재발송이 일어나지 않는다.

---

## 3. 상태 영속성 — 재생성 한 번에 사라진다

### 3.1 Kafka 볼륨 부재 — **해결됨 (PR #69)** ([#36](https://github.com/hyeongyu-data/air-quality-project/issues/36))

`kafka` 서비스에 `volumes:` 키가 없다. KRaft 메타데이터·로그 세그먼트·`__consumer_offsets`가 전부 컨테이너 쓰기 레이어에 있다. 재생성 시 토픽과 오프셋이 소멸하고, 컨슈머는 `auto_offset_reset='latest'`(`consumer.py:208`)로 재구독해 백로그를 **조용히 스킵**한다. `KAFKA_LOG_RETENTION_HOURS: 24`도 같은 결과를 만든다.

즉 "Kafka를 써서 재처리가 가능하다"는 이 아키텍처의 유일한 명분이 실제로는 성립하지 않는다.

### 3.2 Airflow 메타DB가 볼륨 밖 ([#47](https://github.com/hyeongyu-data/air-quality-project/issues/47))

메타DB 경로는 `/opt/airflow/airflow.db`인데 마운트된 볼륨은 `/opt/airflow/logs`뿐이다. 재생성하면 실행 이력·XCom·**DAG on/off 상태**가 전멸한다. 사용자가 켠 DAG가 꺼진 채로 돌아와 조용히 무알림이 된다.

`airflow standalone`은 웹서버·스케줄러·트리거러가 한 SQLite를 공유해 `database is locked` 가능성이 상존하고, 태스크가 스케줄러를 블록하므로 수집이 느려지면 `data_interval_end` 기준 `run_hour`가 실제 시각과 어긋나 **잘못된 수집 모드**로 동작한다.

### 3.3 OpenSearch 미가용 시 쿨다운 무력화 — **해결됨 (PR #70)** ([#37](https://github.com/hyeongyu-data/air-quality-project/issues/37))

생성자에서 예외를 삼키고 `client = None`으로 고정한다(`consumer.py:63-65`). 재연결이 없어 다음 재시작 전까지 그 상태가 유지되고, 그동안 `latest_signature()`가 항상 `None`을 반환해 **매 메시지마다 전 채널 발송**이 일어난다.

healthcheck는 `curl -f /_cluster/health`라 200만 오면 통과한다. **red 클러스터도 healthy로 뜨고** `depends_on: service_healthy`가 아무 보호도 못 한다.

여기에 힙이 128MB이고 일별 인덱스가 무한 증가하며 ISM·인덱스 템플릿이 없다. 결측이 파이썬 `int 0`으로 들어가면 첫 문서 기준으로 `long` 매핑이 잡혀 이후 `45.6`이 **45로 절삭**된다.

---

## 3.4 멱등성 — **해결됨 (PR #64)** ([#41](https://github.com/hyeongyu-data/air-quality-project/issues/41))

발행·색인 어디에도 중복 방지 키가 없어 DAG 재시도가 곧 중복 발행이었고, OpenSearch는 `_id` 자동 생성이라 재처리할 때마다 문서가 쌓였습니다.

**예정된 실행 시각** 기준의 결정적 키 `event_id`(`지역:YYYYMMDDHH`, KST)를 도입했습니다. 벽시계로 만들면 재시도마다 값이 갈려 목적을 잃습니다. Kafka는 `enable_idempotence=True`, OpenSearch는 `_id = event_id`로 upsert 합니다.

## 4. 보안

### 4.1 로그로 새는 시크릿 — **해결됨 (PR #61)** ([#38](https://github.com/hyeongyu-data/air-quality-project/issues/38))

서비스 키가 쿼리스트링으로 전달되고(`producer.py:124-128, 497-503, 773-782`), `requests` 연결 예외 문자열에는 **serviceKey를 포함한 전체 URL**이 들어간다. 이를 그대로 `last_error`에 담아 출력한다(`:510-516`). Airflow 태스크 로그는 볼륨에 영구 보존되고 만료 정책이 없다.

부수 경로: 수집 페이로드 전문 로그(`dags/air_pipeline.py:158`), 수신 메시지 전문 로그(`consumer.py:293`), 카카오 토큰 응답 전체 stdout 출력(`scripts/kakao_get_refresh_token.py:126-128`).

### 4.2 개발용 설정의 노출면 ([#20](https://github.com/hyeongyu-data/air-quality-project/issues/20))

Airflow 계정이 커맨드에 하드코딩돼 재시작마다 `airflow/airflow`로 리셋되고, `AIRFLOW__CORE__FERNET_KEY`가 빈 문자열이라 Connection·Variable이 평문 저장된다. OpenSearch는 보안 플러그인이 꺼진 채 9200을 공개하고, Kafka UI는 인증 없이 `DYNAMIC_CONFIG_ENABLED: 'true'`로 열려 있어 UI에서 클러스터 설정 변경이 가능하다. 컨슈머는 `env_file`로 전 시크릿을 환경변수에 올려 `docker inspect` 한 번에 노출된다.

---

## 5. 시간대 — **해결됨 (PR #65)** ([#39](https://github.com/hyeongyu-data/air-quality-project/issues/39))

전 구간이 naive `datetime.now()`다. OpenSearch는 오프셋 없는 ISO를 UTC로 해석하므로 이력이 9시간 밀린다. 컨슈머 컨테이너에는 `TZ`가 없어(Airflow만 `Asia/Seoul`) 같은 스택 안에서 두 프로세스의 "지금"이 다르고, KST 00~09시 구간에서 일별 인덱스 날짜가 전날로 갈라진다.

한국은 DST가 없어 DST 자체는 무해하다. 문제는 **TZ가 조금이라도 바뀌면 조용히 틀리는 구조**라는 점이다. 오늘 예보 필터가 `datetime.now().strftime("%Y%m%d")` 기반이라(`producer.py:839-854`), README가 제시한 AWS 이전(Lambda는 UTC 기본)을 실행하는 순간 `today_items`가 전량 비어 1.1의 전면 오탐으로 직결된다.

---

## 6. 관측성 ([#49](https://github.com/hyeongyu-data/air-quality-project/issues/49))

**지금 보이는 것**: 컨테이너 stdout, Airflow UI의 태스크 성공/실패, Kafka UI, OpenSearch 인덱스 목록.

**안 보이는 것**:

1. 알림이 실제로 전달됐는지 — 채널별 결과가 집계되지 않는다
2. 데이터 품질 — 결측 지표 수, `data_warnings` 발생률
3. 처리 지연 — 발행 → 발송 end-to-end. 참고로 `max_poll_records=1` + `sleep(10)` 조합이라 **처리량 상한이 초당 0.1건**인데 이를 문서화한 곳이 없다
4. 컨슈머 생존 — healthcheck 없음. 매시간 정상 종료라 크래시 루프와 구분 불가([#40](https://github.com/hyeongyu-data/air-quality-project/issues/40))
5. 무알림이 정상인지 고장인지 — 구분할 신호가 없다([#42](https://github.com/hyeongyu-data/air-quality-project/issues/42))

**최소 알람 세트**: (a) 2시간 이상 신규 이력 없음 (b) 채널 발송 실패율 > 0 (c) 결측 지표 발생 (d) 컨슈머 랙 > 3 (e) DAG 실패 또는 `published_messages == 0` (f) 디스크 80% (g) OpenSearch red (h) 카카오 토큰 만료 임박

---

## 개선 순서

번호는 **의존 순서**입니다. 앞 단계가 뒤 단계의 전제입니다.

| 순서 | 목표 | 이슈 |
| --- | --- | --- |
| 1 ✔ | 거짓말을 멈춘다 — 틀린 정보 발송과 실패의 성공 보고를 끊는다 | ~~[#33](https://github.com/hyeongyu-data/air-quality-project/issues/33) [#34](https://github.com/hyeongyu-data/air-quality-project/issues/34) [#35](https://github.com/hyeongyu-data/air-quality-project/issues/35) [#38](https://github.com/hyeongyu-data/air-quality-project/issues/38) [#50](https://github.com/hyeongyu-data/air-quality-project/issues/50)~~ 완료 |
| 2 ✔ | 상태를 잃지 않는다 — 영속성과 멱등성 | ~~[#41](https://github.com/hyeongyu-data/air-quality-project/issues/41) [#36](https://github.com/hyeongyu-data/air-quality-project/issues/36) [#37](https://github.com/hyeongyu-data/air-quality-project/issues/37)~~ 완료 · [#47](https://github.com/hyeongyu-data/air-quality-project/issues/47) Airflow 메타DB 남음 |
| 3 | 측정할 수 있게 만든다 | [#49](https://github.com/hyeongyu-data/air-quality-project/issues/49) [#42](https://github.com/hyeongyu-data/air-quality-project/issues/42) [#40](https://github.com/hyeongyu-data/air-quality-project/issues/40) |
| 4 | 회귀를 막는다 — 테스트 가능한 구조로 바꾼 뒤 리팩터링 | [#43](https://github.com/hyeongyu-data/air-quality-project/issues/43) [#48](https://github.com/hyeongyu-data/air-quality-project/issues/48) [#45](https://github.com/hyeongyu-data/air-quality-project/issues/45) [#46](https://github.com/hyeongyu-data/air-quality-project/issues/46) |
| 5 | 근거와 증거를 남긴다 | [#52](https://github.com/hyeongyu-data/air-quality-project/issues/52) [#51](https://github.com/hyeongyu-data/air-quality-project/issues/51) [#53](https://github.com/hyeongyu-data/air-quality-project/issues/53) |

순서를 뒤집으면 안 되는 이유가 하나씩 있습니다. 3번 없이 5번을 하면 README에 쓸 숫자가 없고, 4번 없이 리팩터링하면 검증 없는 변경이 되며, 1번 없이 2번을 하면 잘못된 데이터를 성실하게 영속화하게 됩니다.

---

## 프로덕션 전제조건 체크리스트

현재 충족 여부를 함께 표기했습니다.

**데이터 정합성**
- [x] 결측과 실제 0이 코드·저장소·알림 본문에서 구분된다
- [x] 필수 지표 최소 개수 계약이 있고 미달 시 파이프라인이 실패한다
- [ ] 공공 API 응답 픽스처 기반 파싱 테스트가 CI에서 돈다
- [x] 모든 타임스탬프가 tz-aware이며 컨테이너 TZ에 의존하지 않는다

**전달 보증**
- [x] 채널별 발송 결과가 저장·집계되고 쿨다운을 전진시키지 않는다 (자동 재시도는 미구현 — 다음 회차에 재발송)
- [ ] 오프셋은 처리 성공 후 커밋되고 실패 메시지는 DLQ로 간다
- [x] 중복 방지 키(`event_id`)가 있고 OpenSearch는 upsert다
- [ ] 무알림이 정상인지 고장인지 하트비트로 구분된다

**상태 영속성**
- [ ] Kafka 로그·OpenSearch 데이터·Airflow 메타DB가 각각 명명 볼륨에 있다 (Kafka·OpenSearch 완료, **Airflow 메타DB 남음** — #47)
- [x] 인덱스 보존 정책(ISM 90일)이 설정돼 있다 (로그 보존 정책은 #47)
- [ ] 디스크 사용률 알람이 있다

**오케스트레이션**
- [ ] 메타DB가 PostgreSQL이고 executor가 SequentialExecutor가 아니다
- [ ] 이미지 의존성이 버전 고정돼 있고 런타임에 pip install 하지 않는다
- [x] DAG 실패가 사람에게 통보된다 (Slack 실패 콜백)

**보안**
- [ ] 시크릿이 평문 환경변수가 아니라 시크릿 매니저로 주입된다
- [x] 로그·예외 메시지에 키·토큰이 남지 않는다 (마스킹 완료, CI 스캐닝은 #23)
- [ ] OpenSearch 인증/TLS, Airflow Fernet 키, Kafka UI 인증이 활성화돼 있다
- [ ] 관리 포트가 공개 인터페이스에 바인딩되지 않는다
- [x] 카카오 토큰 회전 값이 자동 저장되고 만료 임박 경고가 있다
- [ ] 컨테이너가 non-root로 실행된다

**관측성 · 운영**
- [ ] 위 6절의 메트릭·알람 세트가 배포돼 있다
- [ ] 컨슈머에 healthcheck가 있고 정상 종료 루프가 제거됐다
- [ ] 런북이 있다 — API 장애 시, 채널 인증 만료 시, OpenSearch red 시, 백로그 폭증 시
- [ ] 부하 상한이 측정돼 있다

---

## 이 문서 갱신 방법

새 결함이 발견되면 해당 절에 [트리거 → 현재 동작 → 영향 → 개선]과 파일·라인 근거를 추가하고, 이슈를 열어 링크합니다. 이슈가 닫히면 체크리스트 항목을 체크하고 「개선 순서」 표를 갱신합니다. 진행 현황 요약은 [dashboard.html](dashboard.html)에서 함께 관리합니다.
