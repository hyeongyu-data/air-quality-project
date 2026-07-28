# ADR-0001. 센서 로그 메달리언 파이프라인 제거

- 상태: 채택 (2026-07-29)
- 관련 이슈: [#32](https://github.com/hyeongyu-data/air-quality-project/issues/32)

## 맥락

이 저장소의 본체는 서울 기상·대기질 알림 시스템(Airflow → Kafka → Consumer → OpenSearch/알림)이다. 그와 별개로 센서 로그 메달리언 예제가 함께 있었다.

```
log_generator → Fluent Bit / Logstash → Kafka → Vector → Kinesis Firehose
              → S3 Bronze → Glue Silver → Glue Gold
```

이 구성을 유지할지 판단하기 위해 실동작 여부, 본체와의 연결성, 유지 비용을 확인했다.

## 확인한 사실

**1. 로컬에서 검증 가능한 구간은 Kafka까지다.** README도 "로컬에서 먼저 Kafka까지 확인합니다"라고 스스로 경계를 그어놓았다. Bronze 이후는 AWS 자원 없이 실행할 수 없는데, Firehose·S3·Glue Job을 만들 IaC(Terraform/CloudFormation/CDK)가 저장소에 하나도 없다. 제3자는 재현할 수단이 없고, 작성자도 콘솔 수작업 기억에 의존해야 한다.

**2. CI가 이 코드를 보지 않는다.** `.github/workflows/ci.yml`은 `compileall producer consumer dags scripts`를 돌린다. `glue_jobs`가 빠져 있어 **문법 검사조차 받지 않았다.** 테스트도 0건이고, `awsglue`는 PyPI에 없어 로컬 import 자체가 불가능하다.

**3. 실행하면 두 번째부터 깨진다.** 두 Glue Job 모두 `recursiveFileLookup=true`로 전체를 재스캔한 뒤 `.mode("append")`로 쓴다. Job Bookmark 설정이 없고 `dropDuplicates`는 배치 내부만 커버한다. 즉 **2회 실행하면 Silver·Gold가 통째로 중복 적재되고, Gold의 `event_count`는 실행 횟수만큼 부풀려진다.** 메달리언의 핵심 가치인 재처리 안전성이 성립하지 않는다. 미사용 `TimestampType` import가 남아 있는 것도 한 번도 실행·린트되지 않았음을 보여준다.

**4. 본체와 데이터가 이어지지 않는다.** 공유하는 것은 Kafka 컨테이너와 도커 네트워크뿐이다.

| | 본체 | 메달리언 |
| --- | --- | --- |
| 토픽 | `seoul-weather` | `sensor-json-logs`, `sensor-text-logs` |
| 데이터 | 기상청·에어코리아 실 API | `random.uniform()` 가짜값 |
| 소비자 | Consumer → rules → OpenSearch/알림 | 없음 |

**5. 본체 실행에 실제 부작용이 있었다.** `vector`에만 `profiles: [aws]`가 있고 `log-generator`/`logstash`/`fluent-bit`에는 없었다. README가 안내하는 `docker compose up -d --build`가 본체 데모 때마다 Logstash JVM(256MB) + Fluent Bit + 무한 로그 생성기를 함께 띄웠다.

**6. 문서가 이미 어긋나 있었다.** 기술 스택 표에 Fluent Bit·Logstash·Vector가 없고, 주요 파일 트리에도 해당 디렉터리가 빠져 있었다. 유지되지 않는 부속물이라는 신호다.

## 검토한 대안

| | 장점 | 단점 |
| --- | --- | --- |
| **(A) 전부 삭제** | 본체 집중, 검증 불가 코드 제거, 데모 부작용 해소 | 메달리언이라는 키워드 상실 |
| (B) 기상 데이터를 로컬 메달리언(DuckDB/Parquet)으로 통합 | "끝까지 도는 메달리언" 실증 | **데이터율이 시간당 1건**이라 Gold 시간별 집계가 그룹당 1행. 집계할 것이 없다. OpenSearch가 이미 이력 저장을 하고 있어 새로 얻는 역량도 없다 |
| (C) 별도 저장소로 분리 | 작업물 보존 | 문제를 고치지 않고 옮기기만 한다. 검증 불가 상태인 저장소가 하나 더 생긴다 |

## 결정

**(A) 전부 삭제한다.**

(B)의 유일한 매력은 "실제로 도는 메달리언"인데, 이 데이터율에서 Bronze/Silver/Gold 3계층은 기능이 아니라 장식이다. 없는 문제를 위해 인프라를 짓는 것이므로 채택하지 않는다. (C)는 문제를 이전할 뿐이다.

AWS 설계 의도는 README의 "AWS 이전 방향" 섹션에 코드 주장 없이 설계로만 남긴다. 검증할 수 없는 코드를 저장소에 두지 않는다는 판단 자체를, 그 근거와 함께 기록하는 것이 이 문서의 목적이다.

## 트레이드오프

- 잃는 것: Fluent Bit·Logstash·Vector·Glue를 설정 수준에서 다뤄봤다는 가시적 증거. 다만 git 히스토리(`9ffbe5f`)에 남아 있어 필요하면 언제든 참조할 수 있다.
- 얻는 것: 저장소의 모든 코드가 로컬에서 실행·검증 가능한 상태. 본체 데모 시 불필요한 컨테이너 3개가 뜨지 않는다.

## 재검토 조건

다음 중 하나가 성립하면 이 결정을 다시 본다.

1. 본체 데이터율이 **일 1만 건 이상**으로 올라 OpenSearch 질의로 답이 안 나오는 분석 요구가 생겼을 때
2. Firehose·S3·Glue를 **IaC로 정의**하고 CI에서 검증할 수 있게 됐을 때
3. 기상 데이터 자체를 Bronze로 적재해 본체와 **데이터가 실제로 이어지는** 설계가 준비됐을 때

재도입 시에는 PySpark/Glue가 아니라 로컬에서 완결 검증 가능한 최소 구성(Parquet + DuckDB, Airflow 태스크 2개, 회귀 테스트 1개)부터 시작한다.
