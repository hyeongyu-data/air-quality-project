# ADR-0007. Airflow 메타DB PostgreSQL·LocalExecutor 전환

- 상태: 채택 (2026-09-11)
- 관련 이슈: [#119](https://github.com/hyeongyu-data/air-quality-project/issues/119)
- 관련: [#47/#106](docs/../DEVELOPMENT_PLAN.md)(메타DB 볼륨 영속화), [ADR-0005](0005-aws-migration-path.md)

## 맥락

Airflow가 `SequentialExecutor` + `sqlite:////opt/airflow/airflow.db`로 동작했다.

- **SequentialExecutor**: 태스크가 스케줄러를 블록한다. 한 태스크가 느려지면
  스케줄이 밀리고, `data_interval_end` 기준으로 수집 모드를 고르는 로직이 실제
  시각과 어긋날 수 있다.
- **SQLite + `airflow standalone`**: 웹서버·스케줄러·트리거러가 한 파일을 공유해
  `database is locked` 가 상존한다. 백필·다중 DAG 로 가면 병목.
- 메타DB 백업·복구 절차가 없었다(볼륨 손상 = DAG 상태·이력 전멸).

메타DB 볼륨 영속화(#106), `_PIP_ADDITIONAL_REQUIREMENTS` 버전 고정(#88)은 완료.

## 결정

**PostgreSQL 16 + LocalExecutor 로 전환한다(dev·prod 모두).**

- **PostgreSQL**: `postgres:16-alpine` 전용 서비스·볼륨(`airflow_pg_data`).
  `apache/airflow:2.10.0` 이미지에 `psycopg2 2.9.9` 가 이미 있어 의존성 추가 없음.
  dev·prod 이중 백엔드를 유지하지 않는다 — 검증·문서가 갈라진다.
- **LocalExecutor**: 워커 컨테이너 없이 스케줄러가 태스크를 별도 프로세스로
  병렬 실행한다. 현재 워크로드(하루 4회 스케줄·선형 4태스크)에 충분하고 추가
  인프라가 없다.
- **`airflow standalone` 유지**: 단일 컨테이너에서 웹서버+스케줄러+트리거러.
  LocalExecutor 는 별도 워커가 없으므로 이 모델과 맞는다. 실제 부팅으로
  standalone + Postgres + LocalExecutor 조합이 동작하고 태스크가 병렬 실행되며
  `database is locked` 가 사라짐을 확인했다.
- 병렬성: `PARALLELISM=8`, `MAX_ACTIVE_TASKS_PER_DAG=4`, `MAX_ACTIVE_RUNS_PER_DAG=1`
  (수집 run 겹침 방지 — DAG 는 `catchup=False`).

## 대안과 기각 이유

| 대안 | 기각 이유 |
| --- | --- |
| CeleryExecutor | Redis/RabbitMQ + 워커 컨테이너가 필요. 현재 규모에 과하다. DAG 복잡도·처리량이 커질 때 별도 이슈 |
| KubernetesExecutor | K8s 클러스터 전제 |
| webserver·scheduler·triggerer 컨테이너 분리 | "제대로 된" 구성이지만 배포 구조 개편 범위. standalone 단일 컨테이너로 충분 |
| SQLite 유지 + WAL 모드 | 동시 쓰기 한계는 그대로. `airflow standalone` 의 다중 프로세스 문제를 못 푼다 |
| MWAA 등 관리형 | 클라우드 배포 이슈 ([ADR-0005](0005-aws-migration-path.md)) |

## 마이그레이션

기존 `airflow_home` 의 SQLite `airflow.db` 는 전환 시 **버려진다**(Postgres 에
새 메타DB). DAG 코드는 마운트 그대로라 DAG 는 자동 재등록되지만, 실행 이력·
수동 설정은 유실된다. 이 프로젝트는 Variable/Connection 을 쓰지 않아 실질 영향은
실행 이력뿐. 전환 전 `airflow_home/airflow.db` 를 복사해 두는 것을 권고.

`airflow_home` 볼륨은 유지한다 — 로그·생성된 `airflow.cfg`·`webserver_config.py`·
`standalone_admin_password.txt` 를 보존한다. 볼륨 안의 낡은 `airflow.db` 는
무해하지만 지저분하니 재-`up` 전에 지워도 된다(env 가 `airflow.cfg` 를 덮어써서
동작에는 영향 없음).

## 자격증명 (운영 프로필)

- `postgres` 서비스: `POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password`.
  base 의 평문 `POSTGRES_PASSWORD` 는 오버레이가 `!reset null`(이미지가 평문과
  `_FILE` 동시 설정을 거부).
- `airflow`: 이미지 엔트리포인트가 컨테이너 `command:` 보다 먼저 DB 에 붙으므로
  `command:` export 로는 늦다. `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` 을
  `!reset null` 하고 `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD: "cat /run/secrets/airflow_db_conn"`
  로 읽는다. `_CMD` 는 `shlex.split` 되므로 명령 치환은 못 쓴다 — 파일에 전체
  DSN 을 그대로 둔다. `secrets/postgres_password` 와 같은 비밀번호여야 한다.
- `!reset` 은 Docker Compose ≥ 2.24 필요.

## 재검토 조건

- DAG 의존 그래프가 복잡해지거나(팬아웃·동적 태스크) 처리량이 커져 LocalExecutor
  의 단일 노드 병렬성이 병목일 때 → CeleryExecutor.
- 스케줄러 HA(다중 스케줄러)가 필요할 때 → Postgres 는 이미 준비됨, executor·배포만.
- 클라우드로 옮길 때 → [ADR-0005](0005-aws-migration-path.md) (EventBridge + Lambda,
  Airflow 를 안 쓰는 경로).
