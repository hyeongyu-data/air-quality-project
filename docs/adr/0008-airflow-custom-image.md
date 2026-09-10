# ADR-0008. Airflow 런타임 pip 설치 제거 — 커스텀 이미지

- 상태: 채택 (2026-09-11)
- 관련 이슈: [#120](https://github.com/hyeongyu-data/air-quality-project/issues/120)
- 관련: [#88](../DEVELOPMENT_PLAN.md)(버전 고정), [ADR-0007](0007-airflow-postgres-localexecutor.md)

## 맥락

Airflow 서비스가 `_PIP_ADDITIONAL_REQUIREMENTS` 로 컨테이너 시작마다 4개
패키지를 PyPI 에서 설치했다.

- 시작이 느리고(매 기동 pip 해석·설치), PyPI 장애 시 **기동 실패**.
- Airflow 공식 문서가 이 방식을 dev 전용으로 명시.
- Consumer 는 이미 `Dockerfile` 로 빌드 시 설치. Airflow 만 런타임.

## 결정

**`Dockerfile.airflow` 로 빌드 시 설치한다.**

```dockerfile
FROM apache/airflow:2.10.0
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
```

- `docker-compose.yaml` airflow 서비스: `image: apache/airflow:2.10.0` →
  `build: {context: ., dockerfile: Dockerfile.airflow}` + `image: air-quality-airflow:local`.
  `_PIP_ADDITIONAL_REQUIREMENTS` 제거.
- `requirements-airflow.txt` = `requests` · `kafka-python` · `python-dotenv` 3개.
  **`opensearch-py` 는 뺐다** — `dags/`·`producer/` 트리에 `opensearchpy` import 가
  없다(consumer 전용). `opensearch-py==3.2.0` 은 `grpcio`·`protobuf`·
  `opensearch-protobufs` 까지 전이 의존으로 끌고 온다.
- **`--constraint` 는 안 건다**: Airflow 2.10 제약 파일(2024-08)이
  `requests==2.32.3`·`python-dotenv==1.0.1` 로 고정해 이 저장소의 최신 핀과
  충돌한다(빌드 실패 확인). 직접 의존 3개를 `==` 로 고정하고 base 이미지를
  태그로 고정하는 것으로 결정성을 확보한다. 완전한 전이 의존 lock 이 필요하면
  빌드된 이미지를 `pip freeze` 해 커밋 — 이번 범위 밖.
- 공유 패키지(requests·kafka-python 등) 버전이 `requirements-consumer.txt`·
  `requirements.txt` 와 갈리지 않게 `tests/test_requirements_consistency.py` 가 강제.

## 코드 배치

`dags/`·`producer/`·`consumer/` 는 **바인드 마운트 유지**한다 — dev 반복(핫
리로드). 이미지에는 pip 의존성만 굳힌다.

완전히 봉인된 배포 이미지가 필요하면 `COPY producer consumer` + 마운트 제거가
후속 단계다. localhost·포트폴리오 범위에선 이득이 낮고, dev 편의를 깎는다.
재검토: 실제 배포(레지스트리에 푸시하는 이미지)를 만들 때.

## 검증

- `docker build -f Dockerfile.airflow` → 성공, `pip check` clean.
- `--network none` 에서 `airflow db migrate` 완주 — 런타임에 PyPI 를 안 친다.
- DagBag 파싱(`import_errors: {}`), dev 부팅 로그에 pip 설치 라인 0.
- CI: 이미지 빌드 + dep import + `pip check` + DagBag 파싱 + hadolint.

## 대안과 기각 이유

| 대안 | 기각 이유 |
| --- | --- |
| `_PIP_ADDITIONAL_REQUIREMENTS` 유지 | 근본 문제(매 기동 설치·PyPI 의존)가 그대로 |
| `--constraint` + 최신 핀 | 제약 파일과 충돌(빌드 실패) |
| 제약 파일에 핀 맞추기 | 저장소 전체의 버전 정책을 2024 스냅샷에 묶는다 |
| 프라이빗 PyPI 미러 | 인프라 추가. 이 규모에 과함 |
| 멀티스테이지·크기 최적화 | 후속. 지금은 런타임 설치 제거가 목표 |

## 재검토 조건

- CI 이미지 빌드가 병목이 되면 → GHA 레이어 캐시(`type=gha`).
- 전이 의존성 재현성이 실제 문제를 일으키면 → `pip freeze` lock.
- 배포 이미지를 레지스트리에 푸시하게 되면 → 코드 `COPY`, 마운트 제거.
