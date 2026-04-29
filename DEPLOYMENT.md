# 🚀 Docker Compose 배포 및 검증 가이드

기상지수 알림 시스템을 Docker Compose로 배포하고 검증하는 상세 가이드입니다.

---

## 📋 목차

- [사전 준비](#사전-준비)
- [빠른 시작](#빠른-시작)
- [서비스별 검증](#서비스별-검증)
- [통합 테스트](#통합-테스트)
- [모니터링](#모니터링)
- [문제 해결](#문제-해결)
- [정리 및 정지](#정리-및-정지)

---

## 🔧 사전 준비

### 필수 설치

```bash
# Docker & Docker Compose 버전 확인
docker --version
docker-compose --version

# 권장 버전
# Docker: 20.10+
# Docker Compose: 2.0+
```

### 필수 파일 확인

```bash
# 프로젝트 루트에 다음 파일들이 있는지 확인
ls -la
# .env (환경변수)
# docker-compose.yaml (서비스 정의)
# Dockerfile (Consumer 이미지)
# requirements.txt (Python 패키지)
# dags/ (Airflow DAG)
# producer/ (Producer 코드)
# consumer/ (Consumer 코드)
```

### 환경변수 설정

```bash
# .env 파일 생성 및 확인
cat .env

# 필수 환경변수 확인
grep -E "WEATHER_API_KEY|AIRKOREA_API_KEY|KAFKA_BOOTSTRAP_SERVERS" .env

# 없으면 추가
echo "WEATHER_API_KEY=your_key" >> .env
echo "AIRKOREA_API_KEY=your_key" >> .env
echo "KAFKA_BOOTSTRAP_SERVERS=kafka:9092" >> .env
```

---

## 🚀 빠른 시작

### 1단계: 모든 서비스 시작

```bash
# 컨테이너 빌드 및 시작 (백그라운드)
docker-compose up -d

# 또는 로그를 보면서 시작
docker-compose up
```

### 2단계: 서비스 상태 확인

```bash
# 컨테이너 상태 확인
docker-compose ps

# 예상 결과:
# NAME                 COMMAND                  SERVICE      STATUS
# pj-airflow          "airflow standalone"     airflow      Up (healthy)
# pj-kafka            "/opt/kafka/bin/k..."   kafka        Up (healthy)
# pj-consumer         "python consumer/..."   consumer     Up
# pj-opensearch       "opensearch"             opensearch   Up (healthy)
# pj-kafka-ui         "sh /docker-entryp..."  kafka-ui     Up
```

### 3단계: 웹 UI 접속

| 서비스 | URL | 설명 |
|---|---|---|
| **Airflow** | http://localhost:8080 | 워크플로우 관리 (admin/admin) |
| **Kafka UI** | http://localhost:8081 | Kafka 모니터링 |
| **OpenSearch** | http://localhost:9200 | REST API (curl로 접근) |

---

## ✅ 서비스별 검증

### Kafka 검증

```bash
# 1. 컨테이너 실행 확인
docker-compose exec kafka \
  kafka-broker-api-versions.sh \
  --bootstrap-server=kafka:9092

# 2. 토픽 목록 확인
docker-compose exec kafka \
  kafka-topics.sh \
  --bootstrap-server=kafka:9092 \
  --list

# 3. 토픽 생성 (자동 생성되지만 명시적으로)
docker-compose exec kafka \
  kafka-topics.sh \
  --bootstrap-server=kafka:9092 \
  --create \
  --topic air-quality \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

docker-compose exec kafka \
  kafka-topics.sh \
  --bootstrap-server=kafka:9092 \
  --create \
  --topic health-index \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

docker-compose exec kafka \
  kafka-topics.sh \
  --bootstrap-server=kafka:9092 \
  --create \
  --topic uv-index \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

# 4. 토픽 상세 정보 확인
docker-compose exec kafka \
  kafka-topics.sh \
  --bootstrap-server=kafka:9092 \
  --describe \
  --topic air-quality
```

**예상 결과:**
```
Topic: air-quality	TopicId: XXX	PartitionCount: 1	ReplicationFactor: 1
	Topic: air-quality	Partition: 0	Leader: 1	Replicas: [1]	Isr: [1]
```

### Airflow 검증

```bash
# 1. 웹 UI 접속
# http://localhost:8080
# Username: admin
# Password: admin

# 2. DAG 파일 검증 (컨테이너 내부)
docker-compose exec airflow \
  airflow dags list

# 예상 결과:
# dag_id                   | owner   | last_scheduler_run | last_run          | next_run          | # tasks | state
# realtime_weather_alert   | airflow | [timestamp]        | [timestamp]       | [timestamp]       | 6       | active

# 3. DAG 구문 검증
docker-compose exec airflow \
  airflow dags test realtime_weather_alert 2026-04-28

# 4. 특정 Task 실행 (테스트)
docker-compose exec airflow \
  airflow tasks test realtime_weather_alert validate_environment 2026-04-28
```

### OpenSearch 검증

```bash
# 1. 클러스터 상태 확인
curl -X GET "http://localhost:9200/_cluster/health?pretty"

# 예상 결과:
# {
#   "cluster_name" : "opensearch-cluster",
#   "status" : "green",
#   "timed_out" : false,
#   "number_of_nodes" : 1,
#   "number_of_data_nodes" : 1,
#   "active_primary_shards" : 0,
#   "active_shards" : 0,
#   "relocating_shards" : 0,
#   "initializing_shards" : 0,
#   "unassigned_shards" : 0,
#   "delayed_unassigned_shards" : 0,
#   "number_of_pending_tasks" : 0,
#   "number_of_in_flight_fetch" : 0,
#   "task_max_waiting_in_queue_millis" : 0,
#   "active_shards_percent_as_number" : -1.0
# }

# 2. 인덱스 목록 확인
curl -X GET "http://localhost:9200/_cat/indices?v"

# 3. 특정 인덱스 검색
curl -X GET "http://localhost:9200/weather-alert-*/_search?pretty" | head -50
```

### Consumer 검증

```bash
# 1. 컨테이너 로그 확인
docker-compose logs -f consumer

# 예상 로그:
# consumer_1  | 2026-04-28 10:30:00 - __main__ - INFO - ===================
# consumer_1  | 2026-04-28 10:30:00 - __main__ - INFO - 기상지수 알림 컨슈머 테스트
# consumer_1  | 2026-04-28 10:30:00 - __main__ - INFO - ===================

# 2. 컨테이너 상태 확인
docker-compose ps consumer

# 3. 컨테이너 내부 접속 (디버깅)
docker-compose exec consumer bash

# 컨테이너 내부에서:
# python -c "from consumer.consumer import WeatherAlertConsumer; print('✅ Import OK')"
# exit
```

---

## 🔄 통합 테스트

### 테스트 1: Producer → Kafka → Consumer 흐름

```bash
# 1. Consumer 로그 모니터링 (별도 터미널)
docker-compose logs -f consumer

# 2. Producer 실행 (별도 터미널)
docker-compose exec airflow \
  python /opt/airflow/producer/producer.py

# 3. Kafka UI에서 메시지 확인
# http://localhost:8081
# → Topics → air-quality → Messages 탭

# 4. Consumer 로그에서 처리 확인
# "메시지 수신: {...}"
# "메시지 처리 완료: ..."
```

### 테스트 2: Airflow DAG 실행

```bash
# 1. Airflow UI 접속
# http://localhost:8080

# 2. "realtime_weather_alert" DAG 찾기

# 3. 우측 상단 "Trigger DAG" 클릭

# 4. 로그에서 실행 결과 확인
docker-compose logs -f airflow

# 5. 각 Task의 로그 확인
# Airflow UI → DAG Run → Task Instance → Log
```

### 테스트 3: OpenSearch 데이터 확인

```bash
# 1. 데이터 저장 확인
curl -X GET "http://localhost:9200/weather-alert-*/_count"

# 예상 결과:
# {"count":10,"_shards":{"total":5,"successful":5,"skipped":0,"failed":0}}

# 2. 최근 문서 조회
curl -X GET "http://localhost:9200/weather-alert-*/_search?size=1&pretty" \
  | jq '.hits.hits[0]'

# 3. 지역별 검색
curl -X POST "http://localhost:9200/weather-alert-*/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d'
{
  "query": {
    "match": {
      "region": "서울"
    }
  }
}'
```

---

## 📊 모니터링

### 실시간 로그 모니터링

```bash
# 모든 서비스 로그 (병합)
docker-compose logs -f

# 특정 서비스만
docker-compose logs -f [service-name]

# 마지막 100줄부터 시작
docker-compose logs -f --tail=100 consumer

# 타임스탬프 포함
docker-compose logs -f -t consumer
```

### 리소스 사용량 확인

```bash
# Docker 통계
docker stats

# 특정 컨테이너만
docker stats pj-consumer pj-kafka pj-opensearch

# 계속 갱신 (Ctrl+C로 종료)
watch -n 1 'docker stats --no-stream'
```

### 네트워크 테스트

```bash
# Container 간 연결 테스트
docker-compose exec consumer \
  nc -zv kafka 9092

docker-compose exec consumer \
  curl http://opensearch:9200/_cluster/health

docker-compose exec airflow \
  nc -zv kafka 9092
```

---

## 🐛 문제 해결

### 컨테이너가 시작되지 않음

```bash
# 1. 로그 확인
docker-compose logs [service-name]

# 2. 이미지 재빌드
docker-compose build --no-cache consumer

# 3. 서비스 재시작
docker-compose restart [service-name]

# 4. 처음부터 시작
docker-compose down -v
docker-compose up -d
```

### 포트 충돌

```bash
# 포트 사용 중인 프로세스 확인
lsof -i :8080
lsof -i :9092
lsof -i :9200

# 포트 변경 (docker-compose.yaml의 ports 수정)
# - "8080:8080" → "8081:8080"
```

### 메모리 부족

```bash
# Docker 메모리 설정 확인
docker info | grep -i memory

# 할당량 증가 (Docker Desktop 설정)
# Preferences → Resources → Memory 증가

# 또는 docker-compose.yaml의 deploy.resources 수정
services:
  consumer:
    deploy:
      resources:
        limits:
          memory: 1G
```

### 환경변수 로드 실패

```bash
# .env 파일 확인
cat .env

# 특정 변수 확인
docker-compose exec [service] \
  printenv | grep KAFKA

# 볼륨 마운트 확인 (Dockerfile)
docker-compose exec airflow \
  cat /opt/airflow/.env
```

---

## 🧹 정리 및 정지

### 서비스 중지 (데이터 유지)

```bash
# 모든 컨테이너 중지
docker-compose stop

# 특정 서비스만 중지
docker-compose stop consumer

# 컨테이너 재시작
docker-compose start

# 서비스 재시작
docker-compose restart consumer
```

### 완전 정리 (데이터 삭제)

```bash
# 컨테이너 및 네트워크 삭제 (볼륨 유지)
docker-compose down

# 컨테이너, 네트워크, 볼륨 모두 삭제
docker-compose down -v

# 이미지도 삭제
docker-compose down -v --rmi all
```

### 특정 컨테이너 정리

```bash
# 컨테이너 삭제
docker-compose rm consumer

# 강제 삭제
docker-compose rm -f consumer

# 이미지만 삭제
docker rmi pj-consumer:latest
```

---

## 📋 체크리스트

배포 후 다음 사항을 확인하세요:

- [ ] `docker-compose ps`에서 모든 서비스가 "Up" 상태
- [ ] Airflow UI (localhost:8080) 접속 가능
- [ ] Kafka UI (localhost:8081) 접속 가능
- [ ] OpenSearch (localhost:9200) 응답 확인
- [ ] Kafka 토픽 3개 존재 (air-quality, health-index, uv-index)
- [ ] Consumer 로그에서 메시지 처리 확인
- [ ] OpenSearch에 데이터 저장 확인
- [ ] Airflow DAG이 정상적으로 표시됨
- [ ] 환경변수 로드 확인
- [ ] 헬스체크 통과 (healthy 상태)

---

## 🔍 상세 로그 분석

### Kafka 로그

```bash
docker-compose logs kafka | grep -E "ERROR|WARN|INFO"
```

**정상 메시지:**
- `Started AdminServer on address 0.0.0.0 and port 9093`
- `Partition metadata updated`

**오류 메시지:**
- `Failed to update metadata`
- `Connection refused`

### Airflow 로그

```bash
docker-compose logs airflow | tail -20
```

**정상 메시지:**
- `Scheduler started`
- `Serving on http://`

### Consumer 로그

```bash
docker-compose logs consumer | grep -A 5 "메시지 수신"
```

**정상 메시지:**
- `메시지 수신: {...}`
- `메시지 처리 완료`
- `콘솔 알림 발송 성공`

---

## 📞 지원

문제 발생 시:

1. README.md의 트러블슈팅 섹션 참조
2. 로그 확인: `docker-compose logs -f [service]`
3. 헬스체크: `docker-compose exec [service] [healthcheck command]`
4. 네트워크 테스트: `docker network inspect weather-network`

---

**마지막 업데이트:** 2026-04-28
