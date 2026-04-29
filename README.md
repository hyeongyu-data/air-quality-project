# 🌍 기상지수 실시간 알림 시스템

공공 API(기상청, 에어코리아)에서 실시간 기상지수 예측 데이터를 수집하고,  
임계값 기반으로 정상/주의/경고/위험을 판정한 후,  
행동 가이드와 함께 실시간 알림을 발송하는 **엔드-투-엔드 데이터 엔지니어링 프로젝트**입니다.

---

## 📋 목차

- [프로젝트 개요](#프로젝트-개요)
- [기술 스택](#기술-스택)
- [시스템 아키텍처](#시스템-아키텍처)
- [설치 및 실행](#설치-및-실행)
- [API 키 발급](#api-키-발급)
- [파일 구조](#파일-구조)
- [데이터 흐름](#데이터-흐름)
- [알림 규칙](#알림-규칙)
- [개발 및 테스트](#개발-및-테스트)
- [트러블슈팅](#트러블슈팅)

---

## 🎯 프로젝트 개요

### 문제 정의

기상지수(미세먼지, 자외선, 감기위험 등)는 매일 변하지만,  
사람들은 언제 마스크를 쓰고, 언제 외출을 자제해야 하는지 알기 어렵습니다.

### 솔루션

**예측값 기반 행동 가이드:**
- ✅ 공공 API의 예측 지수를 그대로 활용 (ML 모델 불필요)
- ✅ 임계값으로 "정상" / "주의" / "경고" / "위험" 4단계 판정
- ✅ 각 등급에 맞는 **구체적인 행동** 제시
  - 미세먼지 나쁨 → "KF94 마스크 착용 필수"
  - 자외선 높음 → "선크림 SPF50+ + 자외선 차단 의류"
  - 감기위험 높음 → "손씻기 강화 + 사람 많은 곳 피하기"
- ✅ 콘솔, Slack, OpenSearch 등 다양한 채널 알림
- ✅ 매시간 자동 수집 및 처리 (Airflow DAG)

---

## 🛠️ 기술 스택

| 계층 | 기술 | 역할 |
|---|---|---|
| **스케줄링** | Apache Airflow 2.7 | 매시간 자동 데이터 수집 |
| **메시징** | Apache Kafka 3.7 | 실시간 데이터 스트리밍 |
| **처리** | Python 3.10 | 데이터 파싱, 판정, 알림 |
| **저장소** | OpenSearch 2.11 | 알림 이력 저장 및 검색 |
| **컨테이너** | Docker + Docker Compose | 개발/배포 환경 |
| **라이브러리** | pandas, requests, kafka-python, opensearch-py | 데이터 처리 및 통합 |

---

## 🏗️ 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                      Day 1 (수집 & 발행)                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  기상청 API      에어코리아 API                               │
│      │                │                                      │
│      └────────┬───────┘                                      │
│               │                                              │
│        [Producer] (5단계)                                     │
│        - API 호출                                             │
│        - 데이터 파싱                                          │
│        - Kafka 발행                                          │
│               │                                              │
│      ┌────────┼────────┐                                     │
│      │        │        │                                     │
│   [air-quality] [health-index] [uv-index]                    │
│    (Kafka Topics)                                            │
│                                                              │
│  🔁 매시간 자동 반복 (Airflow DAG)                           │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      Day 2 (처리 & 알림)                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│      [Consumer] (6단계)                                      │
│      - Kafka 구독                                            │
│      - rules.py로 판정                                       │
│      - AlertGrouping으로 분류                                │
│               │                                              │
│      [Alert Manager] (4단계)                                 │
│               │                                              │
│      ┌────────┼────────┬─────────────┐                      │
│      │        │        │             │                      │
│   [콘솔]   [Slack]  [OpenSearch]  [Dashboard]               │
│                                                              │
│  🔁 지속 실행 (Docker Container)                             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 설치 및 실행

### 필수 사항

- Docker & Docker Compose
- Python 3.10+ (로컬 테스트 시)
- 기상청 + 에어코리아 API 키

### 1단계: 저장소 클론 및 환경 설정

```bash
# 프로젝트 디렉토리 생성
mkdir air-quality-project && cd air-quality-project

# 파일 구조
# air-quality-project/
# ├── .env
# ├── docker-compose.yaml
# ├── Dockerfile
# ├── requirements.txt
# ├── dags/
# │   ├── __init__.py
# │   └── air_pipeline.py
# ├── producer/
# │   ├── __init__.py
# │   └── producer.py
# └── consumer/
#     ├── __init__.py
#     ├── consumer.py
#     ├── rules.py
#     └── alert.py
```

### 2단계: 환경변수 설정

`.env` 파일을 프로젝트 루트에 생성하고 다음을 입력하세요:

```env
# ====== API 설정 ======
WEATHER_API_KEY=your_weather_api_key_here
AIRKOREA_API_KEY=your_airkorea_api_key_here

# ====== Kafka 설정 ======
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# ====== OpenSearch 설정 ======
OPENSEARCH_HOST=opensearch
OPENSEARCH_PORT=9200
OPENSEARCH_INDEX_PREFIX=weather-alert

# ====== Airflow 설정 ======
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__FERNET_KEY=

# ====== 알림 설정 ======
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/your/webhook/url
SLACK_ENABLED=false

# ====== 로깅 ======
LOG_LEVEL=INFO
```

**⚠️ 주의:**
- `.env` 파일을 Git에 올리지 마세요 (`.gitignore`에 추가)
- API 키는 실제 값으로 교체하세요
- Slack 알림을 사용하려면 Webhook URL을 설정하세요

### 3단계: Docker Compose로 시작

```bash
# 모든 서비스 시작
docker-compose up -d

# 서비스 상태 확인
docker-compose ps

# 로그 확인
docker-compose logs -f

# 특정 서비스 로그
docker-compose logs -f airflow
docker-compose logs -f kafka
docker-compose logs -f consumer
```

### 4단계: Airflow DAG 배포

```bash
# Airflow 웹 UI 접속
http://localhost:8080

# 로그인 (기본값)
# User: admin
# Password: admin

# DAG 확인
# 왼쪽 메뉴에서 "realtime_weather_alert" DAG 확인
# 수동 실행 또는 스케줄 대기
```

### 5단계: 데이터 수집 확인

```bash
# Kafka 토픽 확인
docker-compose exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

# 메시지 확인
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic air-quality \
  --from-beginning
```

---

## 🔑 API 키 발급

### 기상청 공공데이터 API

1. **포털 접속**
   - https://www.data.go.kr/ 방문
   - 회원가입 (무료)

2. **API 키 발급**
   - "마이페이지" → "개인정보 수정" → "API KEY" 탭
   - API 키 발급 (즉시, 이메일 도착)

3. **이용 신청** (필수)
   - 검색: "기상청 생활기상지수"
   - "이용 신청" 버튼 클릭 (승인 대기 2-3시간)
   - 이용약관 동의 후 신청

4. **API 문서**
   - https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15089055
   - 활용 사례 보기 → REST API

### 에어코리아 (한국환경공단) API

1. **포털 접속**
   - https://www.airkorea.or.kr/web/pm25Guide?pMENU_NO=102 접속

2. **API 키 발급**
   - https://www.data.go.kr/ → "에어코리아" 검색
   - API 키 발급 (즉시)

3. **이용 신청**
   - "이용 신청" 클릭
   - 이용약관 동의 (즉시 승인)

4. **API 문서**
   - https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15000581

**⏱️ 발급 시간:**
- 에어코리아: 즉시 사용 가능 (권장)
- 기상청: 2-3시간 이내 활성화

**테스트 하기:**
```bash
# API 키를 .env에 설정한 후 로컬 테스트
python producer/producer.py

# 샘플 데이터 출력 → Kafka 발행 성공 확인
```

---

## 📂 파일 구조

```
air-quality-project/
│
├── .env                          # 환경변수 (Git 무시)
├── .gitignore                    # .env, __pycache__ 등 무시
├── requirements.txt              # Python 패키지
│   ├── kafka-python
│   ├── requests
│   ├── opensearch-py
│   ├── pandas
│   └── python-dotenv
│
├── Dockerfile                    # Consumer 컨테이너 이미지
├── docker-compose.yaml           # 전체 인프라 정의
│   ├── Kafka
│   ├── Airflow
│   ├── OpenSearch
│   ├── Kafka-UI (모니터링)
│   └── Consumer (업무 처리)
│
├── dags/                         # Airflow DAG
│   ├── __init__.py
│   └── air_pipeline.py          # 📅 7단계: 매시간 자동 수집
│
├── producer/                     # 데이터 수집 & 발행
│   ├── __init__.py
│   └── producer.py              # 📡 5단계: API 호출 → Kafka
│
├── consumer/                     # 데이터 처리 & 알림
│   ├── __init__.py
│   ├── consumer.py              # 🔄 6단계: Kafka 구독 & 처리
│   ├── rules.py                 # 📏 3단계: 임계값 & 판정
│   └── alert.py                 # 🚨 4단계: 알림 발송
│
└── README.md                     # 이 파일
```

### 각 파일의 역할

| 파일 | 라인 수 | 역할 |
|---|---|---|
| `rules.py` | ~500 | 8가지 기상지수별 임계값 정의 + 알림 그룹화 |
| `alert.py` | ~450 | 콘솔, Slack, 이메일, OpenSearch 알림 발송 |
| `producer.py` | ~550 | 기상청/에어코리아 API 호출 + Kafka 발행 |
| `consumer.py` | ~450 | Kafka 구독 + rules 판정 + alert 발송 |
| `air_pipeline.py` | ~350 | Airflow DAG (매시간 자동 실행) |

---

## 📊 데이터 흐름

### 입력 데이터 (공공 API)

**기상청 API:**
```json
{
  "보건기상지수": {
    "cold_risk": 3,          // 감기위험지수 (0-10)
    "asthma_risk": 4,        // 천식질환지수
    "stroke_risk": 2         // 뇌졸중위험지수
  },
  "자외선지수": {
    "uv_index": 6,           // 자외선지수 (0-16)
    "uv_level": "높음"
  }
}
```

**에어코리아 API:**
```json
{
  "pm10": 45,               // 미세먼지 (μg/m³)
  "pm25": 20,               // 초미세먼지
  "o3": 0.065,              // 오존 (ppm)
  "no2": 0.045,             // 이산화질소
  "pm10_grade": "보통",
  "pm25_grade": "보통"
}
```

### 처리 흐름

```
1. [수집] API에서 데이터 수집
   └─ producer/producer.py
   
2. [발행] Kafka 토픽으로 발행
   ├─ air-quality (PM10, PM2.5, O3)
   ├─ health-index (감기위험, 천식위험)
   └─ uv-index (자외선지수)

3. [구독] Consumer가 메시지 수신
   └─ consumer/consumer.py

4. [판정] rules.py로 임계값 비교
   └─ 각 지수별 등급 산정 (좋음/보통/나쁨/매우나쁨)

5. [분류] AlertGrouping으로 행동 그룹화
   ├─ 마스크_필수
   ├─ 외출_자제
   ├─ 자외선_차단
   └─ 기타...

6. [발송] AlertManager로 다중 채널 알림
   ├─ 콘솔 (개발/테스트)
   ├─ Slack (실시간 팀 알림)
   ├─ OpenSearch (이력 저장)
   └─ 대시보드 (시각화)
```

### 출력 예시 (콘솔)

```
================================================================================
🌍 기상지수 알림 | 2026-04-28T10:30:00 | 서울
================================================================================

📊 기상지수 상세 정보:
--------------------------------------------------------------------------------

😷 미세먼지 (PM10): 120
  └─ 등급: 나쁨
  └─ 권고: 마스크 착용 권고 | 실외활동 최소화

😐 초미세먼지 (PM2.5): 45
  └─ 등급: 보통
  └─ 권고: 민감군 주의 | 마스크 착용 권고

😷 자외선 지수: 8
  └─ 등급: 높음
  └─ 권고: 자외선 차단 필수 | 자외선 차단 의류 착용

--------------------------------------------------------------------------------
⚡ 필요한 행동 (Action Groups):
--------------------------------------------------------------------------------

🔴 마스크_필수
  └─ 설명: 마스크 착용이 필수인 상황
  └─ 행동: KF94/KF99 마스크 착용 필수
  └─ 원인: 미세먼지 나쁨, 자외선 높음

🟡 자외선_차단
  └─ 설명: 자외선 차단이 필요한 상황
  └─ 행동: 선크림 SPF50+ 필수, 자외선 차단 의류 착용
  └─ 원인: 자외선 높음

================================================================================
```

---

## 🎯 알림 규칙

### 미세먼지 (PM10)

| 수치 | 등급 | 권고사항 |
|---|---|---|
| ≤ 30 | 좋음 😊 | 외출 자유 \| 창문 개방 좋음 |
| ≤ 80 | 보통 😐 | 민감군 실외활동 제한 권고 |
| ≤ 150 | 나쁨 😷 | 마스크 착용 권고 \| 실외활동 최소화 |
| > 150 | 매우나쁨 ⚠️ | 외출 자제 필수 \| KF94 마스크 필수 |

### 자외선 지수

| 수치 | 등급 | 권고사항 |
|---|---|---|
| ≤ 2 | 낮음 😊 | 자외선 차단 불필요 |
| ≤ 5 | 보통 😐 | 선크림 SPF30+ 권장 |
| ≤ 7 | 높음 😷 | 선크림 SPF50+ 필수 |
| > 7 | 매우높음 ⚠️ | 정오 외출 자제 \| 자외선 차단복 필수 |

### 감기위험지수

| 수치 | 등급 | 권고사항 |
|---|---|---|
| ≤ 2 | 낮음 😊 | 감기 위험 낮음 |
| ≤ 4 | 주의 😐 | 손씻기, うがい 강화 |
| ≤ 6 | 높음 😷 | 마스크 착용 \| 손위생 철저히 |
| > 6 | 고위험 ⚠️ | 외출 자제 \| 사람 많은 곳 피하기 |

### 행동 그룹

**마스크_필수:** PM10/PM2.5 나쁨 이상, 황사 중간 이상  
**외출_자제:** PM10 매우나쁨, 감기 고위험  
**자외선_차단:** 자외선 높음 이상  
**보온_필수:** 체감온도 0도 이하  
**수분_섭취:** 불쾌지수 높음 이상  
**위생_강화:** 감기 주의 이상  

---

## 🧪 개발 및 테스트

### 로컬 개발 (Docker 없이)

```bash
# Python 패키지 설치
pip install -r requirements.txt

# 1. rules.py 테스트 (임계값 판정)
python consumer/rules.py

# 출력:
# ================================================================================
# 기상지수 알림 규칙 테스트
# ================================================================================
# 미세먼지: 120 μg/m³
# 등급: 😷 나쁨
# 권고: 마스크 착용 권고...

# 2. alert.py 테스트 (알림 발송)
python consumer/alert.py

# 출력:
# ================================================================================
# 알림 발송 모듈 테스트
# ================================================================================
# 🌍 기상지수 알림 | 2026-04-28T10:30:00 | 서울
# [상세 알림 출력]

# 3. producer.py 테스트 (API 호출 & Kafka 발행)
# .env 파일에 API 키 설정 후
python producer/producer.py

# 출력:
# ================================================================================
# 기상지수 프로듀서 테스트
# ================================================================================
# 📝 테스트 모드: 실제 API 호출 대신 샘플 데이터 사용
# [샘플 데이터 발행 결과]

# 4. consumer.py 테스트 (메시지 처리)
python consumer/consumer.py

# 출력:
# ================================================================================
# 기상지수 알림 컨슈머 테스트
# ================================================================================
# 테스트 메시지 1/3:
# [알림 처리 결과]
```

### Docker 로컬 테스트

```bash
# 1. 이미지 빌드
docker build -t air-quality-consumer:latest .

# 2. 컨테이너 실행 (로컬 Kafka 필요)
docker run -it \
  --env-file .env \
  --network host \
  air-quality-consumer:latest

# 3. 전체 시스템 테스트
docker-compose up -d

# 4. 로그 모니터링
docker-compose logs -f consumer
docker-compose logs -f airflow

# 5. 종료
docker-compose down
```

### Kafka 메시지 직접 확인

```bash
# 토픽 목록 확인
docker-compose exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

# air-quality 토픽 메시지 확인
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic air-quality \
  --from-beginning \
  --max-messages 5

# 실시간 메시지 구독 (Ctrl+C로 종료)
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic air-quality
```

### OpenSearch 데이터 확인

```bash
# OpenSearch 웹 UI
http://localhost:9200

# 인덱스 목록
curl http://localhost:9200/_cat/indices

# weather-alert 인덱스 검색
curl -X GET "localhost:9200/weather-alert-2026.04.28/_search?pretty"

# 특정 지역 데이터 검색
curl -X POST "localhost:9200/weather-alert-*/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match": {
      "region": "서울"
    }
  }
}'
```

---

## 🚨 트러블슈팅

### Q1: API 키 오류 (`Missing environment variables`)

**증상:** DAG 실행 시 "WEATHER_API_KEY" 환경변수 누락

**해결:**
```bash
# 1. .env 파일 확인
cat .env | grep API_KEY

# 2. Docker 환경변수 로드 확인
docker-compose exec airflow printenv | grep API_KEY

# 3. .env 파일 다시 설정
# .env 파일을 프로젝트 루트에 생성
# docker-compose.yaml의 env_file 항목 확인
```

### Q2: Kafka 연결 오류 (`Failed to connect to Kafka`)

**증상:** Consumer가 Kafka 브로커에 연결 실패

**해결:**
```bash
# 1. Kafka 서비스 상태 확인
docker-compose ps kafka

# 2. Kafka 로그 확인
docker-compose logs kafka

# 3. Kafka 브로커 주소 확인
# .env의 KAFKA_BOOTSTRAP_SERVERS 확인
# docker-compose.yaml의 kafka 서비스 이름 확인

# 4. 네트워크 테스트
docker-compose exec consumer \
  nc -zv kafka 9092
```

### Q3: OpenSearch 연결 오류 (`Failed to connect to OpenSearch`)

**증상:** Consumer가 OpenSearch 연결 실패

**해결:**
```bash
# 1. OpenSearch 서비스 확인
docker-compose ps opensearch

# 2. 연결 테스트
docker-compose exec consumer \
  curl http://opensearch:9200

# 3. 보안 설정 확인 (개발용은 비활성화)
docker-compose.yaml의 plugins.security.disabled 확인
```

### Q4: Airflow DAG이 실행되지 않음

**증상:** Airflow UI에 DAG이 보이지 않음

**해결:**
```bash
# 1. DAG 파일 위치 확인
# dags/air_pipeline.py 파일이 dags/ 디렉토리에 있는지 확인

# 2. Airflow 로그 확인
docker-compose logs airflow

# 3. DAG 구문 검증
docker-compose exec airflow \
  airflow dags test realtime_weather_alert 2026-04-28

# 4. DAG 새로고침
docker-compose exec airflow \
  airflow dags reserialize
```

### Q5: 콘솔에 한글이 깨짐

**증상:** 알림 출력에서 한글이 ???로 표시

**해결:**
```bash
# 1. Docker 이미지 빌드 시 인코딩 설정
# Dockerfile에 추가:
# ENV LANG=C.UTF-8
# ENV PYTHONIOENCODING=utf-8

# 2. Consumer 실행 시 인코딩 지정
PYTHONIOENCODING=utf-8 python consumer/consumer.py
```

### Q6: Slack 알림이 발송되지 않음

**증상:** SLACK_ENABLED=true이지만 알림 미수신

**해결:**
```bash
# 1. Slack Webhook URL 확인
cat .env | grep SLACK_WEBHOOK_URL

# 2. URL 형식 확인 (https://hooks.slack.com/... 형식)

# 3. 네트워크 연결 확인
docker-compose exec consumer \
  curl -X POST YOUR_WEBHOOK_URL \
  -H 'Content-Type: application/json' \
  -d '{"text":"Test"}'

# 4. Slack 워크스페이스 권한 확인
# Incoming Webhooks 앱이 설치되어 있는지 확인
```

### Q7: 메모리 부족 (`OOMKilled`)

**증상:** Container가 자동으로 종료됨

**해결:**
```bash
# 1. Docker 메모리 할당량 증가
# docker-compose.yaml에서:
services:
  consumer:
    deploy:
      resources:
        limits:
          memory: 1G
        reservations:
          memory: 512M

# 2. Airflow 메모리 설정 (환경변수)
AIRFLOW__CORE__DAG_CONCURRENCY=1
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
```

---

## 📈 성능 최적화

### 1. Kafka 파티셔닝

```python
# producer.py에서:
producer.send(
    topic,
    value=data,
    key=region.encode('utf-8')  # 지역별로 같은 파티션으로 라우팅
)
```

**효과:** 같은 지역의 데이터는 순서 보장, 병렬 처리 가능

### 2. OpenSearch 인덱스 관리

```bash
# 일자별 인덱스 자동 생성 (weather-alert-2026.04.28)
# Index Lifecycle Management (ILM) 정책 설정:
# - 7일 후 자동 삭제
# - 또는 다른 저장소로 아카이빙
```

### 3. Airflow 병렬화

```python
# air_pipeline.py에서:
# fetch_air, fetch_health, fetch_uv를 병렬로 실행
[task_fetch_air, task_fetch_health, task_fetch_uv]
```

---

## 📚 추가 리소스

- **Airflow 공식 문서:** https://airflow.apache.org/docs/
- **Kafka 공식 문서:** https://kafka.apache.org/documentation/
- **OpenSearch 공식 문서:** https://opensearch.org/docs/
- **기상청 API 문서:** https://www.data.go.kr/
- **에어코리아 API 문서:** https://www.airkorea.or.kr/

---

## 📝 라이선스

이 프로젝트는 교육 목적의 미니 프로젝트입니다.

---

## 💬 피드백 및 개선

### 가능한 확장 기능

1. **이메일 알림** — alert.py에 SMTP 구현
2. **카톡/문자 알림** — 메시징 서비스 통합
3. **개인 맞춤 알림** — 사용자별 임계값 설정
4. **지역별 예보** — 전국 지역 지원 (현재: 서울만)
5. **Grafana 대시보드** — OpenSearch 데이터 시각화
6. **이상탐지** — 통상 범위를 벗어나는 예측값 감지

---

**마지막 업데이트:** 2026-04-28  
**작성:** Weather Alert Team
