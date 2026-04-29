# ============================================================================
# 기상지수 알림 Consumer 컨테이너 이미지
# ============================================================================

FROM python:3.10-slim

# ============================================================================
# 기본 설정
# ============================================================================

# 작업 디렉토리 설정
WORKDIR /app

# 환경 변수 설정
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# ============================================================================
# 시스템 패키지 업데이트 및 필수 도구 설치
# ============================================================================

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    gcc \
    netcat-traditional \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ============================================================================
# Python 패키지 설치 (레이어 캐싱 최적화)
# ============================================================================

# 1단계: Consumer 전용 requirements 복사 (코드 변경과 독립적)
COPY requirements-consumer.txt .

# 2단계: 의존성 설치
RUN pip install --upgrade pip setuptools && \
    pip install --no-cache-dir -r requirements-consumer.txt

# ============================================================================
# 애플리케이션 코드 복사
# ============================================================================

# 3단계: 프로젝트 파일 복사 (변경 빈번, 따라서 마지막)
COPY . .

# ============================================================================
# 폴더 구조 확인 및 권한 설정
# ============================================================================

RUN chmod -R 755 /app && \
    ls -la /app

# ============================================================================
# 헬스체크 스크립트 (선택사항)
# ============================================================================

# healthcheck용 간단한 스크립트 (OpenSearch 연결 확인)
RUN cat > /app/healthcheck.sh << 'EOF'
#!/bin/bash
set -e

# OpenSearch 연결 확인
curl -f http://${OPENSEARCH_HOST:-localhost}:${OPENSEARCH_PORT:-9200}/_cluster/health || exit 1

# Kafka 연결 확인
python -c "
from kafka import KafkaConsumer
import sys
try:
    consumer = KafkaConsumer(bootstrap_servers='${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}')
    consumer.close()
except Exception as e:
    print(f'Kafka connection failed: {e}')
    sys.exit(1)
"

exit 0
EOF

RUN chmod +x /app/healthcheck.sh

# ============================================================================
# 포트 (정보용, 실제 바인딩은 docker-compose에서)
# ============================================================================

# Consumer는 포트를 직접 바인딩하지 않음 (Kafka/OpenSearch에만 접속)

# ============================================================================
# 시작 명령어 (docker-compose.yaml에서 오버라이드 가능)
# ============================================================================

CMD ["python", "consumer/consumer.py"]

# ============================================================================
# 이미지 메타데이터
# ============================================================================

LABEL maintainer="Weather Alert Team" \
      version="1.0" \
      description="기상지수 알림 Consumer 이미지"

# ============================================================================
# 빌드 방법:
#
#   docker build -t air-quality-consumer:latest .
#
# 실행 방법:
#
#   docker run -it \
#     --env-file .env \
#     --network host \
#     air-quality-consumer:latest
#
# Docker Compose 통합:
#
#   docker-compose up -d
#
# ============================================================================
