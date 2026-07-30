#!/usr/bin/env bash
# OpenSearch Dashboards 초기 구성 — 인덱스 패턴 3종을 만든다.
# 여러 번 실행해도 안전하다(이미 있으면 409를 무시).
#
# 사용: docker compose --profile ops up -d 후
#   ./scripts/setup_dashboards.sh
set -euo pipefail

OSD_URL="${OSD_URL:-http://localhost:5601}"

echo "OpenSearch Dashboards 대기 중..."
for _ in $(seq 1 30); do
  status=$(curl -s "$OSD_URL/api/status" | grep -o '"state":"green"' || true)
  [ -n "$status" ] && break
  sleep 5
done

create_pattern() {
  local id="$1" title="$2" time_field="$3"
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$OSD_URL/api/saved_objects/index-pattern/$id" \
    -H 'osd-xsrf: true' -H 'Content-Type: application/json' \
    -d "{\"attributes\": {\"title\": \"$title\", \"timeFieldName\": \"$time_field\"}}")
  case "$code" in
    200) echo "생성: $title" ;;
    409) echo "이미 있음: $title" ;;
    *)   echo "실패($code): $title" ; exit 1 ;;
  esac
}

create_pattern "weather-metrics" "weather-metrics-*" "timestamp"
create_pattern "weather-alert"   "weather-alert-*"   "timestamp"
create_pattern "weather-cooldown-state" "weather-cooldown-state" "updated_at"

echo ""
echo "완료. $OSD_URL 에서 Discover를 열고 인덱스 패턴을 선택하세요."
echo "자주 쓰는 필터는 docs/observability.md의 알람 기준 표를 참고."
