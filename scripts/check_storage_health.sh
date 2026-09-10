#!/usr/bin/env bash
# Docker 볼륨과 OpenSearch 상태를 점검한다. 인증정보는 인자나 출력으로 받지 않는다.
#
# 호스트에서 실행한다(컨테이너 안 df 는 VM 디스크라 의미 없음).
# cron 예: */15 * * * * cd /path/to/repo && scripts/check_storage_health.sh
#
# 알람 기준 f(디스크)·g(클러스터)를 담당한다. ALERT_WATCH_SLACK_ENABLED=true 이고
# SLACK_WEBHOOK_URL 이 있으면 실패 시 Slack 으로도 통보한다. 나머지 기준
# a·b·c·d 는 consumer/alert_watch.py (#121).
set -u

threshold="${DISK_USAGE_THRESHOLD:-80}"
failed=0
report=""

usage="$(df -P . | awk 'NR==2 {gsub("%", "", $5); print $5}')"
if [ -z "$usage" ] || [ "$usage" -ge "$threshold" ]; then
  msg="디스크 사용률 경고: ${usage:-확인 실패}% (기준 ${threshold}%)"
  echo "$msg"; report="${report}\n• (f) ${msg}"; failed=1
else
  echo "디스크 사용률 정상: ${usage}% (기준 ${threshold}%)"
fi

health_url="${OPENSEARCH_HEALTH_URL:-http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s}"
if curl --silent --show-error --fail "$health_url" >/dev/null; then
  echo "OpenSearch 상태 정상(yellow 이상)"
else
  msg="OpenSearch 상태 점검 실패 또는 red 상태"
  echo "$msg"; report="${report}\n• (g) ${msg}"; failed=1
fi

# 실패 시 Slack 통보 (opt-in). webhook 은 로그·출력에 남기지 않는다.
if [ "$failed" -ne 0 ] && [ "${ALERT_WATCH_SLACK_ENABLED:-false}" = "true" ] && [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  payload="$(printf '{"text":"*[관측 알람]* 스토리지 점검%b"}' "$report")"
  if curl --silent --show-error --fail -X POST -H 'Content-Type: application/json' \
       -d "$payload" "$SLACK_WEBHOOK_URL" >/dev/null 2>&1; then
    echo "Slack 통보 전송"
  else
    echo "Slack 통보 실패"
  fi
fi

exit "$failed"
