#!/usr/bin/env bash
# Docker 볼륨과 OpenSearch 상태를 점검한다. 인증정보는 인자나 출력으로 받지 않는다.
set -u

threshold="${DISK_USAGE_THRESHOLD:-80}"
failed=0

usage="$(df -P . | awk 'NR==2 {gsub("%", "", $5); print $5}')"
if [ -z "$usage" ] || [ "$usage" -ge "$threshold" ]; then
  echo "디스크 사용률 경고: ${usage:-확인 실패}% (기준 ${threshold}%)"
  failed=1
else
  echo "디스크 사용률 정상: ${usage}% (기준 ${threshold}%)"
fi

health_url="${OPENSEARCH_HEALTH_URL:-http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s}"
if curl --silent --show-error --fail "$health_url" >/dev/null; then
  echo "OpenSearch 상태 정상(yellow 이상)"
else
  echo "OpenSearch 상태 점검 실패 또는 red 상태"
  failed=1
fi

exit "$failed"
