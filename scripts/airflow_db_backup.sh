#!/bin/sh
# Airflow 메타DB(PostgreSQL) 백업 (#119).
#
#   scripts/airflow_db_backup.sh [출력파일]
#
# cron 권장(예: 매일 1회). 복구 절차는 docs/RUNBOOK.md "메타DB 백업·복구".
set -eu

OUT="${1:-airflow-db-$(date +%Y%m%d-%H%M%S).sql.gz}"

# --no-owner/--no-privileges: 복구 대상 DB의 롤과 무관하게 적용되도록.
# --clean/--if-exists: 복구 시 기존 객체를 먼저 정리(airflow 중지 후 실행할 것).
docker compose exec -T postgres \
  pg_dump -U airflow -d airflow --clean --if-exists --no-owner --no-privileges \
  | gzip > "$OUT"

echo "백업 완료: $OUT ($(du -h "$OUT" | cut -f1))"
