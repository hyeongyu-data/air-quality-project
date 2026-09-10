#!/bin/bash
# Airflow 메타DB(PostgreSQL) 백업 (#119).
#
#   scripts/airflow_db_backup.sh [출력파일]
#
# 저장소 루트에서, 기본 compose 프로젝트명으로 실행한다고 가정한다. 다른
# 프로젝트명이면 COMPOSE_PROJECT_NAME 또는 `docker compose -p …` 를 맞춰준다.
# cron 권장(예: 매일 1회). 복구 절차는 docs/RUNBOOK.md "메타DB 백업·복구".
set -euo pipefail

OUT="${1:-airflow-db-$(date +%Y%m%d-%H%M%S).sql.gz}"

# 덤프를 임시 파일에 먼저 받고, 성공했을 때만 gzip 한다. pg_dump/도커가 실패하면
# set -e 가 여기서 멈추고 trap 이 임시 파일을 지운다 — 빈 .gz 산출물이 안 남는다.
TMP="$(mktemp "${TMPDIR:-/tmp}/airflow-db.XXXXXX.sql")"
trap 'rm -f "$TMP"' EXIT

# --no-owner/--no-privileges: 복구 대상 DB의 롤과 무관하게 적용되도록.
# --clean/--if-exists: 복구 시 기존 객체를 먼저 정리(airflow 중지 후 실행할 것).
docker compose exec -T postgres \
  pg_dump -U airflow -d airflow --clean --if-exists --no-owner --no-privileges > "$TMP"

[ -s "$TMP" ] || { echo "백업 실패: pg_dump 출력이 비었습니다" >&2; exit 1; }

gzip -c "$TMP" > "$OUT"
echo "백업 완료: $OUT ($(du -h "$OUT" | cut -f1))"
