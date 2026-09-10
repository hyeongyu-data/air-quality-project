#!/bin/sh
# weather_writer(및 기타 internal_users.yml) 비밀번호의 bcrypt 해시를 만든다.
# OpenSearch 이미지의 hash.sh를 그대로 쓴다 — 로컬에 Java 설치 불필요.
#
#   scripts/opensearch_hash.sh 'my-new-password'
#
# 출력 해시를 config/opensearch-security/internal_users.yml의 weather_writer.hash에
# 넣고, 같은 비밀번호를 secrets/opensearch_password에 저장한다(둘이 일치해야 함).
set -eu

if [ $# -ne 1 ]; then
  echo "사용법: $0 '<평문 비밀번호>'" >&2
  exit 2
fi

IMAGE="${OPENSEARCH_IMAGE:-opensearchproject/opensearch:2.8.0}"

docker run --rm "$IMAGE" \
  bash -c 'plugins/opensearch-security/tools/hash.sh -p "$0"' "$1" \
  | grep -E '^\$2[aby]\$'
