"""Airflow 메타DB가 PostgreSQL·LocalExecutor로 유지되는지 가드 (#119).

`docker compose config` 없이(도커 미설치 CI 스텝에서도 돌게) 텍스트로 확인한다.
실제 기동·병렬 실행·복구는 격리 Compose 검증에서 본다.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASE = (ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
PROD = (ROOT / "docker-compose.prod.yaml").read_text(encoding="utf-8")


def test_base_uses_postgres_not_sqlite():
    assert "postgresql+psycopg2://" in BASE
    assert "sqlite:///" not in BASE, "SQLite conn 문자열이 base에 남아 있다"


def test_base_uses_local_executor():
    assert "AIRFLOW__CORE__EXECUTOR: LocalExecutor" in BASE
    assert "AIRFLOW__CORE__EXECUTOR: SequentialExecutor" not in BASE


def test_postgres_service_and_volume_defined():
    assert "postgres:16-alpine" in BASE
    assert "airflow_pg_data:" in BASE
    assert "pg_isready" in BASE


def test_airflow_depends_on_postgres_healthy():
    # airflow 블록 안에 postgres healthy 의존이 있어야 db migrate 가 안 깨진다
    airflow_block = BASE.split("pj-airflow", 1)[1].split("\n  consumer:", 1)[0]
    assert "postgres:" in airflow_block
    assert "condition: service_healthy" in airflow_block


def test_prod_overlay_resets_plain_creds_and_uses_secrets():
    # 평문 자격증명을 !reset 하지 않으면 이미지가 거부하거나 dev 비번으로 붙는다
    assert "POSTGRES_PASSWORD: !reset null" in PROD
    assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: !reset null" in PROD
    assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD:" in PROD
    assert "airflow_db_conn" in PROD and "postgres_password" in PROD


def test_prod_db_conn_cmd_is_bare_cat():
    # _CMD 는 shlex.split 된다 — $(...) 명령 치환이 들어가면 리터럴로 깨진다
    for line in PROD.splitlines():
        if "SQL_ALCHEMY_CONN_CMD:" in line:
            assert "$(" not in line and "$$(" not in line, line
            assert "cat /run/secrets/airflow_db_conn" in line
