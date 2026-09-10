"""공유 패키지 버전이 requirements 파일 사이에서 갈리지 않게 잡는 가드 (#120).

DAG(Airflow 이미지)와 Consumer(자체 이미지)가 서로 다른 kafka-python 으로 같은
토픽을 다루면 계약이 조용히 깨진다 — #22 정합성 원칙의 연장.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _pins(name: str) -> dict:
    out = {}
    for raw in (ROOT / name).read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        m = re.match(r"^([A-Za-z0-9._-]+)\s*==\s*([A-Za-z0-9._-]+)$", line)
        assert m, f"{name}: 고정 핀이 아님 → {raw!r}"
        out[m.group(1).lower().replace("_", "-")] = m.group(2)
    return out


def test_shared_packages_pinned_identically():
    airflow = _pins("requirements-airflow.txt")
    consumer = _pins("requirements-consumer.txt")
    ide = _pins("requirements.txt")

    for a, b, label in ((airflow, consumer, "airflow↔consumer"),
                        (airflow, ide, "airflow↔requirements.txt"),
                        (consumer, ide, "consumer↔requirements.txt")):
        for pkg in a.keys() & b.keys():
            assert a[pkg] == b[pkg], f"{label}: {pkg} {a[pkg]} != {b[pkg]}"


def test_airflow_reqs_is_the_expected_subset():
    airflow = _pins("requirements-airflow.txt")
    # opensearch-py 는 consumer 전용 — Airflow 이미지에 없어야 한다
    assert "opensearch-py" not in airflow
    assert airflow.keys() == {"requests", "kafka-python", "python-dotenv"}
