"""DAG 스케줄과 판정 분기가 어긋나지 않는지 검증 (#73 회귀 방지).

이전 상태: cron은 매시간(`0 * * * *`)인데 collect_scheduled_weather는
00·06·12·18시만 분기를 정의했다. 하루 24회 중 20회가 "정의되지 않은
실행 시각" 경로로 떨어지고 공공 API 호출만 6배였다. README의 동작표도
네 줄뿐이라 문서와 실제가 어긋나 있었다.

airflow를 import 하지 않는다. CI에 airflow가 없기도 하고, 이 검증에는
DAG 파일의 cron 문자열만 있으면 충분하다.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAG_SOURCE = (ROOT / "dags" / "air_pipeline.py").read_text(encoding="utf-8")
PRODUCER_SOURCE = (ROOT / "producer" / "producer.py").read_text(encoding="utf-8")

# 알림 설계상 의미가 정의된 시각
DEFINED_HOURS = {0, 6, 12, 18}


def _cron_hours(expression: str) -> set:
    """`분 시 ...` cron의 시(hour) 필드를 실제 시각 집합으로 푼다."""
    hour_field = expression.split()[1]
    if hour_field == "*":
        return set(range(24))
    if hour_field.startswith("*/"):
        return set(range(0, 24, int(hour_field[2:])))
    return {int(part) for part in hour_field.split(",")}


def _dag_cron() -> str:
    match = re.search(r'schedule_interval="([^"]+)"', DAG_SOURCE)
    assert match, "DAG에서 schedule_interval을 찾지 못했습니다"
    return match.group(1)


def test_schedule_matches_defined_alert_hours():
    """cron이 실제로 도는 시각 = 판정 분기가 정의된 시각."""
    assert _cron_hours(_dag_cron()) == DEFINED_HOURS


def test_producer_defines_branches_for_those_hours():
    """반대 방향도 고정한다 — 프로듀서가 네 시각을 계속 다루는지."""
    assert "if hour == 0:" in PRODUCER_SOURCE
    assert "if hour == 6:" in PRODUCER_SOURCE
    assert "if hour in (12, 18):" in PRODUCER_SOURCE


def test_readme_documents_the_same_hours():
    """문서 동작표의 시각이 cron과 일치하는지."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    table = readme.split("## 현재 동작", 1)[1].split("##", 1)[0]
    documented = {int(h) for h in re.findall(r"^\| (\d{2})시 ", table, re.MULTILINE)}
    assert documented == DEFINED_HOURS


# ---------- cron 파서 자체 ----------

def test_cron_hour_parsing():
    assert _cron_hours("0 */6 * * *") == {0, 6, 12, 18}
    assert _cron_hours("0 * * * *") == set(range(24))
    assert _cron_hours("0 0,6,12,18 * * *") == {0, 6, 12, 18}
    assert _cron_hours("0 */12 * * *") == {0, 12}
