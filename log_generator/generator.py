import json
import os
import random
import time
from datetime import datetime, timezone


LOG_DIR = os.getenv("SENSOR_LOG_DIR", "/sensor_logs")
INTERVAL_SECONDS = float(os.getenv("SENSOR_LOG_INTERVAL_SECONDS", "60"))

SENSORS = ["sensor-seoul-001", "sensor-seoul-002", "sensor-seoul-003"]
METRICS = [
    ("temperature", "celsius", 16.0, 31.0),
    ("humidity", "percent", 30.0, 85.0),
    ("pm10", "ug_m3", 5.0, 140.0),
    ("pm25", "ug_m3", 2.0, 70.0),
]
LEVELS = ["INFO", "INFO", "INFO", "WARN", "ERROR"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_event() -> dict:
    metric_name, unit, low, high = random.choice(METRICS)
    value = round(random.uniform(low, high), 2)
    level = random.choice(LEVELS)
    status = "normal"
    if level == "WARN":
        status = "warning"
    elif level == "ERROR":
        status = "error"

    return {
        "event_time": utc_now(),
        "sensor_id": random.choice(SENSORS),
        "source_type": "json",
        "log_level": level,
        "metric_name": metric_name,
        "metric_value": value,
        "unit": unit,
        "status": status,
        "message": f"{metric_name} reading is {status}",
    }


def write_json_log(event: dict) -> None:
    path = os.path.join(LOG_DIR, "sensor-json.log")
    with open(path, "a", encoding="utf-8") as file:
        file.write(json.dumps(event, ensure_ascii=False) + "\n")


def write_text_log(event: dict) -> None:
    path = os.path.join(LOG_DIR, "sensor-text.log")
    line = (
        f"{event['event_time']} {event['log_level']} "
        f"sensor_id={event['sensor_id']} "
        f"metric={event['metric_name']} "
        f"value={event['metric_value']} "
        f"unit={event['unit']} "
        f"status={event['status']} "
        f"message=\"{event['message']}\""
    )
    with open(path, "a", encoding="utf-8") as file:
        file.write(line + "\n")


def main() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    print(f"Writing sensor logs to {LOG_DIR}", flush=True)

    while True:
        event = build_event()
        write_json_log(event)
        write_text_log({**event, "source_type": "text"})
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
