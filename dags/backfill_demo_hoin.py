"""Q7: Catch up on completed daily intervals and write one file per date."""

import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import CronDataIntervalTimetable, DAG, get_current_context


SEOUL = ZoneInfo("Asia/Seoul")
OUTPUT_DIR = Path("/opt/airflow/logs/q7_output/backfill_demo_hoin")


def write_daily_file() -> None:
    logical_date = get_current_context().get("logical_date")
    if logical_date is None:
        raise ValueError("A logical date is required to name the daily output file")

    day = logical_date.astimezone(SEOUL).date().isoformat()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output = OUTPUT_DIR / f"ds={day}.txt"
    output.write_text(f"logical_date_kst={day}\n", encoding="utf-8")
    logging.info("Wrote daily output %s", output)


with DAG(
    dag_id="backfill_demo_hoin",
    description="Q7: Catch up on daily intervals and write date-specific files",
    start_date=datetime(2026, 11, 3, tzinfo=SEOUL),
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="Asia/Seoul"),
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["week7", "q7"],
) as dag:
    PythonOperator(task_id="write_daily_file", python_callable=write_daily_file)
