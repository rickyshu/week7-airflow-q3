"""Q6: Retry a Python task, then read its return value through XCom."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context


def count_words(text: str) -> int:
    attempt = get_current_context()["ti"].try_number
    if attempt == 1:
        raise RuntimeError("Q6 demo: intentional failure on attempt 1")

    count = len(text.split())
    logging.info("Attempt %s succeeded. Word count: %s", attempt, count)
    return count


def read_word_count() -> None:
    task_instance = get_current_context()["ti"]
    count = task_instance.xcom_pull(task_ids="count_words", key="return_value")
    if count is None:
        raise ValueError("The count_words task did not publish a return value")
    logging.info("XCom pull from count_words: %s words", count)


with DAG(
    dag_id="xcom_demo_hoin",
    description="Q6: Retry once and pass a word count between tasks",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["week7", "q6"],
) as dag:
    count = PythonOperator(
        task_id="count_words",
        python_callable=count_words,
        op_kwargs={"text": "Airflow passes task results through XCom"},
        retries=2,
        retry_delay=timedelta(seconds=15),
    )
    read = PythonOperator(
        task_id="read_word_count",
        python_callable=read_word_count,
    )

    count >> read
