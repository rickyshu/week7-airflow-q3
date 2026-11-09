"""Q3: four tasks in a daily Airflow DAG."""

from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def return_first_message() -> str:
    return "Python1 returned: hello from the first task"


def return_second_message() -> str:
    return "Python2 returned: hello from the second task"


with DAG(
    dag_id="sample_dag",
    description="Q3 daily DAG with two Python return values",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    tags=["week7", "q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    python1 = PythonOperator(task_id="Python1", python_callable=return_first_message)
    python2 = PythonOperator(task_id="Python2", python_callable=return_second_message)
    end = EmptyOperator(task_id="end")

    start >> python1 >> python2 >> end
