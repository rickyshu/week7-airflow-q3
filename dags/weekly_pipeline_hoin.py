"""Download Netflix data, submit a Spark aggregation, and publish its Parquet."""

import logging
import os
from pathlib import Path

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param


INPUT_KEY = "bronze/netflix_titles.csv"
RUN_DIR = "/opt/airflow/logs/q9_output/run-{{ run_id }}"
INPUT_FILE = f"{RUN_DIR}/netflix_titles.csv"
OUTPUT_DIR = f"{RUN_DIR}/parquet"
RUN_AFTER = "{{ dag_run.run_after.isoformat() }}"


def get_bucket(bucket: str | None) -> str:
    bucket = bucket or os.environ.get("Q9_S3_BUCKET", "")
    if not bucket:
        raise ValueError("Supply the bucket run parameter or set Q9_S3_BUCKET")
    return bucket


def download_bronze(bucket: str, input_file: str) -> None:
    bucket = get_bucket(bucket)
    destination = Path(input_file)
    destination.parent.mkdir(parents=True, exist_ok=True)
    s3 = S3Hook(aws_conn_id="q9_aws").get_conn()
    s3.download_file(bucket, INPUT_KEY, str(destination))
    logging.info(
        "Downloaded s3://%s/%s (%d bytes)", bucket, INPUT_KEY, destination.stat().st_size
    )


def upload_silver(bucket: str, output_dir: str, run_after: str) -> None:
    bucket = get_bucket(bucket)
    parts = list(Path(output_dir).glob("part-*.snappy.parquet"))
    if len(parts) != 1:
        raise ValueError(f"Expected one Snappy Parquet file, found {len(parts)}")

    output_date = pendulum.parse(run_after).in_timezone("Asia/Seoul").to_date_string()
    prefix = f"silver/{output_date}/"
    key = f"{prefix}part-00000.snappy.parquet"
    s3 = S3Hook(aws_conn_id="q9_aws").get_conn()
    s3.upload_file(str(parts[0]), bucket, key)
    logging.info("Uploaded s3://%s/%s (%d bytes)", bucket, key, parts[0].stat().st_size)
    object_count = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"] != prefix:
                logging.info("Silver object: %s (%d bytes)", item["Key"], item["Size"])
                object_count += 1
    logging.info("Silver object count: %d", object_count)


with DAG(
    dag_id="weekly_pipeline_hoin",
    description="Aggregate Netflix titles by type and genre each week",
    schedule="0 9 * * 1",
    start_date=pendulum.datetime(2026, 11, 9, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        "bucket": Param(None, type=["null", "string"], minLength=3),
        "min_year": Param(2015, type="integer", minimum=1900, maximum=2100),
    },
    tags=["q9", "week7"],
) as dag:
    download = PythonOperator(
        task_id="download_bronze",
        python_callable=download_bronze,
        op_kwargs={"bucket": "{{ params.bucket or '' }}", "input_file": INPUT_FILE},
    )
    transform = SparkSubmitOperator(
        task_id="transform",
        application=str(Path(__file__).parent / "jobs" / "transform.py"),
        conn_id="q9_spark_local",
        application_args=[
            "--input", INPUT_FILE,
            "--output", OUTPUT_DIR,
            "--min-year", "{{ params.min_year }}",
        ],
        conf={"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"},
    )
    upload = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_silver,
        op_kwargs={
            "bucket": "{{ params.bucket or '' }}",
            "output_dir": OUTPUT_DIR,
            "run_after": RUN_AFTER,
        },
    )

    download >> transform >> upload
