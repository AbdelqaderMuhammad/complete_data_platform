from __future__ import annotations

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

RAW_BUCKET = "raw"
SOURCE_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)
AWS_CONN_ID = "minio_default"


@dag(
    dag_id="ingest_nyc_taxi_raw",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "raw", "nyc_taxi"],
)
def ingest_nyc_taxi_raw():

    @task
    def get_months_to_process() -> list[dict]:
        """Returns the list of (year, month) partitions to ingest.
        Static for now -- swap for a Variable or dbt exposure list later."""
        return [{"year": 2024, "month": m} for m in range(1, 4)]

    @task(retries=3, retry_delay=__import__("datetime").timedelta(minutes=2))
    def download_and_upload(partition: dict) -> str:
        """Streams the source file directly into MinIO without
        materializing the whole thing in task memory."""

        year, month = partition["year"], partition["month"]
        url = SOURCE_URL_TEMPLATE.format(year=year, month=month)
        object_key = (
            f"nyc_taxi/yellow_tripdata/year={year}/month={month:02d}/"
            f"yellow_tripdata_{year}-{month:02d}.parquet"
        )

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Idempotency check: skip re-download if already present
        if hook.check_for_key(object_key, bucket_name=RAW_BUCKET):
            logger.info("Object %s already exists, skipping download", object_key)
            return object_key

        response = requests.get(url, stream=True, timeout=60)
        if response.status_code != 200:
            raise AirflowException(
                f"Failed to download {url}, status={response.status_code}"
            )

        # Stream to a temp path, then upload -- avoids holding the
        # full file in memory for large months
        tmp_path = f"/tmp/{object_key.split('/')[-1]}"
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)

        hook.load_file(
            filename=tmp_path,
            key=object_key,
            bucket_name=RAW_BUCKET,
            replace=True,
        )

        logger.info("Uploaded %s to bucket %s", object_key, RAW_BUCKET)
        return object_key

    @task
    def validate_upload(object_key: str) -> None:
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        if not hook.check_for_key(object_key, bucket_name=RAW_BUCKET):
            raise AirflowException(f"Validation failed: {object_key} not found")

        metadata = hook.head_object(object_key, bucket_name=RAW_BUCKET)
        size_bytes = metadata.get("ContentLength", 0)
        if size_bytes < 1024:  # sanity floor, tune for your data
            raise AirflowException(
                f"Validation failed: {object_key} is suspiciously small "
                f"({size_bytes} bytes)"
            )
        logger.info("Validated %s (%s bytes)", object_key, size_bytes)

    partitions = get_months_to_process()
    uploaded_keys = download_and_upload.expand(partition=partitions)
    validate_upload.expand(object_key=uploaded_keys)


ingest_nyc_taxi_raw()
