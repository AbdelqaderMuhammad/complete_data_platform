import logging

import requests

from pipeline.config import get_s3_client

logger = logging.getLogger(__name__)


def download_nyc_taxi_to_landing(year: int, month: int, bucket_name: str = "landing") -> str:
    """
    Downloads NYC Yellow Taxi trip data for a specific year/month into the
    MinIO landing bucket. Idempotent: skips download if the object already exists.
    Raises on any failure — never swallows errors, so Airflow surfaces real failures.
    """
    month_str = f"{month:02d}"
    nyc_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month_str}.parquet"
    destination_key = f"yellow_taxi/year={year}/month={month_str}/yellow_tripdata_{year}-{month_str}.parquet"

    client = get_s3_client()

    try:
        client.head_bucket(Bucket=bucket_name)
    except client.exceptions.ClientError:
        logger.info("Bucket '%s' not found, creating it.", bucket_name)
        try:
            client.create_bucket(Bucket=bucket_name)
        except client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                logger.info("Bucket '%s' was created concurrently by another task, continuing.", bucket_name)
            else:
                raise

    try:
        client.head_object(Bucket=bucket_name, Key=destination_key)
        logger.info("Already landed, skipping download: s3://%s/%s", bucket_name, destination_key)
        return f"s3://{bucket_name}/{destination_key}"
    except client.exceptions.ClientError:
        pass  # not found — proceed with download

    logger.info("Downloading %s -> s3://%s/%s", nyc_url, bucket_name, destination_key)
    with requests.get(nyc_url, stream=True, timeout=300) as response:
        response.raise_for_status()
        client.upload_fileobj(Fileobj=response.raw, Bucket=bucket_name, Key=destination_key)

    logger.info("Landed: s3://%s/%s", bucket_name, destination_key)
    return f"s3://{bucket_name}/{destination_key}"