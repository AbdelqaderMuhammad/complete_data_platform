from datetime import datetime

from airflow.decorators import dag, task

from pipeline.landing import download_nyc_taxi_to_landing
from pipeline.raw_layer import initialize_iceberg_table_schema, register_parquet_to_iceberg
from airflow.sdk import Asset

NAMESPACE = "open_lakehouse"
TABLE_NAME = "yellow_taxi_trips"
LANDING_BUCKET = "landing"
raw_table_asset = Asset("iceberg://open_lakehouse/yellow_taxi_trips")

@dag(
    dag_id="nyc_taxi_raw_ingestion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "iceberg", "raw"],
)
def nyc_taxi_raw_ingestion():

    @task
    def land(year: int, month: int) -> dict:
        s3_uri = download_nyc_taxi_to_landing(year=year, month=month, bucket_name=LANDING_BUCKET)
        file_key = s3_uri.replace(f"s3://{LANDING_BUCKET}/", "")
        return {"year": year, "month": month, "file_key": file_key}
    
    @task(max_active_tis_per_dag=1)
    def ensure_table(landed: dict) -> dict:
        initialize_iceberg_table_schema(
            namespace_name=NAMESPACE,
            table_name=TABLE_NAME,
            landing_bucket=LANDING_BUCKET,
            file_key=landed["file_key"],
        )
        return landed

    @task(max_active_tis_per_dag=1, outlets=[raw_table_asset])
    def register(landed: dict) -> None:
        register_parquet_to_iceberg(
            namespace_name=NAMESPACE,
            table_name=TABLE_NAME,
            landing_bucket=LANDING_BUCKET,
            file_key=landed["file_key"],
        )

    periods = [{"year": 2025, "month": m} for m in range(1, 5)]

    landed = land.expand_kwargs(periods)
    ensured = ensure_table.expand(landed=landed)
    register.expand(landed=ensured)


nyc_taxi_raw_ingestion()