import os
from botocore.client import Config as BotoConfig

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")


def get_catalog_config() -> dict:
    return {
        "type": "rest",
        "uri": ICEBERG_REST_URI,
        "s3.endpoint": MINIO_ENDPOINT,
        "s3.access-key-id": MINIO_ACCESS_KEY,
        "s3.secret-access-key": MINIO_SECRET_KEY,
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }


def get_s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def get_pyarrow_s3_filesystem():
    from pyarrow import fs
    scheme = "https" if MINIO_ENDPOINT.startswith("https") else "http"
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return fs.S3FileSystem(
        endpoint_override=endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        scheme=scheme,
    )