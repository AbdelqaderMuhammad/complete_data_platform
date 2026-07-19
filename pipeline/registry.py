import json
import logging

from pipeline.config import get_s3_client

logger = logging.getLogger(__name__)

REGISTRY_BUCKET = "landing"
REGISTRY_PREFIX = "_registry"


def _registry_key(namespace: str, table_name: str) -> str:
    return f"{REGISTRY_PREFIX}/{namespace}.{table_name}.json"


def load_registered_files(namespace: str, table_name: str) -> set[str]:
    client = get_s3_client()
    key = _registry_key(namespace, table_name)
    try:
        obj = client.get_object(Bucket=REGISTRY_BUCKET, Key=key)
        return set(json.loads(obj["Body"].read()))
    except client.exceptions.NoSuchKey:
        return set()
    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return set()
        raise


def mark_file_registered(namespace: str, table_name: str, s3_path: str) -> None:
    client = get_s3_client()
    key = _registry_key(namespace, table_name)
    registered = load_registered_files(namespace, table_name)
    registered.add(s3_path)
    client.put_object(
        Bucket=REGISTRY_BUCKET,
        Key=key,
        Body=json.dumps(sorted(registered)).encode("utf-8"),
    )
    logger.info("Registry updated: %s now has %d file(s)", key, len(registered))
