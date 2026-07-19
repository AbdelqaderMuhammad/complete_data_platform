import logging

import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import MonthTransform
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.table.name_mapping import create_mapping_from_schema
from pipeline.config import get_catalog_config, get_pyarrow_s3_filesystem
from pipeline.registry import load_registered_files, mark_file_registered

logger = logging.getLogger(__name__)

PICKUP_TS_COLUMN = "tpep_pickup_datetime"


def initialize_iceberg_table_schema(
    namespace_name: str,
    table_name: str,
    landing_bucket: str,
    file_key: str,
):
    """
    Creates the raw Iceberg table from a source parquet file's schema.
    Intentionally UNPARTITIONED: NYC TLC source files contain a small number
    of rows with pickup timestamps outside the labeled month, which breaks
    add_files()'s file-level partition inference if the table is partitioned
    by pickup date. Partitioning by true business date happens downstream in
    the staging layer, after cleaning/filtering those outlier rows.
    """
    catalog = load_catalog("default", **get_catalog_config())
    catalog.create_namespace_if_not_exists(namespace_name)
    table_identifier = f"{namespace_name}.{table_name}"

    if catalog.table_exists(table_identifier):
        table = catalog.load_table(table_identifier)
        logger.info("Table '%s' already exists, no action taken.", table_identifier)
        return table

    minio_fs = get_pyarrow_s3_filesystem()
    landing_file_path = f"{landing_bucket}/{file_key}"
    dataset = pq.ParquetDataset(landing_file_path, filesystem=minio_fs)
    arrow_schema = dataset.schema

    schema_without_ids = _pyarrow_to_schema_without_ids(arrow_schema)
    iceberg_schema = assign_fresh_schema_ids(schema_without_ids)

    table = catalog.create_table(
        identifier=table_identifier,
        schema=iceberg_schema,
        # no partition_spec — raw layer stays unpartitioned by design
    )
    logger.info("Created unpartitioned raw Iceberg table '%s'.", table_identifier)

    name_mapping = create_mapping_from_schema(iceberg_schema)
    with table.transaction() as tx:
        tx.set_properties(**{"schema.name-mapping.default": name_mapping.model_dump_json()})

    return table


def register_parquet_to_iceberg(
    namespace_name: str,
    table_name: str,
    landing_bucket: str,
    file_key: str,
):
    """
    Zero-copy registers a landed parquet file into the Iceberg table.
    Idempotent via a lightweight registry (see pipeline/registry.py) rather than
    scanning every file in the table's current snapshot.
    """
    catalog = load_catalog("default", **get_catalog_config())
    table_identifier = f"{namespace_name}.{table_name}"
    s3_path = f"s3://{landing_bucket}/{file_key}"

    registered = load_registered_files(namespace_name, table_name)
    if s3_path in registered:
        logger.info("Already registered, skipping: %s", s3_path)
        return

    table = catalog.load_table(table_identifier)
    logger.info("Registering %s into '%s'.", s3_path, table_identifier)
    table.add_files(file_paths=[s3_path])
    mark_file_registered(namespace_name, table_name, s3_path)
    logger.info("Registration complete: %s", s3_path)
