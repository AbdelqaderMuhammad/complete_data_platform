# Opensource Lakehouse

A production-grade open table format lakehouse built on the NYC Taxi dataset, demonstrating Apache Iceberg table mechanics, orchestrated ingestion, dbt transformations, and automated table maintenance.


## Skills demonstrated

- Iceberg internals: ACID guarantees on object storage, snapshot isolation, schema evolution, partition evolution, time travel/rollback, hidden partitioning
- Catalog architecture and REST vs. Hive Metastore vs. Nessie tradeoffs
- Table maintenance as scheduled code: compaction, snapshot expiration, orphan file cleanup
- Idempotent, partition-aligned incremental ingestion via Airflow
- dbt on an Iceberg REST catalog with model contracts and schema tests
- Multi-engine read validation (DuckDB + PyIceberg, optionally a second engine such as Trino)
- Quantified operational impact (storage reduction, query latency, file count)
