# Opensource Lakehouse

A self-managing data lakehouse that helps data teams stop distrusting their own lake. It solves the problem of multiple consumers reading inconsistent, duplicated snapshots from a shared object store, using Apache Iceberg, Airflow, and dbt on the NYC Taxi dataset as the proving ground.

## Problem

- Teams reading from a shared data lake without a transactional table format see different, inconsistent versions of the same data depending on when they query.
- Concurrent writers corrupt or duplicate files with no coordination.
- Nobody notices small-file sprawl or stale snapshots until storage costs or query latency force the issue.
- Recovering from a bad load usually means a manual, undocumented scramble.

This project builds the fix, then operates it the way a production data platform team actually would — with automated maintenance and orchestration, not a one-time notebook demo.

## What it answers

Built on 15M+ NYC yellow taxi trips: **when is demand and revenue highest, and what drives tipping behavior, by hour of day?**

| Pickup hour | Trips | Revenue | Avg tip % |
|---|---|---|---|
| 4am (trough) | 92,891 | $2.1M | 12.1% |
| 8am | 563,402 | $10.3M | 17.1% |
| 5pm (peak) | 942,505 | $17.3M | 21.6% |
| 11pm | 603,287 | $11.8M | 16.1% |

Computed from a governed, tested gold-layer table — not an ad hoc query against raw files.

## Architecture

```
NYC TLC source files
        │
        ▼
   MinIO landing zone (idempotent download)
        │
        ▼
   Raw / Bronze — Iceberg REST catalog
   unpartitioned, preserves source exactly
        │
        ▼   dbt
   Staging / Silver
   cleaned, partitioned by pickup date
        │
        ▼   dbt
   Marts / Gold — fct_trips, dim_date
   consumption layer
```

Two Airflow DAGs, Asset-linked: ingestion emits an event on successful write, transformation triggers automatically off it. No fixed schedules, no manual chaining. Table maintenance (compaction, snapshot expiration, orphan cleanup) runs on its own scheduled DAG.

## Skills demonstrated

- Iceberg internals: ACID guarantees on object storage, snapshot isolation, schema evolution, partition evolution, time travel/rollback, hidden partitioning
- Catalog architecture and REST vs. Hive Metastore vs. Nessie tradeoffs
- Table maintenance as scheduled code: compaction, snapshot expiration, orphan file cleanup
- Idempotent, partition-aligned incremental ingestion via Airflow
- dbt on an Iceberg REST catalog with model contracts and schema tests
- Multi-engine read validation
- Quantified operational impact (storage reduction, query latency, file count)

## Decisions and trade-offs

**REST catalog over Nessie.** Nessie broke snapshot history reliability; I needed consistent time travel for the maintenance-operations demo, so I traded away Nessie's Git-like branching for a catalog guaranteeing snapshot lineage.

**DuckDB/PyIceberg over Spark.** No compute bottleneck justified Spark's operational weight for this dataset size; DuckDB proved the table-format mechanics and multi-engine read story just as well, at a fraction of the setup cost.

**Partitioning applied at staging, not raw.** The source files contain rows whose pickup timestamps span more than one labeled month, which breaks Iceberg's zero-copy file registration when partitioned on ingestion. Raw stays unpartitioned by design; partitioning is applied downstream, after cleaning.

**dbt isolated in its own virtual environment inside the Airflow image**, rather than installed alongside Airflow's own dependencies, since the two have incompatible pinned requirements. Standard separation for any dbt + Airflow deployment.

## Tech stack

MinIO · Apache Iceberg · REST catalog · Airflow 3.1 (Assets, dynamic task mapping) · dbt-duckdb · DuckDB / PyIceberg · GitHub Actions

## Project structure

```
.
├── dags/            # Airflow DAGs — ingestion, transformation, maintenance
├── pipeline/         # ingestion + catalog logic (Python)
├── dbt/              # staging / marts models
├── notebooks/         # exploration.ipynb — ad hoc catalog/data investigation
├── docker/            # docker-compose.yml, Dockerfiles
└── docs/
```

## How to run this project

```bash
git clone <repo-url>
cd complete_data_platform
docker compose -f docker/docker-compose.yml up -d
```

Airflow UI: `localhost:8080`. Trigger `nyc_taxi_raw_ingestion` — the transformation and maintenance DAGs follow automatically via Asset triggers.