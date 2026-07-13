# Task List

## Phase 1 — Infrastructure
- [x] MinIO object storage deployed
- [x] Iceberg REST catalog (`apache/iceberg-rest-fixture`) deployed
- [x] Airflow 3.1.x deployed

## Phase 2 — Iceberg mechanics exploration
- [x] Schema evolution script
- [x] Partitioning script
- [x] Time travel script
- [x] Compaction script
- [x] Branching script
- [x] Incremental reads script

## Phase 3 — Ingestion pipeline
- [ ] Decide raw source scope (which NYC Taxi months/years, file format)
- [ ] Land raw files in MinIO landing zone
- [ ] Define partition strategy (likely pickup date)
- [ ] Build Airflow DAG: raw → Iceberg table write
- [ ] Make the load idempotent (rerun-safe on partial failure)
- [ ] Test: rerun a partial/failed load and confirm no duplicate/corrupt data

## Phase 4 — Transformation layer (dbt)
- [ ] Configure dbt Iceberg adapter against the REST catalog
- [ ] Staging models (1:1 with raw Iceberg tables, light typing/renaming)
- [ ] Intermediate models (joins, business logic)
- [ ] Marts models (final analytical tables)
- [ ] Schema tests on marts (not_null, unique, relationships)
- [ ] Model contracts on marts layer

## Phase 5 — Table maintenance operations (headline skill set)
- [ ] Snapshot expiration policy — define retention window, schedule via Airflow
- [ ] Compaction job — implement, schedule, and **measure**: file count and query latency before/after
- [ ] Orphan file cleanup — implement, schedule
- [ ] Partition evolution demo — change partition spec on a live table without rewriting data, document the scenario
- [ ] Rollback demo — deliberately corrupt/bad-load data, roll back via time travel, document the scenario

## Phase 6 — Data quality and observability
- [ ] Row-count / freshness check per ingestion DAG run
- [ ] dbt tests wired into the DAG (fail the run on test failure)
- [ ] Source freshness / Pydantic validation at the landing zone (not just marts)
- [ ] Basic lineage visibility (dbt docs or OpenMetadata)

## Phase 7 — CI/CD and reproducibility
- [ ] GitHub Actions: `dbt test` on PR
- [ ] GitHub Actions: lint (sqlfluff or similar)
- [ ] GitHub Actions: smoke-test DAG run
- [ ] Confirm full stack comes up from `docker compose up` with zero manual steps

## Stretch goals (fold in if time allows)
- [ ] Multi-engine read validation (DuckDB + a second engine, e.g. Trino) — proves the "one copy, many engines" value prop
- [ ] Concurrent writer conflict demo — trigger two competing writes, show Iceberg's optimistic concurrency control resolve or reject one
- [ ] Cost/storage observability — MinIO bucket size tracked over time, tied to compaction/cleanup results