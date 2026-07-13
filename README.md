Phase 1 — Solidify ingestion with idempotency and asset awareness (closes Airflow #3, #4)

Step 1: Refactor your existing NYC Taxi ingestion DAG so each monthly load is idempotent — re-running a given month overwrites/upserts cleanly rather than duplicating. Use Iceberg's merge/overwrite capability for this.
Step 2: Convert the raw-to-MinIO landing task to declare an Asset as an outlet (e.g. Asset("minio://raw/nyc_taxi/{month}")).
Step 3: Create a second DAG scheduled on that asset (schedule=[Asset(...)]) that triggers the Iceberg load step — this demonstrates asset-aware, data-driven scheduling instead of pure cron.
Step 4: Enable catchup=True deliberately for a historical range (e.g. 12 months) and run a real backfill, then write up what you had to handle (partial failures, rate limits, idempotent re-runs).

Phase 2 — Dynamic task mapping at scale (reinforces Airflow #2, adds #9)

Step 5: Replace any hardcoded per-month task list with .expand() over a dynamically generated list of (year, month) tuples pulled from a config or API call.
Step 6: Write one custom operator (e.g. IcebergMergeOperator or MinioToIcebergOperator) that wraps your repeated load logic, instead of using @task everywhere. This is a concrete, demoable artifact for "have you written custom operators."

Phase 3 — Async/deferrable patterns done correctly (Airflow #6 — the nuanced one)

Step 7: Add a task that genuinely waits on something external (e.g. waiting for a file to land in MinIO, or an API to be ready) using a deferrable sensor (deferrable=True).
Step 8: Add a separate, I/O-heavy step (e.g. downloading many monthly files) using the Airflow 3.2 async @task pattern instead of deferrable, and be ready to explain why you chose async over deferrable here — this is the live 2026 nuance: deferrable operators are still appropriate for long running jobs in external systems with well-built provider packages, but for I/O heavy tasks like downloading many files, native async functionality significantly improves performance. Astronomer

Phase 4 — dbt layer on top of Iceberg (dbt #1, #3, #4, #8)

Step 9: Stand up dbt project structure: stg_nyc_taxi_trips, int_trips_enriched (joins to zone lookup), fct_trips and dim_zones/dim_dates marts. Use Kimball-style naming since that is already your strength from Takamol.
Step 10: Implement the fact table as an incremental model with a merge strategy keyed on a unique trip identifier you construct (pickup_datetime + medallion/hack_license or similar composite).
Step 11: Add a snapshot on the zone lookup table (pretend it changes — simulate an SCD2 case by editing a few zone names over "time" between runs) to prove out snapshot mechanics.
Step 12: Add generic tests (not_null, unique, relationships) plus one custom singular test (e.g. "fare_amount should never be negative").

Phase 5 — Slim CI and state selection (dbt #5 — your highest-leverage gap)

Step 13: Set up git branches for this project (you likely already have this) and generate a "production" manifest by running a full dbt build and saving target/manifest.json to a fixed path.
Step 14: Make a deliberate change to one model, then run dbt build --select state:modified+ --defer --state path/to/prod/manifest --favor-state. Document the before/after model count.
Step 15: Wire this into a GitHub Actions workflow so it runs automatically on PRs — this gives you a CI artifact for the portfolio, not just a CLI demo.

Phase 6 — Orchestrating dbt from Airflow properly (Airflow #1+#9 combined with dbt #13)

Step 16: Use Cosmos to render the dbt DAG as native Airflow tasks (you have studied this — now implement it against this specific project) rather than shelling out to dbt run.
Step 17: Make the dbt-Cosmos DAG asset-triggered off the Iceberg load DAG's output asset, chaining Phase 1's asset-aware scheduling into the transformation layer — this ties the whole pipeline together as one asset-driven graph instead of disconnected cron jobs.

Phase 7 — Differentiators (pick 2–3 based on time, not all)

Step 18 (high leverage): Add a dbt sl semantic model + 2–3 metrics (e.g. total_revenue, avg_trip_distance) on top of fct_trips. Query them via dbt sl query. This is a strong, current talking point given how much the industry is pushing semantic layers right now.
Step 19 (medium leverage): Add a human-in-the-loop HITLOperator step (e.g. "approve this backfill before it writes to the gold layer") — niche but signals you track Airflow 3.1+ closely.
Step 20 (medium leverage): Add DAG-level unit tests using pytest + Airflow's DAG-bag testing pattern, run in the same CI pipeline as your dbt Slim CI.

Phase 8 — Wrap-up artifacts for the interview

Step 21: Write a one-page architecture diagram (MinIO/Iceberg/Airflow/dbt/CI, with asset edges shown) — this becomes your "walk me through it" visual.
Step 22: Write 3–4 STAR-style stories tied directly to specific steps above (e.g. "I chose async over deferrable for X because Y", "I implemented idempotent backfills by Z").