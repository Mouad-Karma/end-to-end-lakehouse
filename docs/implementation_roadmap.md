# Prioritized implementation roadmap

Status: proposed; architecture approval is required before Phase 1 implementation. This planning change creates documentation only. Prioritize a working CSV-to-Gold vertical slice, then orchestrate and harden it before expanding services. P0 foundations precede P1 enrichment; optional P2 tools require a concrete problem and separate justification.

## Phase 1 — Audit and refactoring

**Objective:** preserve working pandas behavior and establish a trustworthy baseline before migration.

**Files:** modify README.md and requirements.txt; propose .gitignore, pyproject.toml, tests/unit/test_legacy_pipeline.py, tests/fixtures/, config/sources.yaml and docs/data_contract.md; only then minimally adjust main.py and existing src modules where necessary.

**Technologies:** Python, pandas, pyarrow, pytest, Git.

**Tasks:**
- Review existing uncommitted CSV-to-Parquet migration without overwriting it. Record which generated outputs to retain and stop tracking bytecode only through approved implementation changes.
- Document raw provenance/license, currency and discount semantics as unknown until established. Profile the whole file rather than the notebook sample.
- Capture current schemas, global totals, counts and legacy date interpretation in regression fixtures. Keep tests of legacy behavior distinct from tests of approved business correctness.
- Investigate mixed dates; obtain source evidence or adopt and label an explicit assumption. Preserve ambiguous raw dates; do not silently change monthly figures. If unresolved, quarantine ambiguity in the target and explain reduced accepted counts.
- Add a main guard and explicit callable entry point, fail with useful exceptions and avoid in-place upstream mutation. Preserve current outputs and function behavior until replacement is validated.
- Declare supported runtime and reproducible dependencies after compatibility evaluation; repair notebook date/string ordering later without making notebooks part of orchestration.

**Deliverables:** baseline contract, small adversarial fixtures, regression tests, documented migration boundary and runnable legacy command.

**Acceptance:** baseline has 9,994 raw rows, unique order IDs, sales 14,956,982 and profit 3,747,121.20; current Silver/Gold match existing artifacts under the legacy parser. Tests expose ambiguous dates, missing IDs, duplicate conflicts, invalid discounts and zero denominators. No silent date-policy changes. Git changes are limited and reviewable.

**Dependency:** architecture approval. This is the first implementation phase; not executed in this audit.

## Phase 2 — PySpark + Delta Lake

**Objective:** deliver CSV → Bronze → Silver → Gold as tested, independent commands before orchestration.

**Files:** create src/lakehouse/common/{config,spark,logging}.py; ingestion/files.py; processing/bronze/orders.py, processing/silver/orders.py, processing/gold/sales.py; config/schemas/; scripts/run_pipeline.py; docker/Dockerfile; minimal compose.yaml; tests/integration/test_delta_pipeline.py. Keep legacy files for comparison.

**Technologies:** PySpark, Delta Lake, Python, minimal Docker runtime, pytest.

**Tasks:**
- Pin compatible Python, Java, Spark and Delta versions and verify startup, Delta write/read and MERGE in one image. Do not assume the existing Python 3.14 environment is suitable.
- Add immutable file landing, stable content-hash batch identity and raw lineage. Preserve original lexical values in Bronze.
- Implement explicit casts, date policy and basic quarantine before trusting Silver. Use decimal money, canonical source/order key and deterministic duplicate handling.
- Implement detail fact_orders and the three existing aggregates with safe ratios; use full small-table aggregate recomputation initially.
- Commit each Delta table transactionally and record input/output versions. Keep local tables unpartitioned and shuffle settings small.

**Deliverables:** documented commands, compatible runtime image, real Delta tables with transaction logs, equivalent approved analytics and schema contracts.

**Acceptance:** run from a tiny fixture and full source; demonstrate committed versions and MERGE update. Same input rerun preserves business rows and totals. A changed source order updates once; unresolved conflicts do not reach Gold. Compare pandas/Spark under identical parsing and rounding contracts. If date policy changes, reconcile monthly differences explicitly rather than forcing false legacy equality. No database/dashboard needed yet.

**Dependency:** Phase 1 contracts and date-policy decision or explicit ambiguity quarantine policy.

## Phase 3 — Airflow orchestration

**Objective:** automate the working commands with visible dependencies and recovery.

**Files:** airflow/dags/retail_lakehouse.py; docker runtime additions; compose.yaml; .env.example; tests/unit/test_dag.py; docs/runbook.md.

**Technologies:** Airflow, PostgreSQL metadata database, Docker Compose, existing Spark runtime.

**Tasks:** define ingest → Bronze → Silver checks → Gold → validation; add bounded retries, task timeouts, manual/backfill parameters and task logs. Use max_active_runs=1, shared paths and explicit Spark lifecycle. Pass manifest/version identifiers rather than data in XCom. Pin Airflow and use its supported initialization sequence; add service health checks and named volumes. Add publishing tasks only after Phase 5 implementation exists.

**Deliverables:** importable DAG, documented manual run and backfill, restart instructions and observable task failures.

**Acceptance:** DAG imports without launching a Spark session or network access; a small integration run succeeds; forced ingestion failure blocks downstream tasks; transient failure retries; restart and replay do not duplicate data. Container paths match on all tasks.

**Dependency:** Phase 2 working vertical slice. Minimal containerization starts earlier; full DevOps polish belongs to Phase 6.

## Phase 4 — Data quality and incremental processing

**Objective:** harden replay and add meaningful multi-source ingestion after the basic pipeline works.

**Files:** src/lakehouse/quality/{rules,quarantine,metrics}.py; common/ledger.py; ingestion/{json_files,rest,postgres}.py; config/quality.yaml; sql/source/{schema,seed}.sql; tests/fixtures/; tests/integration/test_{replay,quality,sources}.py; docs/source_contracts.md.

**Technologies:** Python HTTP/database clients, PySpark, Delta MERGE, pytest, operational PostgreSQL, Airflow.

**Tasks:**
- Expand initial validation to a documented rule matrix, rejection accounting, quality thresholds and idempotent quarantine keys.
- Add JSON fixtures and a deterministic local REST replay source using existing order fields. Clearly label simulation and preserve logical source identity to avoid triple-counting replayed orders. Exercise pagination, timeout, 429/5xx backoff and malformed payload handling. Optional real endpoint must have a separate justified contract.
- Seed an operational database from the existing CSV; explicitly label updated_at/change_version as proposed demo extensions. Add initial snapshot, bounded incremental extraction, tie-breakers/overlap and immutable captures. Define deletion limitation or implement tombstones before claiming delete support.
- Add one-row-per-key source resolution, source-version ordering, unchanged-row no-op behavior and controlled additive schema evolution.
- Exercise commit-before-checkpoint crashes, stale versions, concurrent writer prevention and retained replay manifests. Recompute all aggregates initially to cover both old and new group membership after corrections.

**Deliverables:** three source modes in addition to the original CSV path, control ledger, quarantine tables, quality reports, documented replay and source-version rules.

**Acceptance:** unchanged rerun yields zero business changes; new order inserts once; existing order correction updates once; stale update cannot overwrite newer data. Equal-key conflicts are deterministic or quarantined. Invalid rows remain retrievable with reason codes; input accounting balances. Crash after a Delta commit but before checkpoint can recover without duplicate facts/quarantine. Pagination retry loses no records; cursor does not advance on failed durable commit. Additive schema change is reviewed and incompatible change fails clearly. No implied CDC for unsupported deletes.

**Dependency:** Phases 2–3. Basic validation and file idempotency already start in Phase 2; this phase proves the broader guarantees.

## Phase 5 — PostgreSQL and analytics

**Objective:** publish meaningful, validated datasets for SQL and dashboard consumers.

**Files:** src/lakehouse/serving/postgres.py; sql/create_tables.sql; sql/queries.sql; sql/publication.sql; dashboard export/config; tests/integration/test_publication.py; docs/data_dictionary.md; update DAG.

**Technologies:** PostgreSQL, SQL, Python/JDBC staging, Metabase optional Compose profile.

**Tasks:** define numeric types and primary keys for detail and aggregates; separate operational, metadata and serving roles/databases. Publish through run-specific staging and a single SQL transaction for dashboard consistency. Retain prior successful publication on failure. Add queries and dashboard for sales/profit trends, margin, AOV, category share, region mix and discount distribution. Display the single-row North segment and unknown currency honestly. Document grains, ratio definitions, filters, input versions and date assumptions. Keep fact_orders for EDA; no ML training.

**Deliverables:** populated analytical tables, at least five useful SQL queries, one small dashboard and data dictionary.

**Acceptance:** published totals/counts reconcile with the pinned Gold versions; serving keys are unique and money uses decimals. Failed staging or interrupted publication leaves prior dashboard data intact. Retrying the same publication does not duplicate orders. Empty-output publication policy is explicit. Dashboard can be reproduced from documented configuration; no false customer identity analytics.

**Dependency:** Phase 4 hardened keys/replay; a manual publication prototype may follow Phase 2 but must not claim final guarantees.

## Phase 6 — Docker, CI/CD, and monitoring

**Objective:** integrate reproducibility and operational evidence around the finished pipeline.

**Files:** compose.yaml; docker/; .env.example; .gitignore; .github/workflows/ci.yml; monitoring/{metric_contract,inspection_queries.sql}; docs/{setup,runbook,recovery}.md; scripts/smoke_test.py.

**Technologies:** Docker Compose, GitHub Actions, pytest, structured logging and control-table metrics.

**Tasks:** integrate tested service definitions with pinned images, documented first-start initialization, resource settings, health checks, persistent volumes and optional dashboard startup. Keep secrets out of Git. CI runs lint/import/unit checks plus a small Spark/Delta integration test, database publication smoke test and DAG import checks in compatible environments. External API tests use captured fixtures. Record durations, counts, rejection rate, source lag and publication freshness. Provide queries/check commands for failures and stale runs; add an alert integration only if useful and explicitly authorized. Document backup/restore and safe restart without destructive default cleanup.

**Deliverables:** clean-checkout setup instructions, green CI, failure/recovery demonstration and metric examples. CI validates code; automatic deployment is not required for a local portfolio.

**Acceptance:** reproduce from clean checkout and documented environment configuration; demonstrate two runs and a restart with persistent data. CI succeeds without personal credentials or public API availability. Logs correlate source/batch/run/task without leaking payloads or secrets. Record measured machine resources and runtime; fail health checks clearly. Show a recoverable failure and a restored small backup. No HA or large-scale performance claims.

**Dependency:** Phases 2–5. Tests, logging and container contracts begin earlier; this phase integrates and verifies them.

## Phase 7 — Portfolio presentation

**Objective:** make claims verifiable to a reviewer and explain engineering decisions.

**Files:** README.md; docs/{demo,results,data_dictionary,architecture_decisions}.md; architecture diagram export; small sample reports/dashboard screenshots; cleaned notebooks/exploration.ipynb.

**Technologies:** Markdown, Mermaid, SQL, existing tools.

**Tasks:** document problem, source provenance/license status, architecture, exact commands, current implementation status and known limitations. Publish observed row counts, quality metrics, runtime environment, replay demonstration and CI evidence. Explain why local Spark/Delta was chosen and why Kafka/MinIO/dbt were deferred. Show a short CSV-to-dashboard demo and a failure/recovery walkthrough. Define future feature cutoffs, data availability and retention requirements; clearly distinguish proposal from implementation.

**Deliverables:** recruiter-friendly README, technical runbook, data dictionary and reproducible demo with sample outputs.

**Acceptance:** another reader can follow documented setup and reproduce reported metrics; no secrets, invented columns presented as observed facts, unsupported production claims or claims of implemented ML. Diagrams and feature matrix agree with actual code. Any ambiguous-date policy and simulated operational/API sources are visible.

**Dependency:** Phases 1–6 for completed-project claims; documentation improves throughout.

## Approval checkpoint and next task

Review `target_architecture.md` before Phase 1. The recommended first concrete task is **baseline contracts and regression fixtures for the existing pandas pipeline**, focusing first on source date ambiguity. It has a small scope, protects current functionality and makes later Spark migration measurable. This task has not started; only the three requested planning documents were created.
