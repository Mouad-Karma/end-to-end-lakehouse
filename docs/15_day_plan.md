# Olist Lakehouse — 15-day plan

Build a working, reproducible portfolio project in 15 days, assuming about **4–6 focused hours per day**. This is a target schedule, not a guarantee; leave advanced additions for later.

**Dataset:** the nine Olist CSV files already in `data/raw/` (about 126 MB). They are useful for relational modeling and data engineering practice; they are not evidence of large-scale processing.

**Core stack:** Python, PySpark, Delta Lake, Airflow, PostgreSQL, Docker Compose, pytest, GitHub Actions and one lightweight dashboard.

**Flow:** Olist CSVs → Bronze Delta → Silver Delta → Gold Delta → PostgreSQL → dashboard.

Keep existing code and source files intact during migration. The earlier architecture documents describe the original Supermart proposal; update them for Olist and obtain architecture approval before implementation. This checklist does not itself approve implementation.

## Daily checklist

| Day | Focus | What to do | Finish line |
|---|---|---|---|
| 1 | Olist design | Update the architecture and roadmap. Define each table's grain, keys, joins and first dashboard metrics. Review the design before coding. | Approved Olist scope and diagram |
| 2 | Runtime and project setup | Choose compatible Python/Java/Spark/Delta versions. Add minimal Docker setup, configuration, a Spark session helper and a tiny test fixture. | Delta write/read smoke test works |
| 3 | Bronze ingestion | Read all nine CSVs with explicit source contracts. Preserve raw values, add source/batch metadata and write separate Bronze tables. | Source counts reconcile with Bronze |
| 4 | Safe ingestion reruns | Identify files by content hash, record ingestion state and make repeated ingestion safe. Add a CLI entry point and useful logs. | Same files rerun without duplicate records |
| 5 | Silver core tables | Clean orders, customers, products and sellers. Parse dates, normalize strings and validate keys. Preserve identifiers and ZIP prefixes appropriately. | Typed tables with documented null handling |
| 6 | Silver transaction tables | Clean items, payments and reviews. Use decimal money, validate values and investigate duplicate review identifiers. | Grain and uniqueness rules verified |
| 7 | Quality and geographic lookup | Add quarantine with reason codes and counts. Deduplicate geolocation and build a documented ZIP-level lookup. Join category translations with fallbacks. | Invalid rows traceable; lookup joins preserve counts |
| 8 | Gold sales models | Build order-level and item-level facts. Aggregate payments separately before joining; add category and seller summaries. | No row multiplication; totals reconcile |
| 9 | Gold delivery and review models | Add monthly trends, delivery delays and review summaries. Define eligible order statuses and metric denominators. | Three to five useful, validated analytical tables |
| 10 | Airflow | Wrap tested commands in a DAG with dependencies, retries, timeouts and shared storage paths. | One complete manual DAG run succeeds |
| 11 | Incremental demonstration and recovery | Use clearly labeled staged Olist batches plus a small correction fixture. Demonstrate MERGE, unchanged reruns and retry after failure. | Inserts/updates occur once; recovery preserves totals |
| 12 | PostgreSQL and dashboard | Publish validated Gold through staging and transactional publication. Build a small Metabase or equivalent dashboard. | Dashboard agrees with Gold and SQL totals |
| 13 | Tests and CI | Complete unit/integration tests for keys, joins, quality rules, Delta reruns and publication. Add GitHub Actions using small fixtures. | CI passes without personal credentials |
| 14 | Reproducibility and buffer | Verify Compose initialization, persistence and restart. Test a failure/retry and fix outstanding integration issues. Record resource use. | Documented setup reproduces the pipeline |
| 15 | Portfolio delivery | Finish README, diagram, data dictionary, runbook and screenshots. Record a short end-to-end demonstration. | A reviewer can reproduce and explain the results |

Write tests and introduce Docker alongside the work from Day 2; Days 13–14 complete and verify them. Commit small working changes throughout.

## Olist rules to keep in mind

- **Order items:** key `(order_id, order_item_id)`; one order can contain many items.
- **Payments:** key `(order_id, payment_sequential)`; aggregate to order grain before joining to an order fact.
- **Reviews:** do not assume `review_id` or `order_id` is unique. Inspect repeated records and document how order-level review metrics are selected.
- **Customers:** use `customer_unique_id` for repeat-customer analysis; `customer_id` links orders to their customer records.
- **Categories:** items → products → category translations. Retain untranslated source labels or display an explicit unknown label.
- **Geolocation:** a ZIP prefix has multiple observations. Do not join the raw table directly to transactions; it would multiply rows.
- **Missing dates:** cancellation and incomplete delivery can explain null timestamps. Apply rules by order status rather than dropping every incomplete order.
- **Financial metrics:** item price, freight and payments are different measures. Define each clearly; the source does not provide profit.

## Keep the deadline realistic

Defer **Trino, Iceberg comparisons, Kafka, Kubernetes, MinIO and Prometheus/Grafana**. Add them later only for a specific learning objective. Use structured logs, Airflow task status and simple run/quality metrics initially.

Olist is a static snapshot. Staged batches demonstrate processing behavior, not a live feed or real change-data capture. No ML training is included. If time slips, simplify the dashboard and reduce Gold outputs before sacrificing correct joins, tests or reproducibility.

## Definition of done

- [ ] All nine source files are accounted for with explicit schemas and counts.
- [ ] Bronze, Silver and Gold are real Delta tables with documented grains.
- [ ] Quality checks report rejected records and reasons.
- [ ] Joins and financial totals are reconciled.
- [ ] Repeated input and correction fixtures demonstrate safe reruns and upserts.
- [ ] Airflow runs the pipeline and exposes failures clearly.
- [ ] PostgreSQL serves validated Gold outputs to one useful dashboard.
- [ ] Small meaningful tests pass in CI.
- [ ] Docker setup and restart instructions have been verified.
- [ ] Documentation states actual capabilities, source attribution and limitations.

**First task:** update `target_architecture.md` and `implementation_roadmap.md` for Olist, then review the architecture before starting implementation.
