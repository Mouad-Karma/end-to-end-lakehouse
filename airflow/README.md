# Olist in Airflow

Airflow runs in one Docker container. Its named volume keeps the SQLite
metadata, logs, login, and downloaded Delta jars between restarts. Spark reads
and writes the mounted Olist data directory.

## Start and stop

From the project root, set the host user ID so Spark can write to the data directory.

```bash
printf 'AIRFLOW_UID=%s\n' "$(id -u)" > airflow/.env
airflow_local() {
  docker compose --env-file airflow/.env -f airflow/docker-compose.yaml "$@"
}
airflow_local up -d --build
```

Open [localhost:8080](http://localhost:8080) and sign in with username
`airflow` and password `airflow`. Unpause the DAGs you want to run. Use the play button to
trigger a run manually, and **Grid** to open task states and logs. Use
`airflow_local down` to stop the container and keep its volume.

## What happens in a run

- **Manual full refresh** (`olist_full_refresh`) runs `main.py` on all nine raw
  CSVs to build Bronze, Silver, and Gold. Once a batch is active or archived,
  use a fresh data directory for a full refresh so that rebuilding from the raw
  CSVs cannot undo already processed orders.
- **Hourly order batches** (`olist_incremental_orders`) run once enabled at
  the **top of every hour (UTC)**. The `detect_orders_batches` task checks for active files and
  new CSV content. If it finds none, Airflow skips `apply_orders_batches`.
- The apply task validates each CSV, moves it into `.olist-batches/active/`,
  and cleans the order rows with the existing Silver function. Delta
  `MERGE` matches on `order_id` in Bronze and Silver. A missing ID is inserted.
  A matching ID is updated if its values changed.
- Spark rebuilds the six Gold tables from Silver. After all six writes finish,
  the task moves the CSV to `.olist-batches/processed/` using the file's
  SHA-256 digest as its name. Airflow recognizes the same bytes under any filename.
- A retry resumes the active file first. If an earlier attempt already wrote
  orders, the merge keeps the same rows and Gold is rebuilt again. Delta commits
  each table individually, so check the results after the task succeeds.
  Both DAGs share a filesystem lock.

## Submit an order CSV

Use the same eight headers in the same order as `data/raw/olist_orders_dataset.csv`.
Provide one complete record per ID. Use `YYYY-MM-DD HH:MM:SS` for dates and give
delivered orders a customer delivery timestamp. Customers must already exist in
Silver. Gold uses existing items for sales revenue. Publish batches in update
order with sequential filenames.

```bash
mkdir -p data/incoming
cp /path/to/orders_batch_001.csv data/incoming/.batch.tmp
mv data/incoming/.batch.tmp data/incoming/orders_batch_001.csv
airflow_local exec airflow airflow dags trigger olist_incremental_orders
```

Correct a malformed file in the inbox before the next retry. If a customer
check fails after claiming the file, move it out of `active/` and submit a
corrected copy. Following a write failure, leave the active file for the retry.
A duplicate copy stays in the inbox and can be removed.
