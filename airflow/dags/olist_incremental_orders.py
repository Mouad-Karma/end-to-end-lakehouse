"""Poll the incoming directory and upsert only ready order batches."""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG

from orders_batch import pending_batches


def detect_orders_batches():
    root = Path(os.environ["OLIST_DATA_DIR"])
    paths = pending_batches(root)
    if not paths:
        print("No unprocessed orders batches")
        return False
    print("Detected orders batches:", ", ".join(path.name for path in paths))
    return True


with DAG(
    dag_id="olist_incremental_orders",
    description="Upsert incoming orders and refresh the six Gold tables.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
):
    detect = ShortCircuitOperator(
        task_id="detect_orders_batches",
        python_callable=detect_orders_batches,
    )
    apply = BashOperator(
        task_id="apply_orders_batches",
        cwd="/opt/olist",
        bash_command='flock "$OLIST_DATA_DIR/.olist.lock" python -u -B orders_batch.py',
        retries=1,
        retry_delay=timedelta(seconds=15),
    )
    detect >> apply
