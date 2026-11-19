"""Manual full refresh of all nine source datasets."""
from datetime import datetime, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="olist_full_refresh",
    description="Rebuild all Bronze, Silver and Gold tables from raw CSVs.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
):
    BashOperator(
        task_id="refresh_bronze_silver_gold",
        cwd="/opt/olist",
        bash_command="""
        flock "$OLIST_DATA_DIR/.olist.lock" bash -e -c '
            state="$OLIST_DATA_DIR/.olist-batches"
            if [ -d "$state" ] && [ -n "$(find "$state" -name "*.csv" -print -quit)" ]; then
                echo "Order batches already exist in this data root. Use a fresh root for a full refresh."
                exit 1
            fi
            exec python -u -B main.py --input-dir "$OLIST_DATA_DIR/raw" --output-dir "$OLIST_DATA_DIR"
        '
        """,
    )
