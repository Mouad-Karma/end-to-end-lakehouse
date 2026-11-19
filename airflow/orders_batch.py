"""Apply order CSV batches to the existing local Olist Delta tables."""
import argparse
import csv
import hashlib
import os
from datetime import datetime
from pathlib import Path

ORDER_COLUMNS = (
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
)
REQUIRED_COLUMNS = (
    "order_id", "customer_id", "order_status",
    "order_purchase_timestamp", "order_estimated_delivery_date",
)
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def batch_digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def pending_batches(root):
    state = root / ".olist-batches"
    active = sorted((state / "active").glob("*.csv"))
    candidates = []
    for path in sorted((root / "incoming").glob("orders_batch_*.csv")):
        if archived_path(root, path).is_file():
            print(f"Already processed (same content): {path.name}", flush=True)
        else:
            candidates.append(path)
    return active + candidates


def archived_path(root, path):
    return root / ".olist-batches" / "processed" / f"{batch_digest(path)}.csv"


def validate_csv(path):
    """Validate the schema and one complete, unambiguous order row per ID."""
    order_ids = set()
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source, strict=True)
        if tuple(next(reader, ())) != ORDER_COLUMNS:
            raise ValueError(f"{path.name}: expected header {','.join(ORDER_COLUMNS)}")
        for row in reader:
            line = f"{path.name}:{reader.line_num}"
            if len(row) != len(ORDER_COLUMNS):
                raise ValueError(f"{line}: expected {len(ORDER_COLUMNS)} columns")
            order = dict(zip(ORDER_COLUMNS, row))
            for column in order:
                order[column] = order[column].strip()
            for column in REQUIRED_COLUMNS:
                if not order[column]:
                    raise ValueError(f"{line}: missing {column}")

            order_id = order["order_id"]
            if order_id in order_ids:
                raise ValueError(f"{line}: duplicate order_id {order_id}")
            order_ids.add(order_id)

            for column in ORDER_COLUMNS[3:]:
                value = order[column]
                if not value:
                    continue
                try:
                    parsed = datetime.strptime(value, TIMESTAMP_FORMAT)
                except ValueError as error:
                    raise ValueError(f"{line}: invalid {column}: {value}") from error
                if parsed.strftime(TIMESTAMP_FORMAT) != value:
                    raise ValueError(f"{line}: invalid {column}: {value}")

            if order["order_status"].lower() == "delivered" and not order["order_delivered_customer_date"]:
                raise ValueError(f"{line}: delivered orders require a customer delivery timestamp")
    if not order_ids:
        raise ValueError(f"{path.name}: empty batch")
    return len(order_ids)


def merge_orders(table, source):
    """Join on order_id and update only if a non-key column changed."""
    # Spark SQL's <=> treats two NULL values as equal.
    changes = []
    for column in source.columns:
        if column != "order_id":
            changes.append(f"NOT (target.{column} <=> source.{column})")

    result = (
        table.alias("target")
        .merge(source.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll(condition=" OR ".join(changes))
        .whenNotMatchedInsertAll()
        .execute()
        .first()
    )
    return result.num_inserted_rows, result.num_updated_rows


def apply_batch(spark, root, path, count):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from main import write_table
    from src.Bronze import load_bronze_data
    from src.Gold import transform_gold_data
    from src.Silver import transform_orders

    bronze = load_bronze_data(path, spark).withColumn("order_id", F.trim("order_id"))
    orders = transform_orders(bronze)
    tables = {}
    silver = root / "Silver"
    for name in ("customers", "order_items", "order_payments", "order_reviews", "products", "sellers"):
        tables[name] = spark.read.format("delta").load(str(silver / name))
    if orders.join(tables["customers"], "customer_id", "left_anti").take(1):
        raise ValueError(f"{path.name}: customer_id must exist in Silver/customers")

    bronze_table = DeltaTable.forPath(spark, str(root / "Bronze/orders"))
    silver_path = str(silver / "orders")
    silver_table = DeltaTable.forPath(spark, silver_path)
    print(f"Validated {path.name}: rows={count}", flush=True)
    for layer, table, source in (("Bronze", bronze_table, bronze), ("Silver", silver_table, orders)):
        inserted, updated = merge_orders(table, source)
        print(f"{layer}/orders: inserted={inserted}, updated={updated}", flush=True)

    # Construct Gold from the committed Silver snapshot, including on retries.
    tables["orders"] = spark.read.format("delta").load(silver_path)
    for name, data in transform_gold_data(tables).items():
        write_table(data, root / "Gold" / name)
        print(f"Refreshed Gold/{name}", flush=True)


def apply_batches(root):
    from main import build_spark

    paths = pending_batches(root)
    if not paths:
        print("No unprocessed orders batches", flush=True)
        return

    state = root / ".olist-batches"
    active = state / "active"
    processed = state / "processed"
    active.mkdir(parents=True, exist_ok=True)
    processed.mkdir(exist_ok=True)
    spark = build_spark("local[2]", "Olist incoming orders")
    try:
        for path in paths:
            archive = archived_path(root, path)
            if archive.exists():
                print(f"Already processed (same content): {path.name}", flush=True)
                continue
            count = validate_csv(path)
            # Claim this file atomically. A retry resumes it before any later batch.
            if path.parent != active:
                path = path.rename(active / path.name)
            apply_batch(spark, root, path, count)
            path.rename(archive)
            print(f"Processed {path.name}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=os.environ.get("OLIST_DATA_DIR", "data"))
    apply_batches(parser.parse_args().data_dir)
