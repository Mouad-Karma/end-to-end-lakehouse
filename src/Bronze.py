from pathlib import Path

SOURCE_FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def load_bronze_data(file_path, spark):
    """Read a CSV and keep all values as strings."""
    return spark.read.options(
        header=True,
        inferSchema=False,
        multiLine=True,
        escape='"',
        mode="FAILFAST",
    ).csv(str(file_path))


def load_bronze_tables(input_dir, spark):
    """Check the source files, then load the nine tables."""
    input_dir = Path(input_dir)
    missing = []
    for filename in SOURCE_FILES.values():
        if not (input_dir / filename).is_file():
            missing.append(filename)

    if missing:
        raise FileNotFoundError(f"Missing source CSVs: {', '.join(missing)}")

    tables = {}
    for name, filename in SOURCE_FILES.items():
        path = input_dir / filename
        tables[name] = load_bronze_data(path, spark)
    return tables
