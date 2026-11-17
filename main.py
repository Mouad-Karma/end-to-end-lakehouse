import argparse
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.Bronze import load_bronze_tables
from src.Silver import transform_silver_data
from src.Gold import transform_gold_data

PROJECT_DIR = Path(__file__).resolve().parent


def build_spark(master, app_name="OlistLakehouse"):
    builder = (
        SparkSession.builder.appName(app_name).master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_table(data, path):
    data.write.format("delta").mode("overwrite").save(str(path))


def run_pipeline(spark, input_dir, output_dir):
    """Load raw data and save Bronze, Silver and Gold tables."""
    output = Path(output_dir)

    # Bronze: save the raw tables.
    bronze = load_bronze_tables(input_dir, spark)
    for name, data in bronze.items():
        path = output / "Bronze" / name
        write_table(data, path)
        bronze[name] = spark.read.format("delta").load(str(path))
        print(f"BRONZE - {name}")

    # Silver: clean the saved Bronze tables.
    silver = transform_silver_data(bronze)
    for name, data in silver.items():
        path = output / "Silver" / name
        write_table(data, path)
        silver[name] = spark.read.format("delta").load(str(path))
        print(f"SILVER - {name}")

    # Gold: build business summaries from the saved Silver tables.
    gold = transform_gold_data(silver)
    for name, data in gold.items():
        write_table(data, output / "Gold" / name)
        print(f"GOLD - {name}")


def main():
    parser = argparse.ArgumentParser(description="Olist PySpark Bronze, Silver and Gold pipeline")
    parser.add_argument("--input-dir", default=str(PROJECT_DIR / "data/raw"))
    parser.add_argument("--output-dir", default=str(PROJECT_DIR / "data"))
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()

    spark = build_spark(args.master)
    try:
        run_pipeline(spark, args.input_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
