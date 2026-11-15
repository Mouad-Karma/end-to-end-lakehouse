import argparse
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from src.Bronze import load_bronze_tables
from src.Silver import transform_silver_data
from src.Gold import transform_gold_data

PROJECT_DIR = Path(__file__).resolve().parent


def write_parquet_file(data, path):
    """Save a table as one Parquet file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Spark writes a folder. Move its data file to the requested filename.
    with TemporaryDirectory(prefix=".parquet-", dir=path.parent) as directory:
        staging = Path(directory) / "output"
        data.coalesce(1).write.mode("overwrite").parquet(str(staging))
        parts = list(staging.glob("part-*.parquet"))
        if len(parts) != 1:
            raise RuntimeError(f"Expected one Parquet part file, got {len(parts)}")
        parts[0].replace(path)


def run_pipeline(spark, input_dir, output_dir):
    """Load raw data and save Bronze, Silver and Gold tables."""
    output = Path(output_dir)

    # Bronze: save the raw tables.
    bronze = load_bronze_tables(input_dir, spark)
    for name, data in bronze.items():
        path = output / "Bronze" / f"{name}.parquet"
        write_parquet_file(data, path)
        bronze[name] = spark.read.parquet(str(path))
        print(f"BRONZE - {name}")

    # Silver: clean the saved Bronze tables.
    silver = transform_silver_data(bronze)
    for name, data in silver.items():
        path = output / "Silver" / f"{name}.parquet"
        write_parquet_file(data, path)
        silver[name] = spark.read.parquet(str(path))
        print(f"SILVER - {name}")

    # Gold: build business summaries from the saved Silver tables.
    gold = transform_gold_data(silver)
    for name, data in gold.items():
        write_parquet_file(data, output / "Gold" / f"{name}.parquet")
        print(f"GOLD - {name}")


def main():
    parser = argparse.ArgumentParser(description="Olist PySpark Bronze, Silver and Gold pipeline")
    parser.add_argument("--input-dir", default=str(PROJECT_DIR / "data/raw"))
    parser.add_argument("--output-dir", default=str(PROJECT_DIR / "data"))
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OlistLakehouse")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        run_pipeline(spark, args.input_dir, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
