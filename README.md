# Olist PySpark pipeline

Processes all nine CSVs under `data/raw` into nine Bronze, nine Silver and six Gold tables.
Gold includes order/item facts and sales, seller, delivery and review summaries.

Use Java 17+ and an activated Python virtual environment. The requirements pin
Spark 4.1.1 and Delta Lake 4.2.0. Spark downloads Delta JVM dependencies from
Maven on first use if they are absent from its cache.

```bash
python -m pip install -r requirements.txt
python main.py
```

Use `--input-dir`, `--output-dir`, or `--master` to change the run settings.

## Delta Lake

The pipeline writes tables directly under `data/Bronze/`, `data/Silver/`, and
`data/Gold/`. Each table has its own directory, for example `data/Silver/orders/`.

- **Transaction log and ACID writes**. The `_delta_log/` records which Parquet
  files belong to each version. Each table overwrite commits atomically, and a
  reader sees a consistent snapshot. A pipeline run commits tables individually.
- **Table history and time travel**. Run the Delta pipeline twice to create two
  versions. Open a fresh notebook kernel with the Delta dependencies, run the
  following from the project root, and compare the original orders snapshot
  with the current one.

  ```python
  from delta.tables import DeltaTable
  from main import build_spark

  spark = build_spark("local[2]")
  path = "data/Silver/orders"
  DeltaTable.forPath(spark, path).history().select("version", "operation").show()
  current = spark.read.format("delta").load(path)
  original = spark.read.format("delta").option("versionAsOf", 0).load(path)
  print(current.count(), original.count())
  ```

  Re-running with the same CSVs can create a new version with the same row count.
  Time travel depends on retention of the relevant log and data files.
- **Schema enforcement**. Delta checks incoming writes against the table
  schema. When changing a source schema, update the transformations and decide
  whether to explicitly evolve the destination schema.
- **Incremental upserts**. A future Silver orders job can use `MERGE` on
  `order_id` to update matching orders and insert new ones. The current pipeline
  uses full-table overwrites, so each run rebuilds the tables from the CSVs.

See the [Delta Lake Spark quickstart](https://docs.delta.io/quick-start/)
for more examples.

See [the project reference](docs/PROJECT_REFERENCE.md) for objectives,
architecture, table relationships, current implementation, run commands and the
remaining implementation plan. The exploration notebook is separate from this pipeline.

Validation:

```bash
python -B -m unittest discover -s tests
```
