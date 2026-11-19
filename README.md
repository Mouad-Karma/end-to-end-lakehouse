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

For Docker Compose setup and the Airflow DAGs, see
[Run the Olist pipeline in Airflow](airflow/README.md).

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
- **Incremental upserts**. The Airflow orders DAG uses `MERGE` on `order_id`
  in Bronze and Silver, then recalculates Gold. Running `main.py` performs
  a full rebuild from the nine source CSVs.

See the [Delta Lake Spark quickstart](https://docs.delta.io/quick-start/)
for more examples.

See [the project reference](docs/PROJECT_REFERENCE.md) for objectives,
architecture, table relationships, current implementation, run commands and the
remaining implementation plan. The exploration notebook is separate from this pipeline.

Validation:

```bash
python -B -m unittest discover -s tests
```

## Dashboard Gold

Depuis la racine du projet, installe les dépendances du dashboard et lance Streamlit.

```bash
.venv/bin/python -m pip install -r dashboard/requirements.txt
.venv/bin/python -m streamlit run dashboard/app.py --server.address 127.0.0.1
```

Ouvre [localhost:8501](http://localhost:8501). Choisis une année pour filtrer
les indicateurs et les graphiques. Clique sur **Actualiser** après une mise à
jour de Gold. Arrête Streamlit avec `Ctrl+C`.

DuckDB interroge les tables Delta `sales_monthly` et `fact_order_items` avec
`delta_scan`. Le premier accès peut télécharger l’extension Delta. Le tableau
de bord compte les commandes livrées avec des articles et affiche le prix des
articles séparément des frais de livraison.
