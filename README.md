# Olist PySpark pipeline

Processes all nine CSVs under `data/raw` into nine Bronze, nine Silver and six Gold Parquet files.
Gold includes order/item facts and sales, seller, delivery and review summaries.

Use Java 17+ and Python compatible with your selected PySpark version:

```bash
pip install -r requirements.txt
python main.py --master 'local[2]'
```

Optional: `--input-dir /path/to/csvs --output-dir /path/to/output`.
Runs overwrite named files. Each table is a single local Parquet file, such as
`data/Bronze/customers.parquet` or `data/Silver/customers.parquet`.

See [the project reference](docs/PROJECT_REFERENCE.md) for objectives,
architecture, table relationships, current implementation, run commands and the
remaining implementation plan. The exploration notebook is separate from this pipeline.

Validation:

```bash
python -B -m unittest discover -s tests
```
