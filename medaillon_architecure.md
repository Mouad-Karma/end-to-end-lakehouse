# Olist medallion architecture

## Bronze: keep the source data

Read the nine CSV files from `data/raw` and save nine tables under
`data/delta/Bronze`. Columns stay strings so identifiers and ZIP codes keep their
original values. Multiline review comments are supported.

## Silver: simple cleaning with PySpark

Read the Bronze datasets and save nine cleaned tables under `data/delta/Silver`.
`src/Silver.py` has one function for each dataset. Each function first calls
`clean_common_columns`, which cleans headers, trims text, turns blank values
into nulls and removes exact duplicate rows. Nonblank review prose is preserved.

| Function / table | What it does |
|---|---|
| `transform_customers` | Lowercase city, uppercase state; keep ZIP as text and preserve both customer IDs. |
| `transform_orders` | Convert five timestamps; normalize status; add purchase date/year/month, delivery days and late-delivery indicator. |
| `transform_order_items` | Convert item number, price, freight and shipping date; add item_total = price + freight. |
| `transform_order_payments` | Convert payment sequence, installments and value; lowercase payment type. |
| `transform_order_reviews` | Convert score and review timestamps; retain missing optional comments. |
| `transform_products` | Fix the two `lenght` header typos, convert measurements/counts, calculate volume in cm³, and left join English categories. |
| `transform_sellers` | Lowercase city and uppercase state; keep ZIP as text. |
| `transform_geolocation` | Convert latitude/longitude; lowercase city and uppercase state; remove exact duplicate source rows. |
| `transform_category_translation` | Remove the header BOM through common cleaning and lowercase category labels. |

`transform_silver_data(tables)` calls these functions and returns the nine tables.
The translation table is cleaned before products so it can be used in their join.
Multiple mappings for one category stop the run rather than multiplying products.

Invalid numeric/date strings become null using `try_cast` / `try_to_timestamp`.
Missing delivery dates, missing comments and missing product categories do not
cause rows to be deleted. Missing English translations remain null.
Delivery days use calendar-date differences. Late delivery compares actual and
estimated calendar dates; if either is missing, the result is null.
Source timestamps have no timezone; the CLI uses a UTC Spark session for consistency.

This beginner version has no generic validation configuration, quality-flag
framework, reference-check joins or extra geolocation lookup table. It removes
exact duplicates only, and does not arbitrarily deduplicate business IDs.

## Gold: analytical functions

The pipeline reads saved Silver tables. `src/Gold.py` provides order and item facts,
monthly sales, category sales, seller performance and monthly delivery/review
summaries. `transform_gold_data(tables)` returns all six DataFrames; `main.py`
saves them under `data/delta/Gold/`. Sales include delivered orders only.

Keep these relationships in mind:

- Orders connect to customers through customer_id. customer_unique_id can be used
  later for repeat-customer analysis.
- Items connect to orders, products and sellers.
- Payments and reviews connect to orders.
- Several items, payments or reviews can belong to one order. Joining them directly
  can multiply sales; aggregate each to order level first when needed.
- Geolocation contains several locations per ZIP; joining directly on ZIP can
  multiply rows. Build a lookup first if you need it for Gold.
- These files do not contain product costs or profit/discount columns.

## Running the pipeline

```bash
python main.py
```

Each run overwrites nine Bronze, nine Silver, and six Gold [Delta tables](README.md#delta-lake).
Each table's transaction log records committed versions and supports time travel.
