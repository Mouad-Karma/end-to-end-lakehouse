import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from Bronze import load_bronze_data
from Silver import transform_silver_data
from Gold import create_sales_by_region, create_sales_by_category, create_sales_by_year_and_month

RAW_FILE = "data/raw/Supermart Grocery Sales - Retail Analytics Dataset.csv"
BRONZE_FILE = "data/Bronze/bronze_data.parquet"
SILVER_FILE = "data/Silver/silver_data.parquet"
GOLD_DIR = "data/Gold"
GOLD_BY_REGION = f"{GOLD_DIR}/sales_by_region.parquet"
GOLD_BY_CATEGORY = f"{GOLD_DIR}/sales_by_category.parquet"
GOLD_BY_YEAR_MONTH = f"{GOLD_DIR}/sales_by_year_and_month.parquet"

# Bronze
bronze_data = load_bronze_data(RAW_FILE)
os.makedirs("data/Bronze", exist_ok=True)
bronze_data.to_parquet(BRONZE_FILE, index=False)

print("BRONZE")
print(f"Rows: {len(bronze_data)}")
print(bronze_data.head())
print()

# Silver
silver_data = transform_silver_data(bronze_data)
os.makedirs("data/Silver", exist_ok=True)
silver_data.to_parquet(SILVER_FILE, index=False)

print("SILVER")
print(f"Rows: {len(silver_data)}")
print(silver_data.head())
print()

print(f"Rows removed: {len(bronze_data) - len(silver_data)}")
print(silver_data.dtypes)

# Gold
os.makedirs(GOLD_DIR, exist_ok=True)

gold_by_region = create_sales_by_region(silver_data)
gold_by_category = create_sales_by_category(silver_data)
gold_by_year_month = create_sales_by_year_and_month(silver_data)

gold_by_region.to_parquet(GOLD_BY_REGION, index=False)
gold_by_category.to_parquet(GOLD_BY_CATEGORY, index=False)
gold_by_year_month.to_parquet(GOLD_BY_YEAR_MONTH, index=False)

print()
print("GOLD - Sales by Region")
print(gold_by_region)
print()
print("GOLD - Sales by Category")
print(gold_by_category)
print()
print("GOLD - Sales by Year and Month")
print(gold_by_year_month)

