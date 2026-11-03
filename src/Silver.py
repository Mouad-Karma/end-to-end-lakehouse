import pandas as pd
def load_silver_data(file_path):
    """
    Load data from a Parquet file into a pandas DataFrame.
    """
    try:
        data = pd.read_parquet(file_path)
        return data
    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")
        return None

def standardize_dates(data):
    data["order_date"] = pd.to_datetime(
        data["order_date"],
        format="mixed",
        dayfirst=True,
        errors="coerce"
    )

    data["order_year"] = data["order_date"].dt.year
    data["order_month"] = data["order_date"].dt.month
    data["order_month_name"] = data["order_date"].dt.month_name()
    data["order_quarter"] = "Q" + data["order_date"].dt.quarter.astype(str)
    data["order_day_name"] = data["order_date"].dt.day_name()
    data["is_weekend"] = data["order_date"].dt.dayofweek >= 5

    return data

def standardize_column_names(data):
    """
    Standardize the column names in the DataFrame.
    """
    data.columns = (
        data.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return data

def transform_silver_data(data):
        # clean and transform the data as needed
        data=standardize_column_names(data)
        data=standardize_dates(data)
        data=data.drop_duplicates()
        data=data.dropna(subset=["order_date", "order_id", "customer_name"])
        
        return data

