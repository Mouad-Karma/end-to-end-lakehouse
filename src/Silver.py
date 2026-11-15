"""Simple PySpark cleaning: one function per Olist dataset."""
from pyspark.sql import functions as F


def clean_common_columns(data):
    """Clean headers, trim text, turn blanks into nulls and remove exact duplicates."""
    for column in data.columns:
        clean_name = column.lstrip("\ufeff").strip().lower()
        data = data.withColumnRenamed(column, clean_name)

    for column in data.columns:
        # Keep review messages as written, including their line breaks.
        value = F.col(column) if column.startswith("review_comment_") else F.trim(F.col(column))
        data = data.withColumn(column, F.when(F.length(F.trim(F.col(column))) > 0, value))

    return data.dropDuplicates()


def transform_customers(data):
    """Clean customer locations; keep both customer IDs and ZIP strings."""
    data = clean_common_columns(data)
    data = data.withColumn("customer_city", F.lower("customer_city"))
    data = data.withColumn("customer_state", F.upper("customer_state"))
    return data


def transform_orders(data):
    """Convert dates and add purchase month/year and delivery information."""
    data = clean_common_columns(data)
    date_columns = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for column in date_columns:
        data = data.withColumn(column, F.try_to_timestamp(column, F.lit("yyyy-MM-dd HH:mm:ss")))

    data = data.withColumn("order_status", F.lower("order_status"))
    data = data.withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
    data = data.withColumn("purchase_year", F.year("order_purchase_timestamp"))
    data = data.withColumn("purchase_month", F.month("order_purchase_timestamp"))
    data = data.withColumn("delivery_days", F.datediff(
        "order_delivered_customer_date", "order_purchase_timestamp"))
    data = data.withColumn("is_late_delivery",
        F.to_date("order_delivered_customer_date") > F.to_date("order_estimated_delivery_date"))
    return data


def transform_order_items(data):
    """Convert item numbers, money and shipping dates; add the item total."""
    data = clean_common_columns(data)
    data = data.withColumn("order_item_id", F.expr("try_cast(order_item_id as int)"))
    data = data.withColumn("price", F.expr("try_cast(price as decimal(18,2))"))
    data = data.withColumn("freight_value", F.expr("try_cast(freight_value as decimal(18,2))"))
    data = data.withColumn("shipping_limit_date",
        F.try_to_timestamp("shipping_limit_date", F.lit("yyyy-MM-dd HH:mm:ss")))
    data = data.withColumn("item_total", F.col("price") + F.col("freight_value"))
    return data


def transform_order_payments(data):
    """Convert payment numbers and amounts; normalize payment type."""
    data = clean_common_columns(data)
    data = data.withColumn("payment_sequential", F.expr("try_cast(payment_sequential as int)"))
    data = data.withColumn("payment_installments", F.expr("try_cast(payment_installments as int)"))
    data = data.withColumn("payment_value", F.expr("try_cast(payment_value as decimal(18,2))"))
    data = data.withColumn("payment_type", F.lower("payment_type"))
    return data


def transform_order_reviews(data):
    """Convert review scores and dates; keep reviews with missing comments."""
    data = clean_common_columns(data)
    data = data.withColumn("review_score", F.expr("try_cast(review_score as int)"))
    data = data.withColumn("review_creation_date",
        F.try_to_timestamp("review_creation_date", F.lit("yyyy-MM-dd HH:mm:ss")))
    data = data.withColumn("review_answer_timestamp",
        F.try_to_timestamp("review_answer_timestamp", F.lit("yyyy-MM-dd HH:mm:ss")))
    return data


def transform_category_translation(data):
    """Clean Portuguese and English category labels."""
    data = clean_common_columns(data)
    data = data.withColumn("product_category_name", F.lower("product_category_name"))
    data = data.withColumn("product_category_name_english", F.lower("product_category_name_english"))
    return data


def transform_products(data, translations):
    """Fix header typos, convert measurements and attach English categories."""
    data = clean_common_columns(data)
    data = data.withColumnRenamed("product_name_lenght", "product_name_length")
    data = data.withColumnRenamed("product_description_lenght", "product_description_length")
    number_columns = [
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    ]
    for column in number_columns:
        data = data.withColumn(column, F.expr(f"try_cast({column} as int)"))

    data = data.withColumn("product_category_name", F.lower("product_category_name"))
    data = data.withColumn("product_volume_cm3", F.col("product_length_cm").cast("long")
        * F.col("product_height_cm") * F.col("product_width_cm"))
    # A category must have one translation, otherwise a join could multiply products.
    if translations.groupBy("product_category_name").count().filter("count > 1").limit(1).count():
        raise ValueError("Category translation contains multiple mappings for the same category")
    return data.join(translations, "product_category_name", "left")


def transform_sellers(data):
    """Clean seller locations and preserve ZIP prefixes as strings."""
    data = clean_common_columns(data)
    data = data.withColumn("seller_city", F.lower("seller_city"))
    data = data.withColumn("seller_state", F.upper("seller_state"))
    return data


def transform_geolocation(data):
    """Remove duplicate locations, clean labels and convert coordinates."""
    data = clean_common_columns(data)
    data = data.withColumn("geolocation_city", F.lower("geolocation_city"))
    data = data.withColumn("geolocation_state", F.upper("geolocation_state"))
    data = data.withColumn("geolocation_lat", F.expr("try_cast(geolocation_lat as double)"))
    data = data.withColumn("geolocation_lng", F.expr("try_cast(geolocation_lng as double)"))
    return data


def transform_silver_data(tables):
    """Call each dataset's transformation and return the nine cleaned tables."""
    translations = transform_category_translation(tables["category_translation"])
    return {
        "customers": transform_customers(tables["customers"]),
        "orders": transform_orders(tables["orders"]),
        "order_items": transform_order_items(tables["order_items"]),
        "order_payments": transform_order_payments(tables["order_payments"]),
        "order_reviews": transform_order_reviews(tables["order_reviews"]),
        "products": transform_products(tables["products"], translations),
        "sellers": transform_sellers(tables["sellers"]),
        "geolocation": transform_geolocation(tables["geolocation"]),
        "category_translation": translations,
    }
