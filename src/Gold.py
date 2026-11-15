"""Gold tables: join Silver data and calculate business summaries."""
from pyspark.sql import functions as F


def create_sales_monthly(orders, order_items):
    """Monthly sales for delivered orders with items; freight is separate."""
    orders_filtered = orders.filter(F.col("order_status") == "delivered")
    orders_sales_info = order_items.groupBy("order_id").agg(
        F.sum("price").alias("total_price"),
        F.sum("freight_value").alias("total_freight_value"),
    )
    sales_df = orders_filtered.join(orders_sales_info, on="order_id", how="inner")
    return sales_df.groupBy("purchase_year", "purchase_month").agg(
        F.count("*").alias("total_orders"),
        F.sum("total_price").alias("total_revenue"),
        F.sum("total_freight_value").alias("total_freight"),
    ).orderBy("purchase_year", "purchase_month")


def create_fact_orders(orders, customers, order_items, order_payments, order_reviews):
    """One row per order. Orders and customers need unique IDs."""
    # Sum each child table first so joins do not multiply money amounts.
    items = order_items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.sum("price").alias("total_revenue"),
        F.sum("freight_value").alias("total_freight"),
    )
    payments = order_payments.groupBy("order_id").agg(
        F.count("*").alias("payment_count"),
        F.sum("payment_value").alias("total_payment"),
    )
    reviews = order_reviews.groupBy("order_id").agg(
        F.count("*").alias("review_count"),
        F.avg("review_score").alias("average_review_score"),
    )
    data = orders.join(customers, "customer_id", "left")
    data = data.join(items, "order_id", "left")
    data = data.join(payments, "order_id", "left")
    data = data.join(reviews, "order_id", "left")
    # Missing items, payments or reviews stay null.
    return data


def create_fact_order_items(order_items, orders, products, sellers):
    """One row per item. Orders, products and sellers need unique IDs."""
    order_info = orders.select(
        "order_id", "order_status", "purchase_year", "purchase_month",
    )
    data = order_items.join(order_info, "order_id", "left")
    data = data.join(products, "product_id", "left")
    data = data.join(sellers, "seller_id", "left")
    # Use the Portuguese category if the English translation is missing.
    return data.withColumn("category", F.coalesce(
        "product_category_name_english", "product_category_name", F.lit("unknown"),
    ))


def create_sales_by_category(fact_order_items):
    """Sales by category for delivered orders."""
    delivered = fact_order_items.filter(F.col("order_status") == "delivered")
    return delivered.groupBy("category").agg(
        F.sum("price").alias("total_revenue"),
        F.sum("freight_value").alias("total_freight"),
        F.count("*").alias("total_items"),
        F.countDistinct("order_id").alias("total_orders"),
    ).orderBy("category")


def create_seller_performance(fact_order_items):
    """Sales by seller for delivered orders."""
    delivered = fact_order_items.filter(F.col("order_status") == "delivered")
    return delivered.groupBy("seller_id").agg(
        F.sum("price").alias("total_revenue"),
        F.sum("freight_value").alias("total_freight"),
        F.count("*").alias("total_items"),
        F.countDistinct("order_id").alias("total_orders"),
    ).orderBy("seller_id")


def create_delivery_review_monthly(fact_orders):
    """Monthly delivery and review averages for delivered orders."""
    delivered = fact_orders.filter(F.col("order_status") == "delivered")
    # True becomes 1, false becomes 0; avg ignores missing delivery flags.
    delivered = delivered.withColumn("late_flag", F.col("is_late_delivery").cast("int"))
    return delivered.groupBy("purchase_year", "purchase_month").agg(
        F.count("*").alias("total_orders"),
        F.count("delivery_days").alias("delivery_count"),
        F.avg("delivery_days").alias("average_delivery_days"),
        F.count("late_flag").alias("eligible_delivery_count"),
        F.sum(F.coalesce("late_flag", F.lit(0))).alias("late_orders"),
        F.avg("late_flag").alias("late_rate"),
        F.count("average_review_score").alias("reviewed_orders"),
        # Each scored order has equal weight, even if it has multiple reviews.
        F.avg("average_review_score").alias("average_review_score"),
    ).orderBy("purchase_year", "purchase_month")


def transform_gold_data(tables):
    """Build six Gold tables from Silver; the caller handles persistence."""
    fact_orders = create_fact_orders(
        tables["orders"], tables["customers"], tables["order_items"],
        tables["order_payments"], tables["order_reviews"],
    )
    fact_items = create_fact_order_items(
        tables["order_items"], tables["orders"], tables["products"], tables["sellers"],
    )
    return {
        "fact_orders": fact_orders,
        "fact_order_items": fact_items,
        "sales_monthly": create_sales_monthly(tables["orders"], tables["order_items"]),
        "sales_by_category": create_sales_by_category(fact_items),
        "seller_performance": create_seller_performance(fact_items),
        "delivery_review_monthly": create_delivery_review_monthly(fact_orders),
    }
