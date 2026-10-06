import pandas as pd


def create_sales_by_region(data):
    """
    Aggregate sales data by region.

    Returns a DataFrame with total sales, profit, orders,
    average discount, profit margin, and average order value per region.
    """
    result = (
        data.groupby("region", as_index=False)
        .agg(
            total_sales=("sales", "sum"),
            total_profit=("profit", "sum"),
            total_orders=("order_id", "nunique"),
            average_discount=("discount", "mean")
        )
    )

    result["profit_margin"] = (result["total_profit"] / result["total_sales"]).round(4)
    result["average_order_value"] = (result["total_sales"] / result["total_orders"]).round(2)
    result["average_discount"] = result["average_discount"].round(4)

    return result


def create_sales_by_category(data):
    """
    Aggregate sales data by product category.

    Returns a DataFrame with total sales, profit, orders,
    average discount, profit margin, and average order value per category.
    """
    result = (
        data.groupby("category", as_index=False)
        .agg(
            total_sales=("sales", "sum"),
            total_profit=("profit", "sum"),
            total_orders=("order_id", "nunique"),
            average_discount=("discount", "mean")
        )
    )

    result["profit_margin"] = (result["total_profit"] / result["total_sales"]).round(4)
    result["average_order_value"] = (result["total_sales"] / result["total_orders"]).round(2)
    result["average_discount"] = result["average_discount"].round(4)

    return result


def create_sales_by_year_and_month(data):
    """
    Aggregate sales data by year and month.

    Returns a DataFrame sorted chronologically with total sales, profit,
    orders, average discount, profit margin, and average order value.
    """
    result = (
        data.groupby(["order_year", "order_month"], as_index=False)
        .agg(
            total_sales=("sales", "sum"),
            total_profit=("profit", "sum"),
            total_orders=("order_id", "nunique"),
            average_discount=("discount", "mean")
        )
        .sort_values(by=["order_year", "order_month"])
    )

    result["profit_margin"] = (result["total_profit"] / result["total_sales"]).round(4)
    result["average_order_value"] = (result["total_sales"] / result["total_orders"]).round(2)
    result["average_discount"] = result["average_discount"].round(4)

    return result