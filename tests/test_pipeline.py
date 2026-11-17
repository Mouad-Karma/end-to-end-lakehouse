import csv
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from main import build_spark, run_pipeline, write_table, PROJECT_DIR
from src.Bronze import SOURCE_FILES
from src.Gold import create_sales_monthly, transform_gold_data


class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = build_spark("local[2]", app_name="olist-test")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")
        cls.spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_monthly_sales_aggregates_items_and_filters_status(self):
        orders = self.spark.createDataFrame([
            ("a", "delivered", 2018, 2),
            ("b", "delivered", 2018, 1),
            ("c", "canceled", 2018, 1),
            ("d", "delivered", 2018, 1),  # No items: excluded by inner join.
        ], "order_id string, order_status string, purchase_year int, purchase_month int")
        items = self.spark.createDataFrame([
            ("a", Decimal("10.10"), Decimal("1.00")),
            ("a", Decimal("20.20"), Decimal("2.00")),
            ("b", Decimal("5.00"), Decimal("0.00")),
            ("c", Decimal("99.00"), Decimal("9.00")),
        ], "order_id string, price decimal(18,2), freight_value decimal(18,2)")
        rows = create_sales_monthly(orders, items).collect()
        self.assertEqual([tuple(row) for row in rows], [
            (2018, 1, 1, Decimal("5.00"), Decimal("0.00")),
            (2018, 2, 1, Decimal("30.30"), Decimal("3.00")),
        ])

    def test_gold_preserves_grain_and_metric_populations(self):
        tables = {
            "orders": self.spark.createDataFrame([
                ("a", "c", "delivered", 2018, 1, 10, True),
                ("b", "c", "delivered", 2018, 1, None, None),
                ("x", "c", "canceled", 2018, 1, None, None),
            ], "order_id string, customer_id string, order_status string, "
               "purchase_year int, purchase_month int, delivery_days int, is_late_delivery boolean"),
            "customers": self.spark.createDataFrame([("c", "SP")],
                "customer_id string, customer_state string"),
            "order_items": self.spark.createDataFrame([
                ("a", 1, "p", "s", Decimal("10.00"), Decimal("1.00")),
                ("a", 2, "q", "s", Decimal("20.00"), Decimal("2.00")),
                ("x", 1, "p", "s", Decimal("99.00"), Decimal("9.00")),
            ], "order_id string, order_item_id int, product_id string, seller_id string, "
               "price decimal(18,2), freight_value decimal(18,2)"),
            "order_payments": self.spark.createDataFrame([
                ("a", Decimal("11.00")), ("a", Decimal("22.00")),
            ], "order_id string, payment_value decimal(18,2)"),
            "order_reviews": self.spark.createDataFrame([
                ("a", 2), ("a", 4), ("b", 5),
            ], "order_id string, review_score int"),
            "products": self.spark.createDataFrame([
                ("p", "livros", "books"), ("q", "outros", None),
            ], "product_id string, product_category_name string, product_category_name_english string"),
            "sellers": self.spark.createDataFrame([("s", "RJ")],
                "seller_id string, seller_state string"),
        }
        gold = transform_gold_data(tables)
        orders = {row.order_id: row for row in gold["fact_orders"].collect()}
        self.assertEqual(len(orders), 3)
        self.assertEqual(orders["a"].total_revenue, Decimal("30.00"))
        self.assertEqual(orders["a"].total_payment, Decimal("33.00"))
        self.assertEqual(orders["a"].average_review_score, 3.0)
        self.assertIsNone(orders["b"].total_revenue)
        self.assertEqual(gold["fact_order_items"].count(), 3)
        categories = {row.category: row for row in gold["sales_by_category"].collect()}
        self.assertEqual(set(categories), {"books", "outros"})
        self.assertEqual(categories["books"].total_revenue, Decimal("10.00"))
        seller = gold["seller_performance"].first()
        self.assertEqual((seller.total_items, seller.total_orders), (2, 1))
        self.assertEqual(seller.total_revenue, Decimal("30.00"))
        month = gold["delivery_review_monthly"].first()
        self.assertEqual((month.total_orders, month.eligible_delivery_count), (2, 1))
        self.assertEqual(month.late_rate, 1.0)
        self.assertEqual(month.average_delivery_days, 10.0)
        self.assertEqual(month.average_review_score, 4.0)

    def test_all_layers_and_basic_cleaning(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            raw.mkdir()
            # Preserve the real nine schemas; small deterministic samples plus edge cases.
            for name, filename in SOURCE_FILES.items():
                with (PROJECT_DIR / "data/raw" / filename).open(encoding="utf-8-sig", newline="") as stream:
                    reader = csv.DictReader(stream)
                    row = next(reader)
                    if name == "orders":
                        row["order_delivered_customer_date"] = ""
                    elif name == "order_payments":
                        row["payment_installments"] = "0"
                        row["payment_type"] = "not_defined"
                    elif name == "products":
                        row["product_weight_g"] = "bad"
                        row["product_category_name"] = "missing_translation"
                    elif name == "order_reviews":
                        row["review_comment_message"] = 'A multiline\nreview, with "quotes"'
                    with (raw / filename).open("w", newline="") as out:
                        writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        writer.writerow(row)
                        if name == "geolocation":
                            writer.writerow(row)
                            row = dict(row, geolocation_state="RJ")
                            writer.writerow(row)
            output = root / "output"
            run_pipeline(self.spark, raw, output)

            def read(layer, name):
                return self.spark.read.format("delta").load(str(output / layer / name))

            def assert_table(layer, name):
                self.assertTrue((output / layer / name / "_delta_log").is_dir())

            self.assertEqual(len(list((output / "Bronze").iterdir())), 9)
            self.assertEqual(len(list((output / "Silver").iterdir())), 9)
            for layer in ("Bronze", "Silver"):
                for name in SOURCE_FILES:
                    assert_table(layer, name)
            gold_names = {
                "fact_orders", "fact_order_items", "sales_monthly",
                "sales_by_category", "seller_performance", "delivery_review_monthly",
            }
            self.assertEqual({path.stem for path in (output / "Gold").iterdir()}, gold_names)
            for name in gold_names:
                assert_table("Gold", name)
                read("Gold", name).collect()
            self.assertEqual(read("Gold", "fact_orders").count(), 1)
            self.assertEqual(read("Gold", "fact_order_items").count(), 1)
            self.assertEqual(read("Bronze", "geolocation").count(), 3)
            self.assertEqual(read("Silver", "geolocation").count(), 2)
            self.assertEqual(read("Silver", "geolocation").first().geolocation_zip_code_prefix, "01037")
            order = read("Silver", "orders").first()
            self.assertIsNone(order.order_delivered_customer_date)
            self.assertIsNone(order.is_late_delivery)
            self.assertEqual(order.purchase_year, 2017)
            payment = read("Silver", "order_payments").first()
            self.assertEqual(payment.payment_installments, 0)
            product = read("Silver", "products").first()
            self.assertIsNone(product.product_category_name_english)
            self.assertIsNone(product.product_weight_g)
            review = read("Silver", "order_reviews").first()
            self.assertEqual(review.review_comment_message, 'A multiline\nreview, with "quotes"')
            item = read("Silver", "order_items").first()
            self.assertEqual(item.item_total, Decimal("72.19"))

    def test_delta_overwrite_retains_previous_version(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "orders")
            write_table(self.spark.range(1), path)
            write_table(self.spark.range(2), path)
            self.assertEqual(self.spark.read.format("delta").load(path).count(), 2)
            previous = self.spark.read.format("delta").option("versionAsOf", 0).load(path)
            self.assertEqual(previous.count(), 1)


if __name__ == "__main__":
    unittest.main()
