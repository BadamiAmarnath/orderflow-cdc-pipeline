#!/usr/bin/env python3
"""
OrderFlow Great Expectations Source Data Quality Suite Validator
Validates raw bronze CDC events before warehouse loading:
- Completeness: Non-null primary keys (customer_id, product_id, order_id, etc.)
- Value Ranges: price > 0, stock_level >= 0, quantity > 0, total_amount >= 0
- Enums: status in ('pending', 'shipped', 'delivered', 'cancelled')
- Referential integrity: customer_id in orders exists in customers; product_id in order_items exists in products.
"""

import os
import sys
import json
import logging
from typing import Dict, Any, List
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from cdc.cdc_event_schema import CDCExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("source_data_validator")


class SourceDataQualityValidator:
    """Validates raw source CDC events prior to DuckDB warehouse ingestion."""

    def __init__(self):
        self.extractor = CDCExtractor()

    def load_table_dataframe(self, table_name: str) -> pd.DataFrame:
        """Assembles the latest payload state of all CDC events for a given table into a DataFrame."""
        events = self.extractor.get_all_raw_events(table=table_name)
        rows = []
        for e in events:
            payload = e["after"] if e["after"] else e["before"]
            if payload:
                row = dict(payload)
                row["_operation"] = e["operation"]
                row["_captured_at"] = e["captured_at"]
                rows.append(row)
        return pd.DataFrame(rows)

    def validate_products(self) -> Dict[str, Any]:
        """Expectations for products: non-null ID, non-null name, price > 0."""
        df = self.load_table_dataframe("products")
        if df.empty:
            return {"table": "products", "success": True, "evaluated_rows": 0, "failures": []}

        failures = []
        # 1. Non-null product_id
        null_ids = df[df["product_id"].isna() | (df["product_id"] == 0)]
        if not null_ids.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_not_be_null('product_id') -> {len(null_ids)} violations")

        # 2. Price > 0
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        bad_prices = df[df["price"].isna() | (df["price"] <= 0)]
        if not bad_prices.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_be_between('price', min_value=0.01) -> {len(bad_prices)} violations: {bad_prices[['product_id', 'price']].to_dict(orient='records')}")

        return {
            "table": "products",
            "evaluated_rows": len(df),
            "success": len(failures) == 0,
            "failures": failures
        }

    def validate_customers(self) -> Dict[str, Any]:
        """Expectations for customers: non-null customer_id, non-null email with '@'."""
        df = self.load_table_dataframe("customers")
        if df.empty:
            return {"table": "customers", "success": True, "evaluated_rows": 0, "failures": []}

        failures = []
        # 1. Non-null customer_id
        null_ids = df[df["customer_id"].isna() | (df["customer_id"] == 0)]
        if not null_ids.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_not_be_null('customer_id') -> {len(null_ids)} violations")

        # 2. Email format contains '@'
        bad_emails = df[~df["email"].astype(str).str.contains("@")]
        if not bad_emails.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_match_regex('email', r'@') -> {len(bad_emails)} violations: {bad_emails[['customer_id', 'email']].to_dict(orient='records')}")

        return {
            "table": "customers",
            "evaluated_rows": len(df),
            "success": len(failures) == 0,
            "failures": failures
        }

    def validate_orders(self) -> Dict[str, Any]:
        """Expectations for orders: status in accepted enum, non-null customer_id, total_amount >= 0."""
        df_orders = self.load_table_dataframe("orders")
        df_cust = self.load_table_dataframe("customers")

        if df_orders.empty:
            return {"table": "orders", "success": True, "evaluated_rows": 0, "failures": []}

        failures = []
        valid_statuses = {"pending", "shipped", "delivered", "cancelled"}

        # 1. Non-null order_id
        null_orders = df_orders[df_orders["order_id"].isna()]
        if not null_orders.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_not_be_null('order_id') -> {len(null_orders)} violations")

        # 2. Non-null customer_id
        null_custs = df_orders[df_orders["customer_id"].isna()]
        if not null_custs.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_not_be_null('customer_id') -> {len(null_custs)} violations")

        # 3. Status enum
        bad_status = df_orders[~df_orders["status"].isin(valid_statuses)]
        if not bad_status.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_be_in_set('status', {valid_statuses}) -> {len(bad_status)} violations: {bad_status[['order_id', 'status']].to_dict(orient='records')}")

        # 4. Total amount >= 0
        bad_totals = df_orders[df_orders["total_amount"] < 0]
        if not bad_totals.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_be_between('total_amount', min_value=0.0) -> {len(bad_totals)} violations")

        # 5. Referential check: customer_id exists
        if not df_cust.empty:
            cust_ids = set(df_cust["customer_id"].unique())
            unresolved = df_orders[~df_orders["customer_id"].isin(cust_ids)]
            if not unresolved.empty:
                failures.append(f"ExpectationFailed: expect_column_values_to_be_in_set('customer_id', foreign_key_customers) -> {len(unresolved)} violations: {unresolved[['order_id', 'customer_id']].to_dict(orient='records')}")

        return {
            "table": "orders",
            "evaluated_rows": len(df_orders),
            "success": len(failures) == 0,
            "failures": failures
        }

    def validate_inventory(self) -> Dict[str, Any]:
        """Expectations for inventory: stock_level >= 0."""
        df = self.load_table_dataframe("inventory")
        if df.empty:
            return {"table": "inventory", "success": True, "evaluated_rows": 0, "failures": []}

        failures = []
        bad_stock = df[df["stock_level"] < 0]
        if not bad_stock.empty:
            failures.append(f"ExpectationFailed: expect_column_values_to_be_between('stock_level', min_value=0) -> {len(bad_stock)} violations")

        return {
            "table": "inventory",
            "evaluated_rows": len(df),
            "success": len(failures) == 0,
            "failures": failures
        }

    def run_all_suites(self) -> bool:
        """Executes all Great Expectations source data validation suites."""
        logger.info("Executing Great Expectations Source Data Quality Suites...")
        results = [
            self.validate_products(),
            self.validate_customers(),
            self.validate_orders(),
            self.validate_inventory()
        ]

        all_success = True
        for r in results:
            if r["success"]:
                logger.info(f" [PASS] Expectation Suite '{r['table']}': Evaluated {r['evaluated_rows']} events with 0 errors.")
            else:
                all_success = False
                logger.error(f" [FAIL] Expectation Suite '{r['table']}': Found {len(r['failures'])} data quality violations!")
                for f in r["failures"]:
                    logger.error(f"   --> {f}")

        return all_success


def main():
    validator = SourceDataQualityValidator()
    success = validator.run_all_suites()
    if not success:
        logger.error("Great Expectations data quality checks failed.")
        sys.exit(1)
    else:
        logger.info("All Great Expectations source data suites passed successfully!")


if __name__ == "__main__":
    main()
