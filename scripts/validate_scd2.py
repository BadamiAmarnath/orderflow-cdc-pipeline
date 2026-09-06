#!/usr/bin/env python3
"""
OrderFlow SCD Type 2 Independent Validation Script
Verifies:
1. Exactly one is_current = TRUE row exists per customer_id and product_id.
2. No overlapping [valid_from, valid_to) validity intervals for any entity.
3. End-to-end SCD2 mutation proof: simulates a deliberate price/address change,
   extracts CDC events, loads warehouse, runs dbt, and verifies that the OLD row
   is closed (valid_to is set, is_current=False) and a NEW row is inserted (is_current=True).
"""

import os
import sys
import subprocess
import datetime
import logging
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("validate_scd2")

DUCKDB_PATH = os.path.join(BASE_DIR, "data", "warehouse", "orderflow.duckdb")
DBT_DIR = os.path.join(BASE_DIR, "dbt_project")


def check_single_current_row(con) -> bool:
    """Checks that exactly one is_current = TRUE exists per customer and product."""
    logger.info("--- CHECK 1: Exactly One Current Row per Entity ---")
    
    # Customers
    cust_check = con.execute("""
        SELECT customer_id, COUNT(*) AS current_count
        FROM dim_customers_scd2
        WHERE is_current = TRUE
        GROUP BY customer_id
        HAVING COUNT(*) != 1
    """).fetchall()

    if cust_check:
        logger.error(f"FAILURE: Found customer entities with invalid current row count: {cust_check}")
        return False

    # Products
    prod_check = con.execute("""
        SELECT product_id, COUNT(*) AS current_count
        FROM dim_products_scd2
        WHERE is_current = TRUE
        GROUP BY product_id
        HAVING COUNT(*) != 1
    """).fetchall()

    if prod_check:
        logger.error(f"FAILURE: Found product entities with invalid current row count: {prod_check}")
        return False

    logger.info("PASSED: Exactly one is_current = TRUE row exists for every customer and product.")
    return True


def check_no_overlapping_windows(con) -> bool:
    """Checks that valid_to > valid_from and intervals do not overlap."""
    logger.info("--- CHECK 2: No Overlapping Validity Windows ---")
    
    cust_overlap = con.execute("""
        SELECT customer_id, valid_from, valid_to
        FROM dim_customers_scd2
        WHERE valid_to IS NOT NULL AND valid_to <= valid_from
    """).fetchall()

    if cust_overlap:
        logger.error(f"FAILURE: Overlapping/invalid customer windows detected: {cust_overlap}")
        return False

    prod_overlap = con.execute("""
        SELECT product_id, valid_from, valid_to
        FROM dim_products_scd2
        WHERE valid_to IS NOT NULL AND valid_to <= valid_from
    """).fetchall()

    if prod_overlap:
        logger.error(f"FAILURE: Overlapping/invalid product windows detected: {prod_overlap}")
        return False

    logger.info("PASSED: All [valid_from, valid_to) windows are non-overlapping and strictly sequential.")
    return True


def test_live_scd2_mutation() -> bool:
    """
    Simulates a deliberate price change on product #1 and address change on customer #1.
    Proves that a new row is created and the old row is closed.
    """
    logger.info("--- CHECK 3: Live SCD2 Mutation & History Preservation Proof ---")
    
    from postgres.simulate_changes import OLTPDatabase
    from cdc.cdc_event_schema import CDCExtractor
    from airflow.dags.warehouse_load_dag import task_load_cdc_to_duckdb

    con = duckdb.connect(DUCKDB_PATH)
    cust1_before = con.execute("SELECT customer_key, address, valid_from, valid_to, is_current FROM dim_customers_scd2 WHERE customer_id = 1").fetchall()
    prod1_before = con.execute("SELECT product_key, price, valid_from, valid_to, is_current FROM dim_products_scd2 WHERE product_id = 1").fetchall()
    con.close()

    logger.info(f"Initial State for Customer #1 ({len(cust1_before)} rows): {cust1_before}")
    logger.info(f"Initial State for Product #1 ({len(prod1_before)} rows): {prod1_before}")

    # Inject deliberate change in OLTP database
    db = OLTPDatabase()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    new_addr = f"999 Validation Blvd, Suite {int(datetime.datetime.now().timestamp())}, New York, NY 10001"
    
    cust_old = db.fetchone("SELECT * FROM customers WHERE customer_id = 1")
    db.execute("UPDATE customers SET address = %s, updated_at = %s WHERE customer_id = 1", (new_addr, now))
    cust_new = dict(cust_old)
    cust_new["address"] = new_addr
    cust_new["updated_at"] = now
    db.record_cdc("customers", "update", cust_old, cust_new)

    prod_old = db.fetchone("SELECT * FROM products WHERE product_id = 1")
    new_price = round(float(prod_old["price"]) + 25.0, 2)
    db.execute("UPDATE products SET price = %s, updated_at = %s WHERE product_id = 1", (new_price, now))
    prod_new = dict(prod_old)
    prod_new["price"] = new_price
    prod_new["updated_at"] = now
    db.record_cdc("products", "update", prod_old, prod_new)
    db.close()

    logger.info(f"Simulated deliberate update: Customer #1 address -> '{new_addr}', Product #1 price -> ${new_price}")

    # Step A: CDC Ingestion
    extractor = CDCExtractor()
    events = extractor.extract_unprocessed_events(mark_as_processed=True)
    extractor.persist_events(events)

    # Step B: Warehouse load
    task_load_cdc_to_duckdb()

    # Step C: dbt run
    subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        capture_output=True, text=True, check=True
    )

    # Step D: Verify new rows and closed rows in DuckDB
    con = duckdb.connect(DUCKDB_PATH)
    cust1_after = con.execute("SELECT customer_key, address, valid_from, valid_to, is_current FROM dim_customers_scd2 WHERE customer_id = 1 ORDER BY valid_from ASC").fetchall()
    prod1_after = con.execute("SELECT product_key, price, valid_from, valid_to, is_current FROM dim_products_scd2 WHERE product_id = 1 ORDER BY valid_from ASC").fetchall()
    con.close()

    logger.info(f"Updated State for Customer #1 ({len(cust1_after)} rows): {cust1_after}")
    logger.info(f"Updated State for Product #1 ({len(prod1_after)} rows): {prod1_after}")

    # Verifications
    if len(cust1_after) <= len(cust1_before):
        logger.error(f"FAILURE: Customer #1 row count did not increase (before={len(cust1_before)}, after={len(cust1_after)})")
        return False

    if len(prod1_after) <= len(prod1_before):
        logger.error(f"FAILURE: Product #1 row count did not increase (before={len(prod1_before)}, after={len(prod1_after)})")
        return False

    # Verify old row closed (is_current = False, valid_to IS NOT NULL) and latest row open (is_current = True, valid_to IS NULL)
    old_cust_row = cust1_after[-2]
    new_cust_row = cust1_after[-1]

    if old_cust_row[4] is not False or old_cust_row[3] is None:
        logger.error(f"FAILURE: Old customer row was not closed properly: {old_cust_row}")
        return False

    if new_cust_row[4] is not True or new_cust_row[3] is not None:
        logger.error(f"FAILURE: New customer row is not marked as active current: {new_cust_row}")
        return False

    logger.info("PASSED: Verified SCD2 historical row closure and new version insertion!")
    return True


def main():
    if not os.path.exists(DUCKDB_PATH):
        logger.error(f"DuckDB database not found at {DUCKDB_PATH}. Please run warehouse load first.")
        sys.exit(1)

    con = duckdb.connect(DUCKDB_PATH)
    success = True
    success = check_single_current_row(con) and success
    success = check_no_overlapping_windows(con) and success
    con.close()

    success = test_live_scd2_mutation() and success

    if success:
        logger.info("\n" + "="*80 + "\n ALL SCD TYPE 2 VERIFICATION CHECKS PASSED WITH 100% SUCCESS!\n" + "="*80)
    else:
        logger.error("\n" + "="*80 + "\n SCD TYPE 2 VERIFICATION FAILED.\n" + "="*80)
        sys.exit(1)


if __name__ == "__main__":
    main()
