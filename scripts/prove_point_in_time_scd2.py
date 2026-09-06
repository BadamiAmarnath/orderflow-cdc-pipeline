#!/usr/bin/env python3
"""
OrderFlow: Point-in-Time SCD2 Dimension Key Resolution Proof
Demonstrates that fct_orders links transactions to the exact historical
dimension state valid at order_date, not just the latest/current state:
1. Customer #1 created with Address A on 2026-08-01.
2. Order #101 placed by Customer #1 on 2026-08-10 (BEFORE address change).
3. Customer #1 updates address to Address B on 2026-08-20.
4. Order #102 placed by Customer #1 on 2026-08-25 (AFTER address change).
5. dbt builds dim_customers_scd2 and fct_orders.
6. Proves:
   - Order #101 resolves to Version 1 customer_key (is_current=False, Address A)
   - Order #102 resolves to Version 2 customer_key (is_current=True, Address B)
"""

import os
import sys
import json
import subprocess
import datetime
import logging
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from postgres.simulate_changes import OLTPDatabase, seed_database
from cdc.cdc_event_schema import CDCExtractor
from airflow.dags.warehouse_load_dag import task_load_cdc_to_duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("point_in_time_proof")

DUCKDB_PATH = os.path.join(BASE_DIR, "data", "warehouse", "orderflow.duckdb")
DBT_DIR = os.path.join(BASE_DIR, "dbt_project")


def prove_point_in_time():
    logger.info("=" * 80)
    logger.info("PROVING POINT-IN-TIME SCD2 DIMENSION RESOLUTION IN fct_orders")
    logger.info("=" * 80)

    # 1. Clean Seed
    db = OLTPDatabase()
    seed_database(db)

    # 2. Place Order #101 for Customer #1 (BEFORE address change)
    order1_date = "2026-08-10T12:00:00+00:00"
    db.execute(
        "INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES (%s, %s, %s, %s)",
        (1, order1_date, "delivered", 199.99)
    )
    db.record_cdc("orders", "insert", None, {
        "order_id": 101, "customer_id": 1, "order_date": order1_date,
        "status": "delivered", "total_amount": 199.99
    })
    db.record_cdc("order_items", "insert", None, {
        "order_item_id": 201, "order_id": 101, "product_id": 1, "quantity": 1, "unit_price": 199.99
    })

    # 3. Customer #1 updates address on 2026-08-20 (Creates Version 2 in SCD2)
    change_date = "2026-08-20T10:00:00+00:00"
    new_address = "777 Broadway, Fl 14, New York, NY 10003"
    cust_old = db.fetchone("SELECT * FROM customers WHERE customer_id = 1")
    db.execute("UPDATE customers SET address = %s, updated_at = %s WHERE customer_id = 1", (new_address, change_date))
    cust_new = dict(cust_old)
    cust_new["address"] = new_address
    cust_new["updated_at"] = change_date
    db.record_cdc("customers", "update", cust_old, cust_new)

    # 4. Place Order #102 for Customer #1 (AFTER address change)
    order2_date = "2026-08-25T15:00:00+00:00"
    db.execute(
        "INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES (%s, %s, %s, %s)",
        (1, order2_date, "shipped", 349.50)
    )
    db.record_cdc("orders", "insert", None, {
        "order_id": 102, "customer_id": 1, "order_date": order2_date,
        "status": "shipped", "total_amount": 349.50
    })
    db.record_cdc("order_items", "insert", None, {
        "order_item_id": 202, "order_id": 102, "product_id": 2, "quantity": 1, "unit_price": 349.50
    })
    db.close()

    # 5. Extract & Load to DuckDB
    extractor = CDCExtractor()
    events = extractor.extract_unprocessed_events(mark_as_processed=True)
    extractor.persist_events(events)
    task_load_cdc_to_duckdb()

    # 6. Run dbt
    subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        capture_output=True, text=True, check=True
    )

    # 7. Query DuckDB to prove PIT Resolution
    con = duckdb.connect(DUCKDB_PATH)

    print("\n" + "="*80)
    print("STEP A: Historical Customer SCD2 Versions for Customer #1 (dim_customers_scd2):")
    print("="*80)
    df_cust = con.execute("""
        SELECT customer_key, customer_id, name, address, valid_from, valid_to, is_current
        FROM dim_customers_scd2
        WHERE customer_id = 1
        ORDER BY valid_from ASC
    """).df()
    print(df_cust.to_string(index=False))

    print("\n" + "="*80)
    print("STEP B: Fact Orders for Customer #1 with Resolved Customer Keys (fct_orders):")
    print("="*80)
    df_orders = con.execute("""
        SELECT 
            f.order_id,
            f.customer_id,
            f.customer_key AS resolved_customer_key,
            f.order_date,
            c.address AS address_at_time_of_order,
            c.is_current AS is_currently_active_address_version,
            f.total_amount
        FROM fct_orders f
        JOIN dim_customers_scd2 c ON f.customer_key = c.customer_key
        WHERE f.customer_id = 1 AND f.order_id IN (101, 102)
        ORDER BY f.order_id ASC
    """).df()
    print(df_orders.to_string(index=False))
    print("="*80)

    # Assertions
    order1 = df_orders[df_orders["order_id"] == 101].iloc[0]
    order2 = df_orders[df_orders["order_id"] == 102].iloc[0]

    assert order1["resolved_customer_key"] != order2["resolved_customer_key"], "Keys must differ!"
    assert bool(order1["is_currently_active_address_version"]) is False, "Order 1 must link to OLD closed version"
    assert bool(order2["is_currently_active_address_version"]) is True, "Order 2 must link to CURRENT version"
    assert "San Francisco" in str(order1["address_at_time_of_order"]), "Order 1 must show San Francisco address"
    assert "New York" in str(order2["address_at_time_of_order"]), "Order 2 must show New York address"

    print("\n[PASSED] POINT-IN-TIME VERIFICATION PROVEN:")
    print(f"* Order #101 (placed {order1['order_date']}) resolved to OLD Version Key ({order1['resolved_customer_key']}) with Address: '{order1['address_at_time_of_order']}' (is_current=False)")
    print(f"* Order #102 (placed {order2['order_date']}) resolved to NEW Version Key ({order2['resolved_customer_key']}) with Address: '{order2['address_at_time_of_order']}' (is_current=True)")
    print("="*80 + "\n")
    con.close()


if __name__ == "__main__":
    prove_point_in_time()
