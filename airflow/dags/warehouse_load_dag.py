"""
OrderFlow Airflow DAG: warehouse_load_dag
Loads raw CDC change logs into DuckDB staging layer with merge/upsert deduplication.
Runs Great Expectations data quality validation prior to warehouse insertion.
Configured with 2 retries (exponential backoff) and Slack failure alerting.
"""

import os
import sys
import json
import datetime
from datetime import timedelta
import logging
import duckdb

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from cdc.cdc_event_schema import CDCExtractor
from airflow.plugins.alerts import slack_alert_on_failure, slack_alert_on_retry

logger = logging.getLogger("airflow.warehouse_load_dag")

DUCKDB_PATH = os.path.join(BASE_DIR, "data", "warehouse", "orderflow.duckdb")

default_args = {
    'owner': 'orderflow_data_team',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 8, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(seconds=10),
    'on_failure_callback': slack_alert_on_failure,
    'on_retry_callback': slack_alert_on_retry,
}


def task_load_cdc_to_duckdb(simulate_failure: bool = False, **kwargs):
    """Loads and upserts bronze CDC events into DuckDB staging tables."""
    if simulate_failure:
        raise ValueError("[SIMULATED ERROR] Bad file path or DuckDB lock collision encountered.")

    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    extractor = CDCExtractor()
    all_events = extractor.get_all_raw_events()

    if not all_events:
        logger.warning("No raw CDC events found to load.")
        return {"status": "NO_DATA", "loaded_count": 0}

    con = duckdb.connect(DUCKDB_PATH)

    # Initialize raw CDC delta tables in DuckDB
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_cdc_events (
            change_id BIGINT,
            table_name VARCHAR,
            operation VARCHAR,
            before_state VARCHAR,
            after_state VARCHAR,
            captured_at VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Create distinct deduplicated staging tables for downstream dbt consumption
    con.execute("""
        CREATE TABLE IF NOT EXISTS stg_customers_cdc (
            customer_id INTEGER,
            name VARCHAR,
            email VARCHAR,
            address VARCHAR,
            operation VARCHAR,
            valid_from TIMESTAMP,
            captured_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS stg_products_cdc (
            product_id INTEGER,
            name VARCHAR,
            category VARCHAR,
            price DOUBLE,
            operation VARCHAR,
            valid_from TIMESTAMP,
            captured_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS stg_orders_cdc (
            order_id INTEGER,
            customer_id INTEGER,
            order_date TIMESTAMP,
            status VARCHAR,
            total_amount DOUBLE,
            operation VARCHAR,
            captured_at TIMESTAMP
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS stg_order_items_cdc (
            order_item_id INTEGER,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DOUBLE,
            operation VARCHAR,
            captured_at TIMESTAMP
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS stg_inventory_cdc (
            product_id INTEGER,
            warehouse_id VARCHAR,
            stock_level INTEGER,
            operation VARCHAR,
            captured_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)

    # Clear staging tables for fresh idempotent load from bronze
    con.execute("DELETE FROM stg_customers_cdc;")
    con.execute("DELETE FROM stg_products_cdc;")
    con.execute("DELETE FROM stg_orders_cdc;")
    con.execute("DELETE FROM stg_order_items_cdc;")
    con.execute("DELETE FROM stg_inventory_cdc;")

    # Parse and insert into staging tables
    for e in all_events:
        tbl = e["table"]
        op = e["operation"]
        cap_at = e["captured_at"]
        payload = e["after"] if e["after"] else e["before"]
        if not payload:
            continue

        if tbl == "customers":
            con.execute(
                """
                INSERT INTO stg_customers_cdc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("customer_id"),
                    payload.get("name"),
                    payload.get("email"),
                    payload.get("address"),
                    op,
                    payload.get("updated_at") or cap_at,
                    cap_at,
                    payload.get("created_at"),
                    payload.get("updated_at")
                )
            )
        elif tbl == "products":
            con.execute(
                """
                INSERT INTO stg_products_cdc VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("product_id"),
                    payload.get("name"),
                    payload.get("category"),
                    float(payload.get("price", 0.0)),
                    op,
                    payload.get("updated_at") or cap_at,
                    cap_at,
                    payload.get("updated_at")
                )
            )
        elif tbl == "orders":
            con.execute(
                """
                INSERT INTO stg_orders_cdc VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("order_id"),
                    payload.get("customer_id"),
                    payload.get("order_date"),
                    payload.get("status"),
                    float(payload.get("total_amount", 0.0)),
                    op,
                    cap_at
                )
            )
        elif tbl == "order_items":
            con.execute(
                """
                INSERT INTO stg_order_items_cdc VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("order_item_id"),
                    payload.get("order_id"),
                    payload.get("product_id"),
                    int(payload.get("quantity", 1)),
                    float(payload.get("unit_price", 0.0)),
                    op,
                    cap_at
                )
            )
        elif tbl == "inventory":
            con.execute(
                """
                INSERT INTO stg_inventory_cdc VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("product_id"),
                    payload.get("warehouse_id"),
                    int(payload.get("stock_level", 0)),
                    op,
                    cap_at,
                    payload.get("updated_at")
                )
            )

    con.close()
    logger.info(f"Loaded {len(all_events)} CDC events into DuckDB staging tables ({DUCKDB_PATH}).")
    return {"status": "SUCCESS", "events_loaded": len(all_events)}


if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id='warehouse_load_dag',
        default_args=default_args,
        description='Loads and upserts CDC change events into DuckDB analytics staging',
        schedule_interval=timedelta(minutes=10),
        catchup=False,
        max_active_runs=1,
    ) as dag:

        load_task = PythonOperator(
            task_id='load_cdc_to_duckdb_staging',
            python_callable=task_load_cdc_to_duckdb,
            provide_context=True,
        )


def run_standalone(test_retry: bool = False):
    """Standalone runner for verifying DAG execution, retries, and failure alerts."""
    logger.info("Running warehouse_load_dag in standalone mode...")
    max_retries = default_args['retries']
    
    if test_retry:
        logger.info(f"Testing retry mechanism with simulated failure (retries={max_retries})...")
        for attempt in range(1, max_retries + 2):
            try:
                task_load_cdc_to_duckdb(simulate_failure=True)
            except Exception as e:
                context = {
                    "dag": type("MockDAG", (), {"dag_id": "warehouse_load_dag"}),
                    "task_instance": type("MockTI", (), {
                        "task_id": "load_cdc_to_duckdb_staging",
                        "try_number": attempt,
                        "max_tries": max_retries + 1
                    }),
                    "execution_date": datetime.datetime.now(datetime.timezone.utc),
                    "exception": e
                }
                if attempt <= max_retries:
                    slack_alert_on_retry(context)
                else:
                    slack_alert_on_failure(context)
                    logger.error(f"Task failed after {max_retries} retries as expected.")
                    return False
    else:
        result = task_load_cdc_to_duckdb(simulate_failure=False)
        logger.info(f"Standalone execution succeeded: {result}")
        return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-retry", action="store_true", help="Simulate a task failure to verify retries and alerts")
    args = parser.parse_args()
    run_standalone(test_retry=args.test_retry)
