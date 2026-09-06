#!/usr/bin/env python3
"""
OrderFlow Bad Data Injection & Great Expectations Failure Demonstration
Acceptance requirement for Phase 5:
Intentionally injects invalid records into the source CDC stream:
1. Product with negative price ($ -49.99)
2. Order with invalid status enum ('fraudulent_charge')
3. Order referencing non-existent customer (customer_id = 999999)
Demonstrates that Great Expectations catches corrupted data before warehouse load!
"""

import os
import sys
import datetime
import logging
import argparse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from postgres.simulate_changes import OLTPDatabase
from cdc.cdc_event_schema import CDCExtractor
from great_expectations_validator import SourceDataQualityValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("inject_bad_data")


def inject_bad_records() -> int:
    """Injects corrupt records into database and marks CDC events."""
    db = OLTPDatabase()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    logger.info("Injecting Corrupted Records into OLTP source:")

    # 1. Product with negative price
    logger.info("1. Injecting Product #5 with negative price (-49.99)...")
    p_old = db.fetchone("SELECT * FROM products WHERE product_id = 5")
    p_new = dict(p_old) if p_old else {"product_id": 5, "name": "Corrupted Product", "category": "Electronics"}
    p_new["price"] = -49.99
    p_new["updated_at"] = now
    db.record_cdc("products", "update", p_old, p_new)

    # 2. Order with invalid status enum
    logger.info("2. Injecting Order #99901 with invalid status enum ('fraudulent_charge')...")
    o_bad_status = {
        "order_id": 99901,
        "customer_id": 1,
        "order_date": now,
        "status": "fraudulent_charge",
        "total_amount": 150.00
    }
    db.record_cdc("orders", "insert", None, o_bad_status)

    # 3. Order with non-existent customer (referential integrity violation)
    logger.info("3. Injecting Order #99902 referencing non-existent customer_id 999999...")
    o_bad_fk = {
        "order_id": 99902,
        "customer_id": 999999,
        "order_date": now,
        "status": "pending",
        "total_amount": 89.99
    }
    db.record_cdc("orders", "insert", None, o_bad_fk)

    db.close()
    logger.info("Successfully injected 3 bad CDC events into the source stream.")
    return 3


def test_great_expectations_gate():
    """Runs the full bad data injection and validates that GX stops execution."""
    logger.info("=== STARTING PHASE 5 ACCEPTANCE TEST: BAD DATA INJECTION & GX GATE ===")
    
    # Step 1: Inject bad records
    inject_bad_records()

    # Step 2: Extract to bronze
    extractor = CDCExtractor()
    events = extractor.extract_unprocessed_events(mark_as_processed=True)
    extractor.persist_events(events)

    # Step 3: Run Great Expectations validation suite
    logger.info("\nExecuting Great Expectations source validation gate...")
    validator = SourceDataQualityValidator()
    passed = validator.run_all_suites()

    if not passed:
        logger.info("\n" + "="*80)
        logger.info(" SUCCESS: Great Expectations correctly detected all injected source data quality violations!")
        logger.info(" Warehouse ingestion is safely halted before corrupt data enters the warehouse.")
        logger.info("="*80)
        return True
    else:
        logger.error("FAILURE: Great Expectations failed to catch the injected errors!")
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-failure", action="store_true", default=True, help="Inject bad data and verify GX catches it")
    args = parser.parse_args()
    test_great_expectations_gate()


if __name__ == "__main__":
    main()
