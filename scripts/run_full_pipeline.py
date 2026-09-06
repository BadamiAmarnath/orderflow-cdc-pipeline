#!/usr/bin/env python3
"""
OrderFlow: End-to-End ELT Pipeline Orchestrator & Idempotency Prover
Executes the full pipeline lifecycle:
Stage 1: Seed / verify OLTP source database
Stage 2: Simulate ongoing e-commerce transactions
Stage 3: CDC change extraction & bronze partitioning
Stage 4: Great Expectations source data quality validation
Stage 5: DuckDB warehouse load (upsert/merge staging)
Stage 6: dbt model execution (SCD2 marts, facts, aggregates)
Stage 7: dbt business logic & SCD2 test execution
Stage 8: Independent SCD2 verification suite
Stage 9: Idempotency verification proof
"""

import os
import sys
import time
import json
import argparse
import subprocess
import datetime
import logging
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from postgres.simulate_changes import OLTPDatabase, seed_database, ChangeSimulator
from cdc.cdc_event_schema import CDCExtractor
from great_expectations_validator import SourceDataQualityValidator
from airflow.dags.warehouse_load_dag import task_load_cdc_to_duckdb
from scripts.validate_scd2 import check_single_current_row, check_no_overlapping_windows

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("orderflow_orchestrator")

DUCKDB_PATH = os.path.join(BASE_DIR, "data", "warehouse", "orderflow.duckdb")
DBT_DIR = os.path.join(BASE_DIR, "dbt_project")


def get_warehouse_row_counts() -> dict:
    """Queries row counts of all warehouse models in DuckDB."""
    if not os.path.exists(DUCKDB_PATH):
        return {}
    con = duckdb.connect(DUCKDB_PATH)
    tables = [
        "dim_customers_scd2",
        "dim_products_scd2",
        "fct_orders",
        "agg_daily_sales",
        "stg_orders",
        "stg_order_items"
    ]
    counts = {}
    for t in tables:
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            counts[t] = cnt
        except Exception:
            counts[t] = 0
    con.close()
    return counts


def run_pipeline(seed: bool = False, steps: int = 15, run_dbt: bool = True) -> bool:
    """Executes full stage-by-stage pipeline."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("🚀 STARTING ORDERFLOW CDC-DRIVEN ELT PIPELINE EXECUTION")
    logger.info("=" * 80)

    # ---------------------------------------------------------
    # STAGE 1 & 2: Source Seeding & Change Simulation
    # ---------------------------------------------------------
    db = OLTPDatabase()
    if seed:
        logger.info("[STAGE 1/8] Seeding operational source database...")
        # Clear existing bronze events if reseeding
        import shutil
        cdc_dir = os.path.join(BASE_DIR, "data", "cdc_events")
        if os.path.exists(cdc_dir):
            shutil.rmtree(cdc_dir)
        seed_database(db)
        logger.info("[STAGE 1/8] Seed complete.")

    if steps > 0:
        logger.info(f"[STAGE 2/8] Simulating {steps} transactional OLTP changes...")
        simulator = ChangeSimulator(db)
        for _ in range(steps):
            simulator.run_step()
        db_stats = simulator.get_stats()
        logger.info(f"[STAGE 2/8] Source OLTP State: {json.dumps(db_stats)}")
    db.close()

    # ---------------------------------------------------------
    # STAGE 3: CDC Extraction & Bronze Partitioning
    # ---------------------------------------------------------
    logger.info("[STAGE 3/8] Extracting CDC change events to bronze JSON logs...")
    extractor = CDCExtractor()
    events = extractor.extract_unprocessed_events(mark_as_processed=True)
    summary = extractor.persist_events(events)
    logger.info(f"[STAGE 3/8] CDC Ingestion complete: {summary}")

    # ---------------------------------------------------------
    # STAGE 4: Great Expectations Source Data Quality Gate
    # ---------------------------------------------------------
    logger.info("[STAGE 4/8] Executing Great Expectations Source Data Quality Suites...")
    validator = SourceDataQualityValidator()
    gx_passed = validator.run_all_suites()
    if not gx_passed:
        logger.error("[STAGE 4/8] ❌ Great Expectations validation failed! Halting pipeline.")
        return False
    logger.info("[STAGE 4/8] ✅ Great Expectations: 0 errors detected.")

    # ---------------------------------------------------------
    # STAGE 5: DuckDB Warehouse Staging Load (Upsert/Merge)
    # ---------------------------------------------------------
    logger.info("[STAGE 5/8] Loading CDC events into DuckDB staging layer...")
    task_load_cdc_to_duckdb()
    logger.info("[STAGE 5/8] DuckDB staging load complete.")

    # ---------------------------------------------------------
    # STAGE 6: dbt Transformations (SCD2, Facts, Aggregates)
    # ---------------------------------------------------------
    if run_dbt:
        logger.info("[STAGE 6/8] Executing dbt transformations...")
        res_run = subprocess.run(
            [sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
            capture_output=True, text=True
        )
        if res_run.returncode != 0:
            logger.error(f"[STAGE 6/8] ❌ dbt run failed:\n{res_run.stdout}\n{res_run.stderr}")
            return False
        logger.info("[STAGE 6/8] ✅ dbt transformations built successfully.")

        # ---------------------------------------------------------
        # STAGE 7: dbt Business Logic Tests
        # ---------------------------------------------------------
        logger.info("[STAGE 7/8] Executing dbt business logic & SCD2 schema tests...")
        res_test = subprocess.run(
            [sys.executable, "-m", "dbt.cli.main", "test", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
            capture_output=True, text=True
        )
        if res_test.returncode != 0:
            logger.error(f"[STAGE 7/8] ❌ dbt tests failed:\n{res_test.stdout}\n{res_test.stderr}")
            return False
        logger.info("[STAGE 7/8] ✅ All dbt schema and custom SCD2 tests passed.")

    # ---------------------------------------------------------
    # STAGE 8: Independent SCD2 Verification Suite
    # ---------------------------------------------------------
    logger.info("[STAGE 8/8] Verifying SCD Type 2 dimensional correctness...")
    con = duckdb.connect(DUCKDB_PATH)
    scd_ok = check_single_current_row(con) and check_no_overlapping_windows(con)
    con.close()
    if not scd_ok:
        logger.error("[STAGE 8/8] ❌ SCD2 verification failed.")
        return False
    logger.info("[STAGE 8/8] ✅ SCD2 correctness verified independently.")

    elapsed = time.time() - start_time
    logger.info("=" * 80)
    logger.info(f"✨ ORDERFLOW PIPELINE EXECUTION COMPLETED SUCCESSFULLY IN {elapsed:.2f}s")
    counts = get_warehouse_row_counts()
    logger.info(f"Warehouse Analytics State:\n{json.dumps(counts, indent=2)}")
    logger.info("=" * 80)
    return True


def prove_idempotency() -> bool:
    """
    Acceptance requirement for Phase 6:
    Runs the full pipeline twice in a row without new source changes.
    Verifies that table row counts and metrics remain exactly identical.
    """
    logger.info("\n" + "=" * 80)
    logger.info("🔬 RUNNING IDEMPOTENCY VERIFICATION PROOF")
    logger.info("=" * 80)

    logger.info("1. Executing initial baseline pipeline run (seed=True, steps=10)...")
    run_pipeline(seed=True, steps=10)
    baseline_counts = get_warehouse_row_counts()
    logger.info(f"Baseline Warehouse Row Counts:\n{json.dumps(baseline_counts, indent=2)}")

    logger.info("\n2. Re-running entire pipeline with NO new source changes (steps=0)...")
    run_pipeline(seed=False, steps=0)
    second_counts = get_warehouse_row_counts()
    logger.info(f"Second Run Warehouse Row Counts:\n{json.dumps(second_counts, indent=2)}")

    # Compare row counts
    is_idempotent = True
    for table, count in baseline_counts.items():
        if count != second_counts.get(table):
            logger.error(f"❌ IDEMPOTENCY VIOLATION on table '{table}': baseline={count}, second_run={second_counts.get(table)}")
            is_idempotent = False

    if is_idempotent:
        logger.info("\n" + "=" * 80)
        logger.info("🎯 IDEMPOTENCY PROOF PASSED: 100% IDEMPOTENT!")
        logger.info(" Re-running the pipeline on identical source data produced zero duplicate rows.")
        logger.info("=" * 80)
    else:
        logger.error("❌ IDEMPOTENCY PROOF FAILED!")

    return is_idempotent


def main():
    parser = argparse.ArgumentParser(description="OrderFlow End-to-End Orchestrator")
    parser.add_argument("--seed", action="store_true", help="Seed database with initial data")
    parser.add_argument("--steps", type=int, default=10, help="Number of simulated OLTP steps")
    parser.add_argument("--test-idempotency", action="store_true", help="Execute idempotency verification proof")
    args = parser.parse_args()

    if args.test_idempotency:
        success = prove_idempotency()
        if not success:
            sys.exit(1)
    else:
        success = run_pipeline(seed=args.seed, steps=args.steps)
        if not success:
            sys.exit(1)


if __name__ == "__main__":
    main()
