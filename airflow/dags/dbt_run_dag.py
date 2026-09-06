"""
OrderFlow Airflow DAG: dbt_run_dag
Executes dbt transformations (SCD2 models, facts, business metrics) and runs data quality tests.
Configured with retries and failure notification alerting.
"""

import os
import sys
import subprocess
import datetime
from datetime import timedelta
import logging

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from airflow.plugins.alerts import slack_alert_on_failure, slack_alert_on_retry

logger = logging.getLogger("airflow.dbt_run_dag")

DBT_DIR = os.path.join(BASE_DIR, "dbt_project")

default_args = {
    'owner': 'orderflow_data_team',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 8, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    'on_failure_callback': slack_alert_on_failure,
    'on_retry_callback': slack_alert_on_retry,
}


def task_run_dbt_models(simulate_failure: bool = False, **kwargs):
    """Executes dbt run."""
    if simulate_failure:
        raise RuntimeError("[SIMULATED ERROR] dbt model execution failed due to intentional syntax check.")

    cmd = [sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]
    logger.info(f"Running command: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        logger.error(f"dbt run failed:\n{res.stdout}\n{res.stderr}")
        raise RuntimeError(f"dbt run exited with code {res.returncode}")

    logger.info(f"dbt run succeeded:\n{res.stdout}")
    return {"status": "SUCCESS"}


def task_run_dbt_tests(simulate_failure: bool = False, **kwargs):
    """Executes dbt test."""
    if simulate_failure:
        raise RuntimeError("[SIMULATED ERROR] dbt test failed.")

    cmd = [sys.executable, "-m", "dbt.cli.main", "test", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]
    logger.info(f"Running command: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        logger.error(f"dbt test failed:\n{res.stdout}\n{res.stderr}")
        raise RuntimeError(f"dbt test exited with code {res.returncode}")

    logger.info(f"dbt test succeeded:\n{res.stdout}")
    return {"status": "SUCCESS"}


if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id='dbt_run_dag',
        default_args=default_args,
        description='Triggers dbt transformation pipeline and SCD2 model integrity tests',
        schedule_interval=timedelta(minutes=15),
        catchup=False,
        max_active_runs=1,
    ) as dag:

        dbt_run = PythonOperator(
            task_id='dbt_run_models',
            python_callable=task_run_dbt_models,
            provide_context=True,
        )

        dbt_test = PythonOperator(
            task_id='dbt_test_models',
            python_callable=task_run_dbt_tests,
            provide_context=True,
        )

        dbt_run >> dbt_test


def run_standalone(test_retry: bool = False):
    """Standalone runner for verifying DAG execution, retries, and failure alerts."""
    logger.info("Running dbt_run_dag in standalone mode...")
    max_retries = default_args['retries']
    
    if test_retry:
        logger.info(f"Testing retry mechanism with simulated failure (retries={max_retries})...")
        for attempt in range(1, max_retries + 2):
            try:
                task_run_dbt_models(simulate_failure=True)
            except Exception as e:
                context = {
                    "dag": type("MockDAG", (), {"dag_id": "dbt_run_dag"}),
                    "task_instance": type("MockTI", (), {
                        "task_id": "dbt_run_models",
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
        task_run_dbt_models(simulate_failure=False)
        task_run_dbt_tests(simulate_failure=False)
        logger.info("Standalone execution succeeded.")
        return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-retry", action="store_true", help="Simulate a task failure to verify retries and alerts")
    args = parser.parse_args()
    run_standalone(test_retry=args.test_retry)
