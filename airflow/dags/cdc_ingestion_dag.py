"""
OrderFlow Airflow DAG: cdc_ingestion_dag
Extracts unprocessed CDC change events from source database and partitions them to bronze JSON logs.
Configured with retries (exponential backoff) and Slack/log failure alerts.
"""

import os
import sys
import datetime
from datetime import timedelta
import logging

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

# Ensure repository root is in sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from cdc.cdc_event_schema import CDCExtractor
from airflow.plugins.alerts import slack_alert_on_failure, slack_alert_on_retry

logger = logging.getLogger("airflow.cdc_ingestion_dag")

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


def task_extract_cdc_events(simulate_failure: bool = False, **kwargs):
    """Extracts raw CDC events from database and persists partitioned JSON files."""
    if simulate_failure:
        raise FileNotFoundError("[SIMULATED ERROR] Target CDC destination storage unreachable / bad file path.")

    extractor = CDCExtractor()
    events = extractor.extract_unprocessed_events(mark_as_processed=True)
    summary = extractor.persist_events(events)
    logger.info(f"Task complete. Ingested events: {summary}")
    return {"status": "SUCCESS", "event_count": len(events), "tables": summary}


if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id='cdc_ingestion_dag',
        default_args=default_args,
        description='Extracts CDC change events from operational database to raw bronze store',
        schedule_interval=timedelta(minutes=5),
        catchup=False,
        max_active_runs=1,
    ) as dag:

        extract_task = PythonOperator(
            task_id='extract_and_persist_cdc_events',
            python_callable=task_extract_cdc_events,
            provide_context=True,
        )


def run_standalone(test_retry: bool = False):
    """Standalone runner for verifying DAG execution, retries, and failure alerts."""
    logger.info("Running cdc_ingestion_dag in standalone mode...")
    max_retries = default_args['retries']
    
    if test_retry:
        logger.info(f"Testing retry mechanism with simulated failure (retries={max_retries})...")
        for attempt in range(1, max_retries + 2):
            try:
                task_extract_cdc_events(simulate_failure=True)
            except Exception as e:
                context = {
                    "dag": type("MockDAG", (), {"dag_id": "cdc_ingestion_dag"}),
                    "task_instance": type("MockTI", (), {
                        "task_id": "extract_and_persist_cdc_events",
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
                    logger.error(f"Task definitively failed after {max_retries} retries as expected.")
                    return False
    else:
        result = task_extract_cdc_events(simulate_failure=False)
        logger.info(f"Standalone execution succeeded: {result}")
        return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-retry", action="store_true", help="Simulate a task failure to verify retries and alerts")
    args = parser.parse_args()
    run_standalone(test_retry=args.test_retry)
