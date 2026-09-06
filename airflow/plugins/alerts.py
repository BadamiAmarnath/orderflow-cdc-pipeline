"""
OrderFlow Airflow Plugin: Alerts & Notification Handlers
Provides alert callbacks for Slack webhook and structured error telemetry with retry handlers.
"""

import os
import json
import logging
import datetime

logger = logging.getLogger("airflow.alerts")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def slack_alert_on_failure(context):
    """
    Airflow on_failure_callback that constructs a structured alert payload
    and logs/sends notifications to Slack.
    """
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    execution_date = context.get("execution_date", datetime.datetime.now(datetime.timezone.utc)).isoformat()
    try_number = context.get("task_instance").try_number if context.get("task_instance") else 1
    max_tries = context.get("task_instance").max_tries if context.get("task_instance") else 1
    exception = context.get("exception", "No exception details provided")

    alert_payload = {
        "alert_type": "AIRFLOW_TASK_FAILURE",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": str(execution_date),
        "try_number": try_number,
        "max_tries": max_tries,
        "error_message": str(exception)
    }

    alert_banner = f"""
================================================================================
[ALERT] AIRFLOW TASK FAILURE DETECTED!
--------------------------------------------------------------------------------
* DAG ID:         {dag_id}
* Task ID:        {task_id}
* Execution Date: {execution_date}
* Attempt:        {try_number} / {max_tries}
* Error Message:  {exception}
================================================================================
"""
    logger.error(alert_banner)
    try:
        print(alert_banner, flush=True)
    except UnicodeEncodeError:
        print(alert_banner.encode("ascii", "replace").decode("ascii"), flush=True)

    if SLACK_WEBHOOK_URL:
        try:
            import requests
            requests.post(SLACK_WEBHOOK_URL, json={"text": alert_banner}, timeout=5)
            logger.info("Sent alert notification to Slack webhook.")
        except Exception as err:
            logger.warning(f"Failed sending webhook payload: {err}")

    return alert_payload


def slack_alert_on_retry(context):
    """Callback fired on task retry attempt."""
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    try_number = context.get("task_instance").try_number if context.get("task_instance") else 1
    max_tries = context.get("task_instance").max_tries if context.get("task_instance") else 1
    exception = context.get("exception", "Transient error")

    retry_banner = f"[RETRY WARNING] Task '{task_id}' in DAG '{dag_id}' failed (attempt {try_number}/{max_tries}). Retrying with exponential backoff... Error: {exception}"
    logger.warning(retry_banner)
    try:
        print(retry_banner, flush=True)
    except UnicodeEncodeError:
        print(retry_banner.encode("ascii", "replace").decode("ascii"), flush=True)
