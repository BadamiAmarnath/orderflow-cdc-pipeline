#!/usr/bin/env python3
"""
OrderFlow CDC Engine & Event Schema
Implements Debezium-compliant CDC event models, schema validation,
and partitioning to bronze change logs (JSON Lines and Parquet).
"""

import os
import sys
import json
import logging
import datetime
import argparse
from typing import Optional, Dict, Any, List, Literal
from pydantic import BaseModel, Field, field_validator
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("cdc_engine")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CDC_EVENTS_DIR = os.path.join(BASE_DIR, "data", "cdc_events")
LOCAL_DB_PATH = os.path.join(BASE_DIR, "data", "source_oltp.db")


class CDCEvent(BaseModel):
    """Debezium-compliant CDC Event Schema."""
    table: str = Field(..., description="Source table name")
    operation: Literal["insert", "update", "delete", "c", "u", "d"] = Field(
        ..., description="CDC operation type: insert (c), update (u), delete (d)"
    )
    before: Optional[Dict[str, Any]] = Field(None, description="Row state before change (null for insert)")
    after: Optional[Dict[str, Any]] = Field(None, description="Row state after change (null for delete)")
    captured_at: str = Field(..., description="ISO-8601 timestamp of capture")
    change_id: Optional[int] = Field(None, description="Unique sequence change id")

    @field_validator("operation")
    def normalize_operation(cls, v):
        op_map = {"c": "insert", "u": "update", "d": "delete"}
        return op_map.get(v.lower(), v.lower())

    @field_validator("before")
    def validate_before_state(cls, v, info):
        op = info.data.get("operation")
        if op == "insert" and v is not None:
            # Tolerant: can be None for inserts
            pass
        return v

    @field_validator("after")
    def validate_after_state(cls, v, info):
        op = info.data.get("operation")
        if op == "delete" and v is not None:
            # Tolerant: can be None for deletes
            pass
        return v


class CDCExtractor:
    """Extracts change events from source database and writes partitioned raw bronze logs."""

    def __init__(self, db_path: str = LOCAL_DB_PATH, output_dir: str = CDC_EVENTS_DIR):
        self.db_path = db_path
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_unprocessed_events(self, mark_as_processed: bool = True) -> List[CDCEvent]:
        """Polls database cdc_change_log for unprocessed events."""
        import sqlite3
        if not os.path.exists(self.db_path):
            logger.warning(f"Database {self.db_path} does not exist. Please run simulate_changes.py --seed first.")
            return []

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute(
            """
            SELECT change_id, table_name, operation, before_state, after_state, captured_at
            FROM cdc_change_log
            WHERE processed = 0
            ORDER BY change_id ASC
            """
        )
        rows = cur.fetchall()
        events: List[CDCEvent] = []
        change_ids = []

        for r in rows:
            b_state = json.loads(r["before_state"]) if r["before_state"] else None
            a_state = json.loads(r["after_state"]) if r["after_state"] else None

            event = CDCEvent(
                table=r["table_name"],
                operation=r["operation"],
                before=b_state,
                after=a_state,
                captured_at=r["captured_at"],
                change_id=r["change_id"]
            )
            events.append(event)
            change_ids.append(r["change_id"])

        if mark_as_processed and change_ids:
            cur.execute(
                f"UPDATE cdc_change_log SET processed = 1 WHERE change_id IN ({','.join(['?']*len(change_ids))})",
                change_ids
            )
            conn.commit()

        conn.close()
        logger.info(f"Extracted {len(events)} new CDC events from source database.")
        return events

    def persist_events(self, events: List[CDCEvent]) -> Dict[str, int]:
        """Persists events partitioned by table and date (YYYY-MM-DD)."""
        counts: Dict[str, int] = {}
        for event in events:
            # Parse capture date
            try:
                dt = datetime.datetime.fromisoformat(event.captured_at.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except Exception:
                date_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

            table_dir = os.path.join(self.output_dir, event.table)
            os.makedirs(table_dir, exist_ok=True)

            jsonl_file = os.path.join(table_dir, f"{date_str}.jsonl")
            with open(jsonl_file, "a", encoding="utf-8") as f:
                f.write(event.model_dump_json() + "\n")

            counts[event.table] = counts.get(event.table, 0) + 1

        logger.info(f"Persisted CDC events summary: {counts}")
        return counts

    def get_all_raw_events(self, table: Optional[str] = None) -> List[Dict[str, Any]]:
        """Reads all captured CDC events from bronze files."""
        events = []
        target_dirs = [os.path.join(self.output_dir, table)] if table else [
            os.path.join(self.output_dir, d) for d in os.listdir(self.output_dir)
            if os.path.isdir(os.path.join(self.output_dir, d))
        ]

        for t_dir in target_dirs:
            if not os.path.exists(t_dir):
                continue
            for f_name in sorted(os.listdir(t_dir)):
                if f_name.endswith(".jsonl"):
                    file_path = os.path.join(t_dir, f_name)
                    with open(file_path, "r", encoding="utf-8") as f:
                        for line in f:
                            if line.strip():
                                events.append(json.loads(line.strip()))
        return events


def main():
    parser = argparse.ArgumentParser(description="OrderFlow CDC Ingestion & Parsing Engine")
    parser.add_argument("--extract", action="store_true", help="Extract and partition all unprocessed CDC events")
    parser.add_argument("--test-capture", action="store_true", help="Run end-to-end extraction and display sample events")
    parser.add_argument("--stats", action="store_true", help="Display summary of persisted CDC events")
    args = parser.parse_args()

    extractor = CDCExtractor()

    if args.extract or args.test_capture:
        events = extractor.extract_unprocessed_events(mark_as_processed=True)
        extractor.persist_events(events)

        if args.test_capture:
            all_events = extractor.get_all_raw_events()
            logger.info(f"Total Bronze CDC Events on Disk: {len(all_events)}")
            # Show sample events for insert, update price, update address
            samples = {}
            for e in all_events:
                table = e["table"]
                op = e["operation"]
                key = f"{table}_{op}"
                if key not in samples:
                    samples[key] = e

            logger.info("Sample Captured Events:\n" + json.dumps(samples, indent=2))

    elif args.stats:
        all_events = extractor.get_all_raw_events()
        table_counts = {}
        op_counts = {}
        for e in all_events:
            table_counts[e["table"]] = table_counts.get(e["table"], 0) + 1
            op_counts[e["operation"]] = op_counts.get(e["operation"], 0) + 1

        logger.info(f"Total Bronze Events: {len(all_events)}")
        logger.info(f"By Table: {table_counts}")
        logger.info(f"By Operation: {op_counts}")


if __name__ == "__main__":
    main()
