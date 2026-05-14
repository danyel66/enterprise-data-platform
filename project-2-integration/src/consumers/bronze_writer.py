"""
Bronze Layer Writer
-------------------
Consumes normalized integration events (from Event Grid callbacks or
Service Bus queue) and writes them to the Bronze layer as Parquet files.

In Databricks, these Parquet files would be picked up by Auto Loader
and streamed into Delta Lake — same pattern as Project 1.
Locally, they write to ./data/bronze/ for testing.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger        = logging.getLogger(__name__)
BRONZE_PATH   = Path(os.getenv("BRONZE_OUTPUT_PATH", "./data/bronze"))


def write_to_bronze(records: list[dict], source: str) -> int:
    """
    Write a batch of normalized records to the Bronze layer.

    Args:
        records: list of normalized event dicts
        source:  e.g. "freshdesk" or "procore" — used for partitioning

    Returns:
        number of records written
    """
    if not records:
        logger.info("No records to write")
        return 0

    output_dir = BRONZE_PATH / source
    output_dir.mkdir(parents=True, exist_ok=True)

    df        = pd.DataFrame(records)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename  = output_dir / f"batch_{timestamp}.parquet"

    df.to_parquet(filename, index=False)
    logger.info(f"Wrote {len(records)} records to {filename}")
    return len(records)


def process_event_grid_push(event_payload: dict) -> int:
    """
    Handle a single event pushed by Event Grid (via webhook delivery).
    Event Grid delivers events as a list even for single events.
    """
    events  = event_payload if isinstance(event_payload, list) else [event_payload]
    records = [e["data"] for e in events if "data" in e]

    if not records:
        return 0

    source = records[0].get("source", "unknown")
    return write_to_bronze(records, source)


def drain_service_bus_to_bronze(max_messages: int = 100) -> int:
    """
    Pull messages from Service Bus queue and write to Bronze layer.
    Called on a schedule (e.g. every 15 minutes via Databricks Workflow).
    """
    from src.routing.service_bus_handler import receive_from_queue

    messages = receive_from_queue(max_messages)
    if not messages:
        return 0

    # Group by source for clean partitioning
    by_source: dict[str, list] = {}
    for msg in messages:
        src = msg.get("source", "unknown")
        by_source.setdefault(src, []).append(msg)

    total = 0
    for source, records in by_source.items():
        total += write_to_bronze(records, source)

    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test with mock Freshdesk records
    mock_records = [
        {
            "source":          "freshdesk",
            "event_type":      "ticket_created",
            "ingested_at":     datetime.now(timezone.utc).isoformat(),
            "ticket_id":       f"TKT-{i:04d}",
            "subject":         f"Test ticket {i}",
            "status":          "open",
            "priority":        "medium",
            "requester_email": f"user{i}@example.com",
            "agent_email":     "agent@example.com",
            "group_name":      "Support",
            "created_at":      datetime.now(timezone.utc).isoformat(),
            "updated_at":      datetime.now(timezone.utc).isoformat(),
            "raw_payload":     json.dumps({"mock": True}),
        }
        for i in range(1, 11)
    ]

    written = write_to_bronze(mock_records, "freshdesk")
    print(f"Bronze writer test: {written} records written to {BRONZE_PATH}/freshdesk/")
