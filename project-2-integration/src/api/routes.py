"""
API Routes
----------
Exposes Gold layer data from Project 1 for downstream consumers.
In local dev, reads from ./data/bronze Parquet files.
In production, queries Delta Lake Gold tables on Databricks.
"""

import logging
import os
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException

logger      = logging.getLogger(__name__)
router      = APIRouter(prefix="/api/v1", tags=["Data API"])
BRONZE_PATH = Path(os.getenv("BRONZE_OUTPUT_PATH", "./data/bronze"))


@router.get("/events/{source}")
def get_events(source: str, limit: int = 50):
    """
    Return the latest Bronze-layer records for a given source.
    source: 'freshdesk' or 'procore'
    """
    source_path = BRONZE_PATH / source
    if not source_path.exists():
        raise HTTPException(status_code=404, detail=f"No data found for source: {source}")

    parquet_files = sorted(source_path.glob("*.parquet"), reverse=True)
    if not parquet_files:
        return {"source": source, "records": [], "count": 0}

    # Read the most recent file
    df = pd.read_parquet(parquet_files[0])
    records = df.head(limit).to_dict(orient="records")

    return {"source": source, "records": records, "count": len(records)}


@router.get("/events/{source}/summary")
def get_summary(source: str):
    """Return record counts and latest ingestion timestamp per source."""
    source_path = BRONZE_PATH / source
    if not source_path.exists():
        raise HTTPException(status_code=404, detail=f"No data for source: {source}")

    parquet_files = list(source_path.glob("*.parquet"))
    total_records = 0
    latest_file   = None

    for f in parquet_files:
        df = pd.read_parquet(f)
        total_records += len(df)
        if latest_file is None or f.stat().st_mtime > latest_file.stat().st_mtime:
            latest_file = f

    return {
        "source":        source,
        "total_records": total_records,
        "batch_files":   len(parquet_files),
        "latest_batch":  latest_file.name if latest_file else None,
    }
