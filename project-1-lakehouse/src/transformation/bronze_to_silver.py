"""
bronze_to_silver.py
-------------------
Promotes validated Bronze records to the Silver layer.

Silver layer principles:
  - Cleaned and conformed — no nulls in critical fields
  - Deduplicated on natural key
  - Standardized data types and column naming
  - Enriched with derived fields (no business aggregations yet)
  - Every record traceable back to its Bronze batch_id

Business context:
    Silver is the trusted, analyst-ready layer. Analysts and ML
    engineers query Silver directly when they need record-level
    detail. Gold is for executive KPIs and dashboards.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_FACILITIES = [
    "DAL-01", "DAL-02", "ATL-01", "CHI-01", "SEA-01",
    "NYC-01", "LAX-01", "MIA-01", "DEN-01", "PHX-01",
    "BOS-01", "HOU-01",
]

FACILITY_METRO_MAP = {
    "DAL-01": "Dallas",    "DAL-02": "Dallas",
    "ATL-01": "Atlanta",   "CHI-01": "Chicago",
    "SEA-01": "Seattle",   "NYC-01": "New York",
    "LAX-01": "Los Angeles", "MIA-01": "Miami",
    "DEN-01": "Denver",    "PHX-01": "Phoenix",
    "BOS-01": "Boston",    "HOU-01": "Houston",
}

FACILITY_REGION_MAP = {
    "DAL-01": "South",  "DAL-02": "South",
    "ATL-01": "South",  "HOU-01": "South",
    "CHI-01": "Midwest",
    "NYC-01": "Northeast", "BOS-01": "Northeast",
    "MIA-01": "Southeast",
    "SEA-01": "West",   "LAX-01": "West",
    "DEN-01": "West",   "PHX-01": "West",
}


# ---------------------------------------------------------------------------
# Energy Silver Transformer
# ---------------------------------------------------------------------------

class EnergyBronzeToSilver:
    """
    Transforms Bronze energy demand records into the Silver layer.

    Transformations applied:
      1. Cast types correctly
      2. Parse and enrich timestamps
      3. Standardize column names (snake_case, no eia_ prefix)
      4. Map facility metadata (metro, region)
      5. Derive energy efficiency flags
      6. Deduplicate on natural key
      7. Drop records that fail critical checks
      8. Add Silver audit columns
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.promotion_timestamp = datetime.utcnow().isoformat()
        self._initial_count = len(df)
        logger.info(f"EnergyBronzeToSilver initialized | input_records={self._initial_count}")

    # -----------------------------------------------------------------------
    # Step 1 — Type Casting
    # -----------------------------------------------------------------------

    def cast_types(self) -> "EnergyBronzeToSilver":
        """Cast all columns to correct types."""
        logger.info("Step 1: Casting types")

        self.df["eia_value"] = pd.to_numeric(self.df["eia_value"], errors="coerce")
        self.df["eia_period"] = pd.to_datetime(self.df["eia_period"], errors="coerce")
        self.df["_ingestion_timestamp"] = pd.to_datetime(
            self.df["_ingestion_timestamp"], errors="coerce"
        )

        null_after_cast = self.df["eia_value"].isnull().sum()
        if null_after_cast > 0:
            logger.warning(f"  {null_after_cast} records have null eia_value after cast")

        return self

    # -----------------------------------------------------------------------
    # Step 2 — Rename and Standardize Columns
    # -----------------------------------------------------------------------

    def rename_columns(self) -> "EnergyBronzeToSilver":
        """Rename EIA-prefixed columns to clean Silver names."""
        logger.info("Step 2: Renaming columns to Silver schema")

        rename_map = {
            "eia_period":           "measurement_timestamp",
            "eia_respondent":       "eia_region_code",
            "eia_respondent_name":  "eia_region_name",
            "eia_type":             "measurement_type_code",
            "eia_type_name":        "measurement_type_name",
            "eia_value":            "energy_demand_mwh",
            "eia_value_units":      "energy_units",
            "eia_region":           "source_region",
            "_ingestion_timestamp": "bronze_ingestion_timestamp",
            "_source_system":       "source_system",
            "_batch_id":            "bronze_batch_id",
        }

        # Only rename columns that exist
        existing_renames = {k: v for k, v in rename_map.items() if k in self.df.columns}
        self.df = self.df.rename(columns=existing_renames)

        return self

    # -----------------------------------------------------------------------
    # Step 3 — Timestamp Enrichment
    # -----------------------------------------------------------------------

    def enrich_timestamps(self) -> "EnergyBronzeToSilver":
        """Derive date parts from measurement_timestamp for partitioning."""
        logger.info("Step 3: Enriching timestamps")

        ts = self.df["measurement_timestamp"]
        self.df["measurement_date"]   = ts.dt.date.astype(str)
        self.df["measurement_year"]   = ts.dt.year
        self.df["measurement_month"]  = ts.dt.month
        self.df["measurement_day"]    = ts.dt.day
        self.df["measurement_hour"]   = ts.dt.hour
        self.df["day_of_week"]        = ts.dt.day_name()
        self.df["is_weekend"]         = ts.dt.dayofweek >= 5
        self.df["is_peak_hour"]       = ts.dt.hour.between(9, 18)

        return self

    # -----------------------------------------------------------------------
    # Step 4 — Facility Enrichment
    # -----------------------------------------------------------------------

    def enrich_facility(self) -> "EnergyBronzeToSilver":
        """Add facility metro and region from reference maps."""
        logger.info("Step 4: Enriching facility metadata")

        self.df["facility_metro"]  = self.df["facility_id"].map(FACILITY_METRO_MAP)
        self.df["facility_region"] = self.df["facility_id"].map(FACILITY_REGION_MAP)

        unknown_facilities = self.df[self.df["facility_metro"].isnull()]["facility_id"].unique()
        if len(unknown_facilities) > 0:
            logger.warning(f"  Unknown facility IDs: {unknown_facilities}")

        return self

    # -----------------------------------------------------------------------
    # Step 5 — Derived Business Fields
    # -----------------------------------------------------------------------

    def add_derived_fields(self) -> "EnergyBronzeToSilver":
        """
        Add derived fields that support downstream analytics.
        No aggregations here — record-level enrichment only.
        """
        logger.info("Step 5: Adding derived fields")

        # Demand tier classification
        conditions = [
            self.df["energy_demand_mwh"] < 5000,
            self.df["energy_demand_mwh"].between(5000, 15000),
            self.df["energy_demand_mwh"] > 15000,
        ]
        choices = ["low", "medium", "high"]
        self.df["demand_tier"] = np.select(conditions, choices, default="unknown")

        # Demand vs 24h rolling average (approximate with group mean)
        self.df["demand_vs_daily_avg"] = self.df.groupby(
            ["facility_id", "measurement_date"]
        )["energy_demand_mwh"].transform(
            lambda x: (x - x.mean()) / x.mean() * 100
        ).round(2)

        return self

    # -----------------------------------------------------------------------
    # Step 6 — Deduplication
    # -----------------------------------------------------------------------

    def deduplicate(self) -> "EnergyBronzeToSilver":
        """
        Remove duplicate records on the natural key.
        Keep the most recently ingested version of each record.
        """
        logger.info("Step 6: Deduplicating on natural key")

        before = len(self.df)
        self.df = self.df.sort_values(
            "bronze_ingestion_timestamp", ascending=False
        ).drop_duplicates(
            subset=["measurement_timestamp", "facility_id", "source_region"],
            keep="first",
        )
        after = len(self.df)
        logger.info(f"  Removed {before - after} duplicate records")

        return self

    # -----------------------------------------------------------------------
    # Step 7 — Drop Invalid Records
    # -----------------------------------------------------------------------

    def drop_invalid(self) -> "EnergyBronzeToSilver":
        """
        Drop records that cannot be promoted to Silver.
        Log counts for lineage tracking.
        """
        logger.info("Step 7: Dropping invalid records")

        before = len(self.df)

        # Drop records with null critical fields
        self.df = self.df.dropna(
            subset=["measurement_timestamp", "facility_id", "energy_demand_mwh"]
        )

        # Drop records with negative energy values
        self.df = self.df[self.df["energy_demand_mwh"] >= 0]

        # Drop records with unknown facilities
        self.df = self.df[self.df["facility_id"].isin(VALID_FACILITIES)]

        after = len(self.df)
        logger.info(f"  Dropped {before - after} invalid records")

        return self

    # -----------------------------------------------------------------------
    # Step 8 — Add Silver Audit Columns
    # -----------------------------------------------------------------------

    def add_silver_metadata(self) -> "EnergyBronzeToSilver":
        """Stamp Silver records with promotion metadata."""
        logger.info("Step 8: Adding Silver audit columns")

        self.df["_silver_promotion_timestamp"] = self.promotion_timestamp
        self.df["_silver_layer"]               = "silver"
        self.df["_silver_version"]             = "1.0"
        self.df["_records_in"]                 = self._initial_count
        self.df["_records_out"]                = len(self.df)

        return self

    # -----------------------------------------------------------------------
    # Run Full Pipeline
    # -----------------------------------------------------------------------

    def transform(self) -> pd.DataFrame:
        """
        Execute the full Bronze → Silver transformation chain.
        Returns the Silver-ready DataFrame.
        """
        logger.info("Starting Bronze → Silver transformation")

        result = (
            self
            .cast_types()
            .rename_columns()
            .enrich_timestamps()
            .enrich_facility()
            .add_derived_fields()
            .deduplicate()
            .drop_invalid()
            .add_silver_metadata()
            .df
        )

        logger.success(
            f"Bronze → Silver complete | "
            f"in={self._initial_count} | out={len(result)} | "
            f"retention={round(len(result)/self._initial_count*100, 1)}%"
        )

        return result


# ---------------------------------------------------------------------------
# Server Capacity Silver Transformer
# ---------------------------------------------------------------------------

class CapacityBronzeToSilver:
    """
    Transforms Bronze server capacity CSV records into Silver layer.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.promotion_timestamp = datetime.utcnow().isoformat()
        self._initial_count = len(df)

    def transform(self) -> pd.DataFrame:
        logger.info(f"Capacity Bronze → Silver | input={self._initial_count}")

        df = self.df.copy()

        # Type casting
        df["report_timestamp"]       = pd.to_datetime(df["report_timestamp"], errors="coerce")
        df["server_utilization_pct"] = pd.to_numeric(df["server_utilization_pct"], errors="coerce")
        df["cooling_efficiency_pue"] = pd.to_numeric(df["cooling_efficiency_pue"], errors="coerce")
        df["network_throughput_gbps"]= pd.to_numeric(df["network_throughput_gbps"], errors="coerce")

        # Derived fields
        df["availability_pct"] = (
            (df["active_servers"] /
             (df["active_servers"] + df["failed_servers"] + df["maintenance_servers"]))
            * 100
        ).round(2)

        df["is_high_utilization"] = df["server_utilization_pct"] > 80
        df["is_cooling_alert"]    = df["cooling_temp_celsius"] > 25
        df["is_pue_efficient"]    = df["cooling_efficiency_pue"] < 1.5

        # Facility enrichment
        df["facility_metro"]  = df["facility_id"].map(FACILITY_METRO_MAP)
        df["facility_region"] = df["facility_id"].map(FACILITY_REGION_MAP)

        # Silver metadata
        df["_silver_promotion_timestamp"] = self.promotion_timestamp
        df["_silver_layer"]               = "silver"

        # Deduplication
        df = df.drop_duplicates(
            subset=["facility_id", "report_timestamp", "rack_type"],
            keep="first"
        )

        # Drop nulls on critical fields
        df = df.dropna(subset=["facility_id", "report_timestamp", "server_utilization_pct"])

        logger.success(f"Capacity Silver complete | out={len(df)}")
        return df


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from src.ingestion.api_source import BronzeEnergyIngestion
    from src.ingestion.csv_source import BronzeCSVIngestion

    print("\n=== Running Bronze → Silver: Energy ===")
    energy_bronze = BronzeEnergyIngestion().ingest_all_regions(days_back=3)
    energy_silver = EnergyBronzeToSilver(energy_bronze).transform()

    print("\n=== Running Bronze → Silver: Capacity ===")
    capacity_bronze = BronzeCSVIngestion().ingest_server_capacity(days_back=3)
    capacity_silver = CapacityBronzeToSilver(capacity_bronze).transform()

    print(f"\n{'='*60}")
    print("Silver Layer Summary")
    print(f"{'='*60}")
    print(f"Energy records  : {len(energy_silver)}")
    print(f"Capacity records: {len(capacity_silver)}")
    print(f"\nEnergy Silver sample:")
    print(energy_silver[[
        "measurement_timestamp", "facility_id", "facility_metro",
        "energy_demand_mwh", "demand_tier", "is_peak_hour"
    ]].head(3).to_string())
