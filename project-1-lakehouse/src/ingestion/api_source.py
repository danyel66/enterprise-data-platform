"""
api_source.py
-------------
Ingests hourly electricity demand data from the U.S. Energy Information
Administration (EIA) API into the Bronze layer of the lakehouse.

Business context:
    Data center power consumption is directly tied to regional grid demand.
    This module pulls real hourly energy data to simulate facility-level
    power usage patterns across 12 data center locations.

EIA API Docs: https://www.eia.gov/opendata/
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EIA_BASE_URL = os.getenv("EIA_BASE_URL", "https://api.eia.gov/v2")
EIA_API_KEY  = os.getenv("EIA_API_KEY", "")

# EIA region codes mapped to simulated data center facility names
FACILITY_REGION_MAP = {
    "TEX":  "DAL-01",   # Texas         → Dallas Facility 1
    "MIDA": "DAL-02",   # Mid-Atlantic   → Dallas Facility 2
    "CAR":  "ATL-01",   # Carolinas      → Atlanta Facility 1
    "CENT": "CHI-01",   # Central        → Chicago Facility 1
    "NW":   "SEA-01",   # Northwest      → Seattle Facility 1
}


# ---------------------------------------------------------------------------
# EIA Client
# ---------------------------------------------------------------------------

class EIAEnergyClient:
    """
    Client for the EIA Electricity Demand API.

    Pulls hourly electricity demand by region and maps each region
    to a simulated data center facility for Bronze layer ingestion.
    """

    def __init__(self, api_key: str = EIA_API_KEY):
        if not api_key:
            raise ValueError(
                "EIA_API_KEY is not set. Register free at https://www.eia.gov/opendata/"
            )
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"X-Params": json.dumps({"api_key": api_key})})
        logger.info("EIAEnergyClient initialized")

    def fetch_hourly_demand(
        self,
        region: str,
        start_date: str,
        end_date: str,
        limit: int = 500,
    ) -> list[dict]:
        """
        Fetch hourly electricity demand for a given EIA region.

        Args:
            region:     EIA region code (e.g. 'TEX', 'MIDA')
            start_date: ISO date string 'YYYY-MM-DDTHH'
            end_date:   ISO date string 'YYYY-MM-DDTHH'
            limit:      Max records per request (EIA max = 5000)

        Returns:
            List of raw demand records as dicts
        """
        endpoint = f"{EIA_BASE_URL}/electricity/rto/region-data/data/"

        params = {
            "api_key":          self.api_key,
            "frequency":        "hourly",
            "data[0]":          "value",
            "facets[respondent][]": region,
            "facets[type][]":   "D",          # D = Demand
            "start":            start_date,
            "end":              end_date,
            "sort[0][column]":  "period",
            "sort[0][direction]": "asc",
            "length":           limit,
            "offset":           0,
        }

        logger.info(f"Fetching EIA demand | region={region} | {start_date} → {end_date}")

        try:
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            records = data.get("response", {}).get("data", [])
            logger.success(f"Fetched {len(records)} records for region {region}")
            return records

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching EIA data: {e}")
            raise
        except requests.exceptions.Timeout:
            logger.error("EIA API request timed out")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise


# ---------------------------------------------------------------------------
# Bronze Ingestion
# ---------------------------------------------------------------------------

class BronzeEnergyIngestion:
    """
    Transforms raw EIA API records into Bronze layer schema.

    Bronze layer principles:
      - No business logic
      - No transformations beyond minimal typing
      - Every record stamped with ingestion metadata
      - Failures logged, not silently dropped
    """

    def __init__(self):
        self.client = EIAEnergyClient()
        self.ingestion_timestamp = datetime.utcnow().isoformat()

    def _add_bronze_metadata(self, record: dict, region: str) -> dict:
        """
        Add Bronze layer audit columns to every record.
        These columns are never modified in Silver or Gold.
        """
        return {
            # Original EIA fields
            "eia_period":        record.get("period"),
            "eia_respondent":    record.get("respondent"),
            "eia_respondent_name": record.get("respondent-name"),
            "eia_type":          record.get("type"),
            "eia_type_name":     record.get("type-name"),
            "eia_value":         record.get("value"),
            "eia_value_units":   record.get("value-units"),

            # Facility mapping
            "facility_id":       FACILITY_REGION_MAP.get(region, f"UNKNOWN-{region}"),
            "eia_region":        region,

            # Bronze audit metadata
            "_ingestion_timestamp": self.ingestion_timestamp,
            "_source_system":       "EIA_API",
            "_source_endpoint":     "electricity/rto/region-data",
            "_batch_id":            f"eia_{region}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "_is_valid":            record.get("value") is not None,
        }

    def ingest_all_regions(
        self,
        days_back: int = 7,
        output_path: str = None,
    ) -> pd.DataFrame:
        """
        Ingest hourly demand for all mapped facility regions.

        Args:
            days_back:   Number of days of history to pull
            output_path: If provided, saves Bronze data as parquet locally

        Returns:
            Pandas DataFrame of all Bronze records
        """
        end_dt   = datetime.utcnow()
        start_dt = end_dt - timedelta(days=days_back)

        start_str = start_dt.strftime("%Y-%m-%dT%H")
        end_str   = end_dt.strftime("%Y-%m-%dT%H")

        all_records = []

        for region, facility in FACILITY_REGION_MAP.items():
            logger.info(f"Processing region={region} → facility={facility}")
            try:
                raw_records = self.client.fetch_hourly_demand(
                    region=region,
                    start_date=start_str,
                    end_date=end_str,
                )
                bronze_records = [
                    self._add_bronze_metadata(r, region) for r in raw_records
                ]
                all_records.extend(bronze_records)
                logger.success(f"Region {region}: {len(bronze_records)} Bronze records staged")

            except Exception as e:
                logger.error(f"Failed to ingest region {region}: {e}")
                # Continue with other regions — partial failure is acceptable at Bronze
                continue

        df = pd.DataFrame(all_records)
        logger.info(f"Total Bronze records: {len(df)}")

        if output_path:
            df.to_parquet(output_path, index=False)
            logger.success(f"Bronze data saved → {output_path}")

        return df

    def get_schema(self) -> dict:
        """
        Returns the expected Bronze schema for documentation
        and Great Expectations suite generation.
        """
        return {
            "eia_period":               "string",
            "eia_respondent":           "string",
            "eia_respondent_name":      "string",
            "eia_type":                 "string",
            "eia_type_name":            "string",
            "eia_value":                "float",
            "eia_value_units":          "string",
            "facility_id":              "string",
            "eia_region":               "string",
            "_ingestion_timestamp":     "string",
            "_source_system":           "string",
            "_source_endpoint":         "string",
            "_batch_id":                "string",
            "_is_valid":                "boolean",
        }


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """
    Run directly to test local ingestion:
        python src/ingestion/api_source.py
    """
    import os

    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)

    ingestion = BronzeEnergyIngestion()

    df = ingestion.ingest_all_regions(
        days_back=3,
        output_path=f"{output_dir}/bronze_energy_{datetime.utcnow().strftime('%Y%m%d')}.parquet",
    )

    print(f"\n{'='*60}")
    print(f"Bronze Ingestion Complete")
    print(f"{'='*60}")
    print(f"Total records : {len(df)}")
    print(f"Regions       : {df['eia_region'].nunique()}")
    print(f"Facilities    : {df['facility_id'].nunique()}")
    print(f"Date range    : {df['eia_period'].min()} → {df['eia_period'].max()}")
    print(f"Valid records : {df['_is_valid'].sum()} / {len(df)}")
    print(f"\nSample:\n")
    print(df.head(3).to_string())
