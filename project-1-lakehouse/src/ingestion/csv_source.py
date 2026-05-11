"""
csv_source.py
-------------
Generates and ingests simulated facility operations CSV files
into the Bronze layer.

Business context:
    Data center operations teams export daily CSV reports from their
    facility management systems covering server capacity, cooling,
    network throughput, and customer billing. This module simulates
    those exports and ingests them into Bronze.

In production:
    Replace the data generator with a real SFTP pickup, SharePoint
    download, or blob storage trigger via Databricks Auto Loader.
"""

import os
import random
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from loguru import logger
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FACILITIES = ["DAL-01", "DAL-02", "ATL-01", "CHI-01", "SEA-01",
              "NYC-01", "LAX-01", "MIA-01", "DEN-01", "PHX-01",
              "BOS-01", "HOU-01"]

RACK_TYPES  = ["standard_1U", "blade", "gpu_compute", "storage_dense", "networking"]
CUSTOMERS   = [fake.company() for _ in range(30)]  # 30 simulated enterprise customers


# ---------------------------------------------------------------------------
# Data Generators
# ---------------------------------------------------------------------------

def generate_server_capacity_data(
    facility_ids: list[str],
    date: datetime,
    rows_per_facility: int = 24,
) -> pd.DataFrame:
    """
    Simulate hourly server capacity metrics per facility.
    Represents data that would come from a DCIM (Data Center
    Infrastructure Management) system export.
    """
    records = []
    for facility in facility_ids:
        for hour in range(rows_per_facility):
            ts = date.replace(hour=hour, minute=0, second=0, microsecond=0)

            # Simulate realistic utilization curves
            # Peak hours: 9AM–6PM business hours
            is_peak = 9 <= hour <= 18
            base_util = random.uniform(0.65, 0.80) if is_peak else random.uniform(0.40, 0.60)

            records.append({
                "facility_id":           facility,
                "report_timestamp":      ts.isoformat(),
                "report_hour":           hour,
                "total_rack_units":      random.randint(800, 2000),
                "occupied_rack_units":   int(random.randint(800, 2000) * base_util),
                "server_utilization_pct": round(base_util * 100, 2),
                "rack_type":             random.choice(RACK_TYPES),
                "cooling_temp_celsius":  round(random.uniform(18.0, 27.0), 1),
                "cooling_efficiency_pue": round(random.uniform(1.2, 1.8), 3),
                "active_servers":        random.randint(200, 800),
                "failed_servers":        random.randint(0, 5),
                "maintenance_servers":   random.randint(0, 10),
                "network_throughput_gbps": round(random.uniform(10.0, 100.0), 2),
                "network_utilization_pct": round(random.uniform(30.0, 85.0), 2),
                "report_source":         "DCIM_EXPORT",
            })

    df = pd.DataFrame(records)
    logger.info(f"Generated {len(df)} server capacity records for {len(facility_ids)} facilities")
    return df


def generate_billing_data(
    facility_ids: list[str],
    date: datetime,
    rows_per_facility: int = 10,
) -> pd.DataFrame:
    """
    Simulate daily customer billing records per facility.
    Represents data that would come from a billing/ERP system export.
    """
    records = []
    for facility in facility_ids:
        for _ in range(rows_per_facility):
            rack_count  = random.randint(1, 50)
            rate_per_rack = random.uniform(800, 2500)  # monthly rate per rack

            records.append({
                "facility_id":         facility,
                "billing_date":        date.strftime("%Y-%m-%d"),
                "customer_id":         fake.uuid4(),
                "customer_name":       random.choice(CUSTOMERS),
                "contract_type":       random.choice(["colocation", "wholesale", "cloud"]),
                "rack_count":          rack_count,
                "power_allocation_kw": round(rack_count * random.uniform(2.0, 10.0), 2),
                "monthly_rate_usd":    round(rack_count * rate_per_rack, 2),
                "daily_revenue_usd":   round((rack_count * rate_per_rack) / 30, 2),
                "contract_start_date": fake.date_between(start_date="-3y", end_date="-30d").isoformat(),
                "contract_end_date":   fake.date_between(start_date="+30d", end_date="+3y").isoformat(),
                "invoice_status":      random.choice(["paid", "pending", "overdue"]),
                "report_source":       "ERP_BILLING_EXPORT",
            })

    df = pd.DataFrame(records)
    logger.info(f"Generated {len(df)} billing records for {len(facility_ids)} facilities")
    return df


# ---------------------------------------------------------------------------
# Bronze CSV Ingestion
# ---------------------------------------------------------------------------

class BronzeCSVIngestion:
    """
    Ingests facility CSV exports into Bronze layer with full audit metadata.

    Simulates the pattern used when an operations team drops CSV files
    into a blob storage folder and Databricks Auto Loader picks them up.
    """

    def __init__(self, output_dir: str = "data/raw"):
        self.output_dir = output_dir
        self.ingestion_timestamp = datetime.utcnow().isoformat()
        os.makedirs(output_dir, exist_ok=True)

    def _add_bronze_metadata(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Stamp every Bronze record with audit columns."""
        df["_ingestion_timestamp"] = self.ingestion_timestamp
        df["_source_system"]       = "CSV_FACILITY_EXPORT"
        df["_source_file"]         = source_file
        df["_batch_id"]            = f"csv_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        df["_row_hash"]            = df.apply(
            lambda r: str(hash(tuple(r))), axis=1
        )
        return df

    def ingest_server_capacity(self, days_back: int = 7) -> pd.DataFrame:
        """
        Generate and ingest server capacity CSVs for the past N days.
        Returns combined Bronze DataFrame.
        """
        all_dfs = []
        for i in range(days_back):
            date = datetime.utcnow() - timedelta(days=i)
            df   = generate_server_capacity_data(FACILITIES, date)
            fname = f"server_capacity_{date.strftime('%Y%m%d')}.csv"
            fpath = os.path.join(self.output_dir, fname)
            df.to_csv(fpath, index=False)

            df = self._add_bronze_metadata(df, fname)
            all_dfs.append(df)
            logger.success(f"Saved → {fpath}")

        combined = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Total server capacity Bronze records: {len(combined)}")
        return combined

    def ingest_billing(self, days_back: int = 7) -> pd.DataFrame:
        """
        Generate and ingest billing CSVs for the past N days.
        Returns combined Bronze DataFrame.
        """
        all_dfs = []
        for i in range(days_back):
            date = datetime.utcnow() - timedelta(days=i)
            df   = generate_billing_data(FACILITIES, date)
            fname = f"billing_{date.strftime('%Y%m%d')}.csv"
            fpath = os.path.join(self.output_dir, fname)
            df.to_csv(fpath, index=False)

            df = self._add_bronze_metadata(df, fname)
            all_dfs.append(df)
            logger.success(f"Saved → {fpath}")

        combined = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Total billing Bronze records: {len(combined)}")
        return combined


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ingestion = BronzeCSVIngestion(output_dir="data/raw")

    print("\n=== Generating Server Capacity Data ===")
    capacity_df = ingestion.ingest_server_capacity(days_back=3)

    print("\n=== Generating Billing Data ===")
    billing_df = ingestion.ingest_billing(days_back=3)

    print(f"\n{'='*60}")
    print(f"CSV Bronze Ingestion Complete")
    print(f"{'='*60}")
    print(f"Server capacity records : {len(capacity_df)}")
    print(f"Billing records         : {len(billing_df)}")
    print(f"Facilities covered      : {capacity_df['facility_id'].nunique()}")
    print(f"\nCapacity sample:\n")
    print(capacity_df[["facility_id", "report_timestamp", "server_utilization_pct",
                        "cooling_temp_celsius", "_batch_id"]].head(3).to_string())
    print(f"\nBilling sample:\n")
    print(billing_df[["facility_id", "billing_date", "customer_name",
                       "daily_revenue_usd", "invoice_status"]].head(3).to_string())
