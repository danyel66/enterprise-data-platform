"""
silver_to_gold.py
-----------------
Aggregates Silver records into Gold layer business KPIs.

Gold layer principles:
  - Business-ready, pre-aggregated for speed
  - Named for business consumers, not systems
  - Every table answers a specific business question
  - Connects directly to Power BI and ML feature store

Gold tables produced:
  1. gold_facility_energy_kpis     — Daily energy KPIs per facility
  2. gold_regional_performance     — Regional rollup for executive view
  3. gold_capacity_summary         — Server utilization + availability
  4. gold_ml_features              — Feature table for demand forecasting
"""

import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger


# ---------------------------------------------------------------------------
# Gold Table 1: Facility Energy KPIs
# ---------------------------------------------------------------------------

def build_gold_facility_energy_kpis(silver_energy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Daily energy demand KPIs per facility.

    Business question answered:
        'How much energy did each facility consume per day,
         and how does that compare to our baseline?'

    Consumers: Operations team, Power BI dashboard, Finance
    """
    logger.info("Building gold_facility_energy_kpis")

    df = silver_energy_df.copy()

    gold = df.groupby(["facility_id", "facility_metro", "facility_region",
                       "measurement_date"]).agg(
        total_energy_mwh        = ("energy_demand_mwh", "sum"),
        avg_hourly_demand_mwh   = ("energy_demand_mwh", "mean"),
        peak_demand_mwh         = ("energy_demand_mwh", "max"),
        min_demand_mwh          = ("energy_demand_mwh", "min"),
        demand_std_dev          = ("energy_demand_mwh", "std"),
        peak_hour_count         = ("is_peak_hour", "sum"),
        weekend_hour_count      = ("is_weekend", "sum"),
        high_demand_hour_count  = ("demand_tier", lambda x: (x == "high").sum()),
        data_points             = ("energy_demand_mwh", "count"),
    ).reset_index()

    # Derived KPIs
    gold["peak_to_avg_ratio"] = (
        gold["peak_demand_mwh"] / gold["avg_hourly_demand_mwh"]
    ).round(3)

    gold["demand_volatility"] = (
        gold["demand_std_dev"] / gold["avg_hourly_demand_mwh"]
    ).round(3)

    gold["data_completeness_pct"] = (
        (gold["data_points"] / 24) * 100
    ).round(1)

    # Gold metadata
    gold["_gold_layer"]             = "gold"
    gold["_gold_table"]             = "facility_energy_kpis"
    gold["_gold_build_timestamp"]   = datetime.utcnow().isoformat()

    logger.success(f"gold_facility_energy_kpis: {len(gold)} rows")
    return gold


# ---------------------------------------------------------------------------
# Gold Table 2: Regional Performance (Executive View)
# ---------------------------------------------------------------------------

def build_gold_regional_performance(gold_facility_df: pd.DataFrame) -> pd.DataFrame:
    """
    Regional energy rollup for C-suite and executive dashboards.

    Business question answered:
        'Which regions are consuming the most energy and
         what is our cost exposure by geography?'

    Consumers: Executive dashboard, Finance, Real estate planning
    """
    logger.info("Building gold_regional_performance")

    gold = gold_facility_df.groupby(
        ["facility_region", "measurement_date"]
    ).agg(
        total_regional_energy_mwh   = ("total_energy_mwh", "sum"),
        avg_facility_demand_mwh     = ("avg_hourly_demand_mwh", "mean"),
        peak_facility_demand_mwh    = ("peak_demand_mwh", "max"),
        facility_count              = ("facility_id", "nunique"),
        avg_peak_to_avg_ratio       = ("peak_to_avg_ratio", "mean"),
        avg_data_completeness_pct   = ("data_completeness_pct", "mean"),
    ).reset_index()

    # Estimated cost — $0.08/kWh average US industrial rate
    gold["estimated_daily_cost_usd"] = (
        gold["total_regional_energy_mwh"] * 1000 * 0.08
    ).round(2)

    gold["cost_per_facility_usd"] = (
        gold["estimated_daily_cost_usd"] / gold["facility_count"]
    ).round(2)

    gold["_gold_layer"]           = "gold"
    gold["_gold_table"]           = "regional_performance"
    gold["_gold_build_timestamp"] = datetime.utcnow().isoformat()

    logger.success(f"gold_regional_performance: {len(gold)} rows")
    return gold


# ---------------------------------------------------------------------------
# Gold Table 3: Capacity Summary
# ---------------------------------------------------------------------------

def build_gold_capacity_summary(silver_capacity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Daily server capacity and availability summary per facility.

    Business question answered:
        'Are our facilities running at safe utilization levels
         and what is our infrastructure availability SLA?'

    Consumers: Operations, SLA reporting, Customer success
    """
    logger.info("Building gold_capacity_summary")

    df = silver_capacity_df.copy()
    df["report_date"] = pd.to_datetime(df["report_timestamp"]).dt.date.astype(str)

    gold = df.groupby(["facility_id", "facility_metro", "report_date"]).agg(
        avg_server_utilization_pct  = ("server_utilization_pct", "mean"),
        max_server_utilization_pct  = ("server_utilization_pct", "max"),
        avg_cooling_temp_celsius    = ("cooling_temp_celsius", "mean"),
        max_cooling_temp_celsius    = ("cooling_temp_celsius", "max"),
        avg_pue                     = ("cooling_efficiency_pue", "mean"),
        avg_network_throughput_gbps = ("network_throughput_gbps", "mean"),
        peak_network_throughput_gbps= ("network_throughput_gbps", "max"),
        avg_availability_pct        = ("availability_pct", "mean"),
        high_utilization_hours      = ("is_high_utilization", "sum"),
        cooling_alert_hours         = ("is_cooling_alert", "sum"),
        pue_efficient_hours         = ("is_pue_efficient", "sum"),
        total_active_servers        = ("active_servers", "max"),
        total_failed_servers        = ("failed_servers", "max"),
    ).reset_index()

    # SLA classification
    conditions = [
        gold["avg_availability_pct"] >= 99.9,
        gold["avg_availability_pct"].between(99.0, 99.9),
        gold["avg_availability_pct"] < 99.0,
    ]
    gold["sla_tier"] = np.select(conditions, ["platinum", "gold", "at_risk"], "unknown")

    gold["_gold_layer"]           = "gold"
    gold["_gold_table"]           = "capacity_summary"
    gold["_gold_build_timestamp"] = datetime.utcnow().isoformat()

    logger.success(f"gold_capacity_summary: {len(gold)} rows")
    return gold


# ---------------------------------------------------------------------------
# Gold Table 4: ML Feature Table
# ---------------------------------------------------------------------------

def build_gold_ml_features(
    gold_energy_df: pd.DataFrame,
    gold_capacity_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Feature table for the demand forecasting ML model.

    Joins energy KPIs with capacity metrics to produce a
    rich feature set for predicting next-day energy demand.

    Consumers: MLflow demand forecasting model, Data science team
    """
    logger.info("Building gold_ml_features")

    energy = gold_energy_df[[
        "facility_id", "measurement_date",
        "total_energy_mwh", "avg_hourly_demand_mwh",
        "peak_demand_mwh", "peak_to_avg_ratio",
        "demand_volatility", "high_demand_hour_count",
        "peak_hour_count", "weekend_hour_count",
    ]].copy()
    energy = energy.rename(columns={"measurement_date": "date"})

    capacity = gold_capacity_df[[
        "facility_id", "report_date",
        "avg_server_utilization_pct", "avg_pue",
        "avg_network_throughput_gbps", "avg_availability_pct",
        "high_utilization_hours", "cooling_alert_hours",
    ]].copy()
    capacity = capacity.rename(columns={"report_date": "date"})

    features = energy.merge(capacity, on=["facility_id", "date"], how="left")

    # Lag features — previous day demand (key ML feature)
    features = features.sort_values(["facility_id", "date"])
    features["energy_demand_lag_1d"] = features.groupby("facility_id")[
        "total_energy_mwh"
    ].shift(1)
    features["energy_demand_lag_7d"] = features.groupby("facility_id")[
        "total_energy_mwh"
    ].shift(7)

    # Rolling average features
    features["demand_rolling_7d_avg"] = features.groupby("facility_id")[
        "total_energy_mwh"
    ].transform(lambda x: x.rolling(7, min_periods=1).mean()).round(2)

    # Target variable — next day demand (what the model predicts)
    features["target_next_day_demand_mwh"] = features.groupby("facility_id")[
        "total_energy_mwh"
    ].shift(-1)

    features["_gold_layer"]           = "gold"
    features["_gold_table"]           = "ml_features"
    features["_gold_build_timestamp"] = datetime.utcnow().isoformat()

    logger.success(f"gold_ml_features: {len(features)} rows | "
                   f"{len(features.columns)} features")
    return features


# ---------------------------------------------------------------------------
# Gold Pipeline Runner
# ---------------------------------------------------------------------------

def run_silver_to_gold(
    silver_energy_df: pd.DataFrame,
    silver_capacity_df: pd.DataFrame,
    output_dir: str = "data/processed",
) -> dict[str, pd.DataFrame]:
    """
    Run the full Silver → Gold pipeline and return all Gold tables.
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    logger.info("Starting Silver → Gold pipeline")

    gold_energy   = build_gold_facility_energy_kpis(silver_energy_df)
    gold_regional = build_gold_regional_performance(gold_energy)
    gold_capacity = build_gold_capacity_summary(silver_capacity_df)
    gold_features = build_gold_ml_features(gold_energy, gold_capacity)

    gold_tables = {
        "facility_energy_kpis": gold_energy,
        "regional_performance": gold_regional,
        "capacity_summary":     gold_capacity,
        "ml_features":          gold_features,
    }

    for name, df in gold_tables.items():
        path = os.path.join(output_dir, f"gold_{name}.parquet")
        df.to_parquet(path, index=False)
        logger.success(f"Saved {name} → {path} ({len(df)} rows)")

    return gold_tables


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from src.ingestion.api_source import BronzeEnergyIngestion
    from src.ingestion.csv_source import BronzeCSVIngestion
    from src.transformation.bronze_to_silver import (
        EnergyBronzeToSilver, CapacityBronzeToSilver
    )

    print("\n=== Bronze Layer ===")
    energy_bronze   = BronzeEnergyIngestion().ingest_all_regions(days_back=7)
    capacity_bronze = BronzeCSVIngestion().ingest_server_capacity(days_back=7)

    print("\n=== Silver Layer ===")
    energy_silver   = EnergyBronzeToSilver(energy_bronze).transform()
    capacity_silver = CapacityBronzeToSilver(capacity_bronze).transform()

    print("\n=== Gold Layer ===")
    gold_tables = run_silver_to_gold(energy_silver, capacity_silver)

    print(f"\n{'='*60}")
    print("Gold Layer Summary")
    print(f"{'='*60}")
    for name, df in gold_tables.items():
        print(f"  {name:<30} {len(df):>6} rows | {len(df.columns):>3} columns")

    print(f"\nRegional Performance Sample:")
    print(gold_tables["regional_performance"][[
        "facility_region", "measurement_date",
        "total_regional_energy_mwh", "estimated_daily_cost_usd",
        "facility_count"
    ]].head(5).to_string())
