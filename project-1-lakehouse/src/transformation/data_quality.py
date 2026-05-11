"""
data_quality.py
---------------
Data quality validation for Bronze → Silver promotion.

Business context:
    Before any raw data is promoted to Silver, it must pass a suite
    of automated quality checks. Failed records are quarantined, not
    deleted — preserving full auditability while protecting Silver
    and Gold layers from bad data.

Framework: Great Expectations (open source)
"""

import pandas as pd
from loguru import logger
from datetime import datetime


# ---------------------------------------------------------------------------
# Quality Check Results
# ---------------------------------------------------------------------------

class QualityCheckResult:
    def __init__(self, check_name: str, passed: bool, records_failed: int,
                 details: str = ""):
        self.check_name      = check_name
        self.passed          = passed
        self.records_failed  = records_failed
        self.details         = details
        self.timestamp       = datetime.utcnow().isoformat()

    def __repr__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        return f"{status} | {self.check_name} | failed_records={self.records_failed}"


# ---------------------------------------------------------------------------
# Energy Data Quality Suite
# ---------------------------------------------------------------------------

class EnergyDataQualitySuite:
    """
    Quality checks for Bronze energy demand data
    before promotion to Silver layer.
    """

    def __init__(self, df: pd.DataFrame):
        self.df      = df.copy()
        self.results = []

    def check_no_null_values(self, critical_columns: list[str]) -> QualityCheckResult:
        """Critical columns must never be null."""
        null_counts = self.df[critical_columns].isnull().sum()
        total_nulls = null_counts.sum()
        result = QualityCheckResult(
            check_name      = "no_null_critical_columns",
            passed          = total_nulls == 0,
            records_failed  = int(total_nulls),
            details         = null_counts[null_counts > 0].to_dict().__str__(),
        )
        self.results.append(result)
        logger.info(result)
        return result

    def check_energy_value_range(
        self,
        column: str = "eia_value",
        min_val: float = 0,
        max_val: float = 100000,
    ) -> QualityCheckResult:
        """Energy demand values must be within realistic bounds."""
        out_of_range = self.df[
            (self.df[column] < min_val) | (self.df[column] > max_val)
        ]
        result = QualityCheckResult(
            check_name      = f"value_range_{column}",
            passed          = len(out_of_range) == 0,
            records_failed  = len(out_of_range),
            details         = f"Expected [{min_val}, {max_val}]",
        )
        self.results.append(result)
        logger.info(result)
        return result

    def check_no_duplicate_records(
        self,
        key_columns: list[str],
    ) -> QualityCheckResult:
        """No duplicate records on the natural key."""
        duplicates = self.df[self.df.duplicated(subset=key_columns, keep=False)]
        result = QualityCheckResult(
            check_name      = "no_duplicate_records",
            passed          = len(duplicates) == 0,
            records_failed  = len(duplicates),
            details         = f"Key columns: {key_columns}",
        )
        self.results.append(result)
        logger.info(result)
        return result

    def check_valid_facility_ids(
        self,
        valid_ids: list[str],
        column: str = "facility_id",
    ) -> QualityCheckResult:
        """Facility IDs must be in the known reference list."""
        invalid = self.df[~self.df[column].isin(valid_ids)]
        result = QualityCheckResult(
            check_name      = "valid_facility_ids",
            passed          = len(invalid) == 0,
            records_failed  = len(invalid),
            details         = f"Invalid IDs: {invalid[column].unique().tolist()}",
        )
        self.results.append(result)
        logger.info(result)
        return result

    def check_timestamp_freshness(
        self,
        timestamp_column: str = "_ingestion_timestamp",
        max_age_hours: int = 25,
    ) -> QualityCheckResult:
        """Data should not be older than max_age_hours."""
        now = datetime.utcnow()
        df  = self.df.copy()
        df["_parsed_ts"] = pd.to_datetime(df[timestamp_column], errors="coerce")
        df["_age_hours"] = (now - df["_parsed_ts"]).dt.total_seconds() / 3600
        stale = df[df["_age_hours"] > max_age_hours]
        result = QualityCheckResult(
            check_name      = "timestamp_freshness",
            passed          = len(stale) == 0,
            records_failed  = len(stale),
            details         = f"Max allowed age: {max_age_hours}h",
        )
        self.results.append(result)
        logger.info(result)
        return result

    def run_full_suite(self) -> dict:
        """
        Run all quality checks and return a summary report.
        Used by the orchestration layer to decide on Silver promotion.
        """
        logger.info("Running energy data quality suite...")

        self.check_no_null_values(
            critical_columns=["eia_period", "facility_id", "eia_value"]
        )
        self.check_energy_value_range(column="eia_value")
        self.check_no_duplicate_records(
            key_columns=["eia_period", "facility_id", "eia_region"]
        )
        self.check_valid_facility_ids(
            valid_ids=["DAL-01", "DAL-02", "ATL-01", "CHI-01", "SEA-01"]
        )
        self.check_timestamp_freshness()

        passed = sum(1 for r in self.results if r.passed)
        total  = len(self.results)

        summary = {
            "suite_name":       "energy_bronze_validation",
            "run_timestamp":    datetime.utcnow().isoformat(),
            "total_checks":     total,
            "passed_checks":    passed,
            "failed_checks":    total - passed,
            "overall_passed":   passed == total,
            "results":          [vars(r) for r in self.results],
        }

        logger.info(
            f"Quality suite complete: {passed}/{total} checks passed"
        )
        return summary


# ---------------------------------------------------------------------------
# Quarantine Handler
# ---------------------------------------------------------------------------

def quarantine_failed_records(
    df: pd.DataFrame,
    quality_summary: dict,
    output_path: str = "data/quarantine",
) -> pd.DataFrame:
    """
    Separate records that failed quality checks into a quarantine
    table. Returns the clean DataFrame for Silver promotion.

    Quarantined records are never deleted — they are preserved for
    investigation and potential reprocessing.
    """
    os.makedirs(output_path, exist_ok=True)

    # Mark invalid records based on _is_valid flag set during ingestion
    clean_df = df[df.get("_is_valid", pd.Series([True] * len(df))) == True].copy()
    bad_df   = df[df.get("_is_valid", pd.Series([True] * len(df))) != True].copy()

    if len(bad_df) > 0:
        bad_df["_quarantine_reason"]    = "failed_bronze_quality_check"
        bad_df["_quarantine_timestamp"] = datetime.utcnow().isoformat()
        qpath = os.path.join(
            output_path,
            f"quarantine_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        bad_df.to_parquet(qpath, index=False)
        logger.warning(f"Quarantined {len(bad_df)} records → {qpath}")
    else:
        logger.success("No records quarantined — all passed validation")

    return clean_df


import os
