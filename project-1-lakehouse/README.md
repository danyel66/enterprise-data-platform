# Project 1: Azure Databricks Lakehouse Platform

## Business Problem

A growing data center company operates 12 facilities across the US. Operational data — power consumption, server capacity, network throughput, and customer billing — lives in separate systems. Leadership makes decisions based on week-old exports and gut instinct.

This platform centralizes all operational data into a governed, medallion-architecture lakehouse on Azure Databricks. Clean, trusted data is available in real time for analytics, reporting, and predictive AI workloads.

## Business Value

| Before | After |
|--------|-------|
| Data scattered across 5 systems | Single governed lakehouse |
| Weekly manual exports | Automated incremental ingestion |
| No data quality checks | Automated validation + anomaly alerts |
| Analysts wait 2 days for reports | Self-serve Gold layer queries |
| No audit trail | Full lineage via Unity Catalog |

## Architecture
## Tech Stack

| Layer | Technology |
|-------|-----------|
| Compute | Azure Databricks (Spark 3.5) |
| Storage | Azure Data Lake Storage Gen2 |
| Table Format | Delta Lake |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows |
| Data Quality | Great Expectations |
| Streaming | Confluent Kafka + Spark Structured Streaming |
| Visualization | Power BI |
| ML Tracking | MLflow |
| IaC | Azure CLI + Bash |
| Language | Python 3.11 / PySpark / SQL |

## Data Sources

1. **EIA Energy API** — Real hourly energy consumption data (public, free)
2. **Facility Operations CSVs** — Simulated server capacity and network throughput
3. **Kafka Stream** — Simulated real-time power sensor readings (Confluent Cloud free tier)

## Project Structure## Setup Instructions

### Prerequisites
- Azure account (free tier works)
- Databricks workspace on Azure
- Confluent Cloud account (free tier)
- Python 3.11+

### Local Setup

```bash
git clone git@github.com:danyel66/enterprise-data-platform.git
cd enterprise-data-platform/project-1-lakehouse
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Fill in your credentials in .env
```

### Azure Setup

```bash
chmod +x infra/azure_setup.sh
./infra/azure_setup.sh
```

## Medallion Layer Definitions

| Layer | Purpose | Update Frequency | Consumers |
|-------|---------|-----------------|-----------|
| Bronze | Raw, unmodified source data | Real-time / batch | Data Engineers |
| Silver | Cleaned, conformed, deduplicated | Every 15 min | Analysts, Engineers |
| Gold | Aggregated, business-ready KPIs | Hourly | Executives, BI, ML |

## Unity Catalog Governance Model## Author

**Daniel Nduka** — Data Engineer | Azure Databricks Certified
- GitHub: [danyel66](https://github.com/danyel66)
- LinkedIn: [Daniel Nduka](https://www.linkedin.com/in/daniel-nduka-0bab5b21a/)
