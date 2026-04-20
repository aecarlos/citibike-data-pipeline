# 🚲 CitiBike NYC: End-to-End Data Engineering Pipeline

[![Dashboard](https://img.shields.io/badge/Looker%20Studio-Live%20Dashboard-blue?style=for-the-badge&logo=googlestreams)](https://datastudio.google.com/reporting/c9f44950-67e5-45ca-8f33-2705a7755096)

## 1. Project Overview
This project is an automated **End-to-End Data Pipeline** designed to ingest, store, and transform NYC CitiBike data. It handles millions of historical trip records, station metadata, and real-time station availability.

**The Problem:** CitiBike data is fragmented across various API endpoints and monthly archives. This pipeline centralizes that data into a Google BigQuery Data Warehouse, enabling complex analysis of urban mobility patterns through an optimized BI dashboard.

## 2. Tech Stack & Architecture
The project follows a modular **ELT (Extract, Load, Transform)** architecture:

* **Extraction:** Python scripts fetching data from CitiBike GBFS APIs and S3 buckets.
* **Data Lake:** **Google Cloud Storage (GCS)** for raw file persistence.
* **Data Warehouse:** **Google BigQuery**, optimized with partitioning and clustering.
* **Transformation:** **dbt (Data Build Tool)** for modular, version-controlled SQL modeling.
* **Orchestration:** A custom Python runner (`run_pipeline.py`) managing execution flow and cost optimization.

## 3. Intelligent Orchestration (Cost Optimization)
The pipeline is designed to be cost-aware, distinguishing between **historical data** (static/monthly) and **operational data** (dynamic/hourly). To avoid unnecessary processing costs, the orchestrator supports two modes:

* **Full Pipeline (Daily/Monthly):** Runs the entire ingestion suite and rebuilds all dbt models.
    ```bash
    python run_pipeline.py
    ```
* **Lightweight Update (Hourly):** Only fetches real-time `station_status`, uploads to GCS, and refreshes specific status models.
    ```bash
    python run_pipeline.py --only-status
    ```

> **Impact:** This strategy reduces BigQuery compute costs and GCS operations by approximately **80%** compared to a naive hourly full-rebuild strategy.

## 4. Production Deployment & Automation
The production version of this pipeline is deployed on a **Google Cloud VM (Compute Engine)** to ensure 24/7 availability and dashboard freshness.

### Hourly Automation via Cron
The pipeline is automated via `crontab` to ensure the data stays current without manual intervention:
* **Schedule:** Every hour on the hour (`0 * * * *`).
* **Environment:** Executed via a dedicated Python Virtual Environment (`venv`) for dependency isolation.
* **Logging:** Automated runs are logged to `logs/cron_log.txt` for monitoring and debugging.

**To view the active crontab on the VM:**
```bash
crontab -l
```
## 5. Data Warehouse Optimization
To ensure high performance and lower query costs in BigQuery, the following strategies were implemented:

* **Partitioning:** The `trips` table is partitioned by **Day** (`started_at`). This allows BigQuery to prune data outside the requested time range during scans, significantly reducing processing costs.
* **Clustering:** Tables are clustered by `start_station_name`. This optimizes performance for station-specific filters and geographical analysis frequently used in the dashboard.

## 6. Setup & Reproducibility (Local Development)

While the VM handles production, you can reproduce this pipeline locally for development or testing by following these steps:

### Prerequisites
* **Python 3.10+**
* **GCP Project** with a Service Account (Required Roles: `BigQuery Admin`, `Storage Admin`).

### Installation & Configuration
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/aecarlos/citibike-data-pipeline.git](https://github.com/aecarlos/citibike-data-pipeline.git)
    cd citibike-data-pipeline
    ```
2.  **Environment Setup:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```
3.  **Local Credentials:**
    * Create a `.env` file in the `configs/` folder based on the project requirements.
    * Place your GCP JSON key at the path defined in your `.env` (e.g., `configs/gcp_credentials.json`).
4.  **Run the Pipeline:**
    ```bash
    # For a first-time full load
    python run_pipeline.py
    ```

---

> **Security Note:** Credentials and environment variables are explicitly excluded from version control via `.gitignore`. The project uses `python-dotenv` to manage secrets safely across both local and VM environments.
