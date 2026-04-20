# 🚲 CitiBike NYC: End-to-End Data Engineering Pipeline

[![Dashboard](https://img.shields.io/badge/Looker%20Studio-Live%20Dashboard-blue?style=for-the-badge&logo=googlestreams)](https://datastudio.google.com/s/pOa7a2lKIbU)

## 1. Project Overview
This project is an automated **End-to-End Data Pipeline** designed to ingest, store, and transform NYC CitiBike data. It handles millions of historical trip records, station metadata, and real-time station availability.

**The Problem:** CitiBike data is fragmented across various API endpoints and historical archives. This pipeline centralizes that data into a Cloud Data Warehouse, enabling complex analysis of urban mobility patterns through an optimized BI dashboard.

---

## 2. Tech Stack & Architecture
The project follows a modular **ELT (Extract, Load, Transform)** architecture orchestrated via Python:

* **Extraction:** Python scripts fetching data from CitiBike GBFS APIs.
* **Data Lake:** **Google Cloud Storage (GCS)** for raw file persistence.
* **Data Warehouse:** **Google BigQuery**, optimized with partitioning and clustering.
* **Transformation:** **dbt (Data Build Tool)** for modular, version-controlled SQL modeling.
* **Orchestration:** A custom Python runner (`run_pipeline.py`) managing execution flow and cost optimization.

---

## 3. Intelligent Orchestration (Cost Optimization)
The pipeline is designed to be cost-aware, distinguishing between **historical data** (static/monthly) and **operational data** (dynamic/hourly).

To avoid the "overkill" of processing millions of rows every hour, the orchestrator supports two modes:

* **Full Pipeline (Daily/Monthly):** Runs the entire ingestion suite and rebuilds all dbt models.
    ```bash
    python run_pipeline.py
    ```
* **Lightweight Update (Hourly):** Only fetches real-time `station_status`, uploads to GCS, and refreshes specific status models.
    ```bash
    python run_pipeline.py --only-status
    ```

**Impact:** This strategy reduces BigQuery compute costs and GCS operations by approximately **80%** compared to a naive hourly full-rebuild strategy.

---

## 4. Data Warehouse Optimization
To ensure high performance and lower query costs in BigQuery, the following strategies were implemented:

* **Partitioning:** The trips table is partitioned by **Day** (`started_at`). This allows BigQuery to prune data outside the requested time range during scans.
* **Clustering:** Tables are clustered by `start_station_name`. This optimizes performance for station-specific filters and geographical analysis frequently used in the dashboard.

---

## 5. dbt Transformation Layers
The data is transformed using 8 dbt models focused on data quality and business logic:
* **Staging (`stg_`):** Cleans raw data, renames columns for consistency, and casts data types.
* **Marts (`mart_`):** Final aggregated tables (e.g., `mart_daily_rides`, `mart_station_metrics`) that power the Looker Studio dashboard with sub-second response times.

---

## 6. Setup & Reproducibility

### Prerequisites
* Python 3.10+
* GCP Project with a Service Account (Roles: BigQuery Admin, Storage Admin).

### Installation
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/aecarlos/citibike-data-pipeline.git
    cd citibike-data-pipeline
    ```
2.  **Configuration:**
    * Add your GCP JSON key to `configs/gcp_credentials.json`.
    * Define your project and bucket variables in a `configs/.env` file.
3.  **Install & Run:**
    ```bash
    pip install -r requirements.txt
    python run_pipeline.py
    ```

---

> **Security Note:** Credentials and environment variables are explicitly excluded from version control via `.gitignore`.
