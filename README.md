# 🚌 TransitFlow — Automated ETL Pipeline for Transit Delay Analysis

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square&logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?style=flat-square&logo=dbt)
![Airflow](https://img.shields.io/badge/Airflow-2.9.0-017CEE?style=flat-square&logo=apacheairflow)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![CI](https://github.com/dhruvipatel03/transitflow/actions/workflows/dbt_tests.yml/badge.svg)

An end-to-end data engineering pipeline that ingests **4.2 million+ rows** of real-world TTC (Toronto Transit Commission) GTFS transit data, transforms it using dbt, and orchestrates the entire workflow with Apache Airflow — all containerized with Docker.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│              TTC GTFS Static Feed (Toronto Open Data)           │
│         routes.txt │ stops.txt │ trips.txt │ stop_times.txt     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                              │
│                   Python + psycopg2                             │
│         Bulk COPY insert (100x faster than row-by-row)          │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│               RAW LAYER — PostgreSQL                            │
│  raw.routes │ raw.stops │ raw.trips │ raw.stop_times            │
│                    4,242,313+ rows                              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              TRANSFORMATION LAYER — dbt                         │
│  Staging Models:      stg_routes, stg_stops,                    │
│                       stg_trips, stg_stop_times                 │
│  Mart Models:         fct_trip_stops, fct_stops_per_route       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│             ANALYTICS LAYER — PostgreSQL                        │
│       analytics.fct_trip_stops                                  │
│       analytics.fct_stops_per_route                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              ORCHESTRATION — Apache Airflow                     │
│       DAG: transitflow_pipeline (@weekly schedule)              │
│       Task 1: ingest_gtfs_data → Task 2: run_dbt_models         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Version | Role |
|------|---------|------|
| **Python** | 3.11 | Ingestion scripts, data processing |
| **PostgreSQL** | 15 | Raw + analytics data warehouse |
| **dbt** | 1.11 | Data transformation & modeling |
| **Apache Airflow** | 2.9.0 | Pipeline orchestration & scheduling |
| **Docker Compose** | Latest | Containerization & reproducibility |
| **pandas** | 3.x | Data manipulation |
| **psycopg2** | Latest | PostgreSQL connectivity |

---

## 📊 Data Overview

| Table | Layer | Rows |
|-------|-------|------|
| `raw.routes` | Raw | 188 |
| `raw.stops` | Raw | 9,464 |
| `raw.trips` | Raw | 123,210 |
| `raw.stop_times` | Raw | 4,242,313 |
| `analytics.fct_trip_stops` | Mart | 4,242,313 |
| `analytics.fct_stops_per_route` | Mart | 188 |

---

## 📁 Project Structure

```
transitflow/
├── dags/
│   ├── transitflow_dag.py      # Airflow DAG definition
│   └── gtfs.zip                # GTFS feed (mounted into Airflow)
├── ingestion/
│   ├── fetch_static.py         # Python ingestion script
│   └── gtfs.zip                # Raw GTFS data source
├── transitflow/                # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_routes.sql
│   │   │   ├── stg_stops.sql
│   │   │   ├── stg_trips.sql
│   │   │   └── stg_stop_times.sql
│   │   └── marts/
│   │       ├── fct_trip_stops.sql
│   │       └── fct_stops_per_route.sql
│   └── dbt_project.yml
├── docker-compose.yml          # PostgreSQL container
├── docker-compose.airflow.yml  # Airflow containers
├── requirements.txt
└── .env.example
```

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop
- Python 3.11+
- Git

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/transitflow.git
cd transitflow
```

### 2. Set up environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 3. Start PostgreSQL
```bash
docker-compose up -d
```

### 4. Run ingestion
```bash
python ingestion/fetch_static.py
```

### 5. Run dbt transformations
```bash
cd transitflow
dbt run
```

### 6. Start Airflow
```bash
docker-compose -f docker-compose.airflow.yml up -d
```

Visit **http://localhost:8080** (admin / admin) and trigger the `transitflow_pipeline` DAG.

---

## 🔍 Sample Insights

Top 5 busiest TTC routes by number of trips:

| Route | Name | Trips | Stops |
|-------|------|-------|-------|
| 36 | FINCH WEST | 2,548 | 145 |
| 505 | DUNDAS | 2,363 | 84 |
| 504 | KING | 2,337 | 108 |
| 52 | LAWRENCE WEST | 2,298 | 195 |
| 1 | LINE 1 (YONGE-UNIVERSITY) | 2,207 | 76 |

---

## 📈 dbt Model Lineage

```
raw.routes ──────────────────────────────────────────┐
raw.stops  ──────► stg_stops ────────────────────────┤
raw.trips  ──────► stg_trips ──► stg_routes ──────────► fct_trip_stops
raw.stop_times ──► stg_stop_times ───────────────────┘
                                          │
                                          └──────────► fct_stops_per_route
```

---

## ✅ Key Engineering Decisions

**Bulk COPY over row-by-row INSERT** — Switched from individual INSERT statements to PostgreSQL's native COPY command via `cursor.copy_expert()`, reducing load time for 4.2M rows from 30+ minutes to under 30 seconds.

**Multi-layer dbt modeling** — Followed the staging → marts pattern to separate raw cleaning concerns from business logic, making models independently testable and reusable.

**Separate Docker Compose files** — Kept the data warehouse and Airflow infrastructure separate to allow independent scaling and easier debugging.

**Weekly DAG scheduling** — GTFS static feeds update weekly, so the pipeline is scheduled to match the upstream data cadence.

---

## 🎯 Future Improvements

- Add GTFS Realtime feed for live delay tracking
- Integrate dbt tests (`not_null`, `unique`, custom) for data quality
- Add Grafana dashboard connected to PostgreSQL analytics schema
- Replace PostgreSQL warehouse with BigQuery or Snowflake
- Set up CI/CD with GitHub Actions to run dbt tests on every push

---

## 👤 Author

Built as part of a Data Engineering portfolio for Fall 2026 internship applications.