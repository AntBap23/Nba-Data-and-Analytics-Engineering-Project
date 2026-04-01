# NBA Analytics Engineering Platform

An end-to-end analytics engineering project built around NBA player game data and a modern ELT stack:

- Python for ingestion
- Google Cloud Storage for the data lake
- BigQuery for the warehouse
- dbt for transformations
- Prefect for orchestration
- Power BI for analytics and reporting

The repository is structured like a production-style data platform, with clear separation between ingestion, storage, warehouse modeling, orchestration, testing, and BI.

## Overview

This project is designed to support multi-season NBA data pipelines from raw ingestion through analytics-ready models.

Key design principles:

- Raw data lands in `data/raw/` locally and can be promoted to GCS
- Warehouse fact tables are season-aware and include `season_id`
- BigQuery models are intended to be partitioned by `game_date` or `season_id`, depending on table grain
- dbt handles staging, intermediate, and mart transformations
- Prefect coordinates ingestion and transformation workflows
- Power BI sits on top of curated marts for business-facing analysis

## Data Model Direction

The platform is built for multi-season analysis rather than a single-season demo.

- `dim_seasons` is the conformed season dimension
- Fact tables such as player game stats and games include `season_id` as a foreign key
- Event-grain tables should generally be partitioned by `game_date`
- Season-grain tables can be partitioned by `season_id`

This makes the project easier to backfill, test, and scale as more seasons are added.

## Current Repository Layout

```text
.
├── analytics/          # Power BI assets, dashboards, and data dictionary
├── config/             # Environment and platform configuration placeholders
├── data/
│   └── raw/            # Local raw CSV extracts
├── data_lake/          # GCS zone conventions and storage scaffolding
├── dbt/                # dbt project used for warehouse transformations
├── docs/               # Architecture and data model documentation
├── ingestion/          # Python ingestion and load layer
├── logs/               # Logging config and runtime log folders
├── orchestration/      # Prefect flows, tasks, and deployment scaffolding
├── tests/              # Pipeline, dbt, and data quality tests
├── warehouse/          # BigQuery-oriented warehouse structure
├── .env                # Local environment variables (not for commit)
├── Makefile            # Project task runner placeholder
└── requirements.txt    # Python dependencies
```

## Core Layers

### Ingestion

The ingestion layer is responsible for collecting source data and moving it into cloud storage and the warehouse.

Primary files:

- `ingestion/ingest_player_logs.py`
- `ingestion/upload_to_gcs.py`
- `ingestion/load_to_bigquery.py`
- `ingestion/config.py`

Shared utilities:

- `ingestion/utils/gcs_client.py`
- `ingestion/utils/bq_client.py`
- `ingestion/utils/validation.py`

### dbt

The dbt project lives in the root-level `dbt/` directory and follows standard model layering:

- `models/staging/`
- `models/intermediate/`
- `models/marts/facts/`
- `models/marts/dimensions/`

Example models already scaffolded:

- `stg_player_game_logs`
- `int_player_game_enriched`
- `fact_player_game_stats`
- `fact_team_game_stats`
- `dim_players`
- `dim_teams`
- `dim_seasons`

The dbt project is configured under:

- `dbt/dbt_project.yml`
- `dbt/profiles.yml.example`

Note: the real `profiles.yml` should live in `~/.dbt/` and not be committed.

### Orchestration

Prefect is the orchestration layer for running ingestion and transformation workflows.

Key scaffolded files:

- `orchestration/flows/main_flow.py`
- `orchestration/tasks/ingestion_task.py`
- `orchestration/tasks/dbt_task.py`

### Analytics

The analytics layer is designed for Power BI deliverables and business-facing documentation.

Included areas:

- `analytics/powerbi/`
- `analytics/dashboards/`
- `analytics/data_dictionary/`

## Local Data

Historical CSV extracts are stored under `data/raw/`.

Current local extracts include:

- `player_logs_2015-16.csv`
- `player_logs_2016-17.csv`
- `player_logs_2017-18.csv`
- `player_logs_2018-19.csv`
- `player_logs_2019-20.csv`
- `player_logs_2020-21.csv`
- `player_logs_2021-22.csv`
- `player_logs_2022-23.csv`
- `player_logs_2023-24.csv`
- `player_logs_2024-25.csv`

These files provide a starting point for multi-season ingestion, warehouse loading, and transformation testing.

## Environment Configuration

Project configuration is driven by environment variables through the local `.env` file.

The current environment setup is designed to stay minimal and focused on:

- environment selection
- local data paths
- NBA ingestion settings
- GCP project and location
- a single GCS bucket
- a single BigQuery dataset
- dbt configuration
- Prefect settings
- logging settings

## Getting Started

### 1. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure dbt locally

Create your real dbt profile in:

```bash
~/.dbt/profiles.yml
```

Make sure it includes a profile named `nba_analytics`, since that is what the project expects in `dbt/dbt_project.yml`.

### 4. Review local environment variables

Update your local `.env` with the values needed for:

- `GCP_PROJECT_ID`
- `GCS_BUCKET`
- `BQ_DATASET`
- `DBT_TARGET`
- `PREFECT_API_URL`

### 5. Run ingestion or transformation steps

Depending on what you are building next, typical workflows will be:

```bash
python ingestion/ingest_player_logs.py
python ingestion/upload_to_gcs.py
python ingestion/load_to_bigquery.py
```

and later:

```bash
cd dbt
dbt debug
dbt run
dbt test
```

## Testing

The project separates testing concerns so pipeline and transformation validation can evolve independently.

Current test structure includes:

- `tests/unit/`
- `tests/integration/`
- `tests/pipeline/`
- `tests/data_quality/`
- `tests/dbt/`

This layout supports both code-level testing and warehouse/data validation as the platform matures.

## Why This Repo Structure Works

This repository is organized to resemble how a real analytics engineering team would manage an internal data platform:

- ingestion code is separate from warehouse logic
- dbt transformations are isolated in their own project
- orchestration is independent from transformation logic
- BI assets are separated from data pipelines
- configuration, documentation, tests, and logs each have a dedicated home

That separation makes the project easier to maintain, extend, and deploy.

## Roadmap

Near-term improvements that fit naturally into the current structure:

- implement ingestion scripts against the simplified `.env` config
- land raw extracts in GCS using a raw, staging, and processed prefix strategy
- load season-aware tables into BigQuery
- build out dbt staging, intermediate, and mart SQL
- add schema, relationship, uniqueness, and freshness tests
- wire the pipeline together with Prefect deployments
- publish curated marts to Power BI

## License

This project is licensed under the MIT License. See [LICENSE](/Users/bapbap23/Desktop/Nba-Data-and Analytics Engineering/LICENSE) for details.
