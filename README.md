# NBA Data and Analytics Engineering

This repository is an NBA-focused data engineering workspace for collecting multi-season player game logs, enriching them with player metadata, and organizing the project into the layers you would expect in a modern analytics platform.

The project has been refocused away from the earlier Streamlit and model-demo setup. Today, the core of the repository is the data pipeline foundation: ingestion scripts, storage-oriented folder structure, warehouse and transformation scaffolding, and room to grow into orchestration, quality checks, monitoring, and BI.

## What This Project Does

- Pulls regular-season NBA player game logs from the NBA Stats API
- Stores season-level CSV exports in `data/`
- Enriches player logs with biographical attributes such as height, weight, team, and listed position
- Organizes the repo into analytics engineering domains such as ingestion, transformations, warehouse, orchestration, monitoring, and quality
- Provides a foundation for future dbt, BigQuery, Prefect, Power BI, and observability work

## Current State

This repo is best understood as a platform foundation rather than a finished analytics product.

- The primary working flow today is Python-based data extraction and enrichment
- The top-level `app.py` is a placeholder and is not the main interface
- Many subfolders already exist to support a more complete analytics stack, but several are still scaffolds with placeholder READMEs
- Sample season exports are already checked into `data/` for recent NBA seasons

## Repository Layout

```text
.
├── app.py                    # Placeholder entrypoint; points users toward data ingestion
├── data/                     # Season CSV exports and derived flat files
├── scripts/                  # Data collection and merge utilities
├── ingestion/                # Ingestion-layer docs and future pipeline code
├── data_lake/                # Raw/landing-zone design docs and assets
├── transformations/          # dbt and transformation layer structure
├── warehouse/                # Warehouse-oriented structure and docs
├── analytics/                # BI/reporting assets, including Power BI
├── orchestration/            # Prefect-oriented orchestration scaffolding
├── quality/                  # Data quality patterns and tests
├── monitoring/               # Monitoring and observability structure
├── config/                   # Environment-specific config areas
├── credentials/              # Credential guidance and placeholders
├── docs/                     # Project documentation and standards
├── shared/                   # Shared assets and reusable helpers
├── tests/                    # Platform and pipeline test area
└── requirements.txt          # Python dependencies
```

## Key Scripts

### `scripts/fetch_ten_seasons.py`

The main ingestion script. It:

- Determines the latest completed NBA season
- Builds a rolling window of completed seasons
- Calls `leaguegamefinder` from `stats.nba.com`
- Splits requests into smaller date windows to reduce failure risk
- Retries failed requests with backoff
- Writes one CSV per season to `data/`
- Optionally supports a combined export

Example:

```bash
python scripts/fetch_ten_seasons.py
```

Fetch fewer seasons:

```bash
python scripts/fetch_ten_seasons.py --seasons 3
```

Write to a different directory:

```bash
python scripts/fetch_ten_seasons.py --output-dir tmp/nba_exports
```

### `scripts/fetch_player_bio.py`

Fetches active player metadata using `nba_api` and writes a lookup file to `data/nba_player_bio.csv`.

Example:

```bash
python scripts/fetch_player_bio.py
```

### `scripts/merge_bio_into_logs.py`

Joins a season log file with player bio data and standardizes fields such as `HEIGHT_IN` and `WEIGHT_LB`.

Current defaults target the `2023-24` season files:

```bash
python scripts/merge_bio_into_logs.py
```

### `scripts/merge_positions.py`

Merges external position labels into player logs using player names. This is useful if you are supplementing NBA API data with a separate curated position file.

## Getting Started

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Pull raw season data

```bash
python scripts/fetch_ten_seasons.py
```

### 4. Fetch player metadata

```bash
python scripts/fetch_player_bio.py
```

### 5. Enrich a season export

```bash
python scripts/merge_bio_into_logs.py
```

## Data Assets

The `data/` directory already includes season exports such as:

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

These files give the project a ready-to-use historical base for downstream modeling, warehousing, and analytics work.

## Engineering Direction

The folder structure suggests a target architecture like this:

1. Ingest raw NBA data into a landing zone
2. Standardize and validate raw extracts
3. Load curated datasets into an analytical warehouse
4. Transform datasets into analytics-ready models with dbt
5. Orchestrate recurring runs with Prefect
6. Serve reporting outputs through BI tools such as Power BI
7. Add monitoring, logging, and data quality checks around the pipeline

## Where To Add Work Next

- Add configurable input and output paths to the merge scripts
- Move hard-coded filenames into environment-aware config
- Build warehouse load scripts for BigQuery
- Add dbt models for staging, intermediate, and marts layers
- Add data validation tests for schema, nulls, uniqueness, and freshness
- Add Prefect flows for scheduled ingestion and transformation runs
- Expand project documentation in `docs/architecture/` and `docs/runbooks/`

## Notes

- NBA Stats endpoints can rate-limit or temporarily block requests, so the ingestion script uses sleeps and retries
- The current enrichment script is tailored to one season by default and should be generalized if you want a repeatable multi-season job
- Several directories are intentionally scaffolded now so the platform can evolve without reorganizing the repo later

## License

This project is licensed under the MIT License. See [LICENSE](/Users/bapbap23/Desktop/Nba-Data-and Analytics Engineering/LICENSE) for details.
