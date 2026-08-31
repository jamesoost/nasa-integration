# NASA Integration

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/Engine-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![Lint](https://img.shields.io/badge/Lint-Ruff-D7FF64?style=for-the-badge&logo=ruff&logoColor=111111)](https://docs.astral.sh/ruff/)
[![Tests](https://img.shields.io/badge/Tests-Pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![CI](https://github.com/jamesoost/nasa-integration/actions/workflows/ci.yml/badge.svg)](https://github.com/jamesoost/nasa-integration/actions/workflows/ci.yml)

## Purpose

This repository is a PySpark ETL learning project built around the NASA InSight Mars weather API. It's purpose is to learn and demonstrate how to design a small pipeline with clear extract, transform, and load stages, data quality checks, and repeatable outputs in PySpark.

## Quickstart

Requirements: Python 3.11, Java runtime (required by Spark).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create `.env` in the project root:

```bash
echo "API-Key=DEMO_KEY" > .env
```

Run full pipeline:

```bash
python -m nasa_pipeline.pipeline --stage all
```

## Flow

1. extract: fetch API payload and write timestamped raw JSON to `data/raw/`.
2. transform: flatten/clean to staged CSV in `data/staging/`, validate records, and write rejects to `data/quarantine/`.
3. load: upsert valid records into SQLite table `processed_weather` in `data/processed/weather.db`.

## Stage Execution

Run stages independently:

```bash
python -m nasa_pipeline.pipeline --stage extract
python -m nasa_pipeline.pipeline --stage transform
python -m nasa_pipeline.pipeline --stage load
```

Standalone behavior:

1. transform reads the latest raw snapshot from `data/raw/`.
2. load reads current `data/staging/`, revalidates it, then upserts.

## Validation and Quarantine

Records are quarantined when any of the following fields is missing:

1. `sol` id
2. AT average temperature
3. HWS average wind speed
4. PRE average pressure

The pipeline logs data quality metrics, including total records, processed records, quarantined records, acceptance rate, and reject-reason breakdown.

## Processed Output (SQLite)

Database: `data/processed/weather.db`

Table: `processed_weather`

Upsert key: `sol` (existing `sol` rows are updated, new `sol` rows are inserted).

Example queries:

```bash
sqlite3 data/processed/weather.db "SELECT COUNT(*) AS total_rows FROM processed_weather;"
sqlite3 data/processed/weather.db "SELECT sol, season, at_average_temp FROM processed_weather ORDER BY sol DESC LIMIT 5;"
sqlite3 data/processed/weather.db "SELECT AVG(pre_average_pressure) AS avg_pressure FROM processed_weather;"
```

## Sample Run Output

```text
INFO nasa_pipeline.extract: Saved raw payload to data/raw/insight_weather_20260831T191323Z.json
INFO nasa_pipeline.transform: Staged 7 rows to data/staging
INFO nasa_pipeline.transform: Validation summary total=7 processed=7 quarantined=0 acceptance_rate=1.00
INFO nasa_pipeline.transform: Wrote 0 quarantined rows to data/quarantine
INFO nasa_pipeline.load: Upserted 7 processed rows into data/processed/weather.db (table=processed_weather)
```

## Next Improvements

Some things which I would like to improve with time:

1. Add retry/backoff handling for NASA API failures and transient network timeouts.
2. Track data lineage between raw file, staging snapshot, and loaded SQLite upserts.
3. Add schema evolution handling for API field changes and backward compatibility.
4. Partition or timestamp staging/quarantine outputs for historical snapshots.
5. Add containerized execution and scheduled runs for repeatable deployment.

## Code Checks

Run tests:

```bash
pytest
```
Runs tests against transform and load behavior, including validation and upsert logic.

Run lint:

```bash
ruff check .
```
Runs lint checks against the Python codebase. The same check runs in GitHub Actions CI.

CI workflow: `.github/workflows/ci.yml`

