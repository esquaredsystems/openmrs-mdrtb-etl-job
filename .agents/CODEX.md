# CODEX.md

## Project Context
This project is an OpenMRS MDR-TB ETL job for an OpenMRS upgrade to v2.8.
It extracts data from a source database, stages it in target-side underscore-prefixed tables, applies targeted transformations needed for the upgraded OpenMRS schema, and loads selected records into the target environment.
The code is operational migration tooling, not a general-purpose application. 
Preserve data semantics, source IDs, UUIDs, timestamps, retired/voided flags, and table ordering unless the migration requirement explicitly says otherwise.

## Tech Stack
- Python 3.12, pinned in `Pipfile`.
- MySQL as both source and target database.
- SQLAlchemy 2.x with PyMySQL for database access.
- pandas and openpyxl for Excel-backed migration inputs in `resources/`.
- python-dotenv for `.env` configuration.
- loguru for logging to `etl.log`.
- pytest for tests.
- Docker support via `Dockerfile`.

Important paths:
- `main.py`: CLI entry point and extract/transform/load orchestration.
- `etl/`: table-group extract, transform, and load logic.
- `models/schema_models.py`: staging table DDL helpers for underscore-prefixed tables.
- `config/database.py`: source and target SQLAlchemy engine creation.
- `config/config.py`: shared runtime config, including `BATCH_SIZE`.
- `utils/helpers.py`: Excel/resource loaders.
- `resources/`: migration input spreadsheets and SQL assets.
- `tests/`: pytest coverage, currently focused on helper/resource loading behavior.

## Configuration
Runtime database settings come from `.env`:
Do not commit real credentials or patient data.

## Running
Install dependencies with either:

```bash
pipenv install --dev
```

or:

```bash
pip install -r requirements.txt
pip install pytest
```

Run tests:

```bash
pytest
```

Run ETL jobs:

```bash
python main.py --extract
python main.py --extract --hard-reset
python main.py --transform
python main.py --load
```

## Conventions That Matter
- Keep ETL modules organized by OpenMRS domain/table group: `patient`, `encounter`, `obs`, `concept`, `drug`, `orders`, `program`, etc.
- Follow the existing function naming pattern:
  - `extract_<table_or_group>()`
  - `transform_<table_or_group>()`
  - `load_<table_or_group>()`
  - `extract_<domain>_group()`
  - `load_<domain>_group()`
- Staging tables are underscore-prefixed, for example `_concept`, `_patient`.
- Use SQLAlchemy `text()` with bound parameters for SQL that includes values.
- Commit database writes explicitly after inserts.
- For large OpenMRS tables, use batching with `BATCH_SIZE` and `yield_per`, matching the existing `obs`, `patient`, `encounter`, `orders`, `drug`, and `report` patterns.
- Use `INSERT IGNORE` where the existing ETL uses it, because reruns and resume flows rely on idempotent inserts.
- Keep Excel-backed reference data loading centralized in `utils/helpers.py`.
- Preserve existing resource sheet names and column names exactly unless the spreadsheet migration contract changes.
- Use `utils.logger` instead of bare `print()` for ETL status messages.
- Add focused pytest coverage for helper behavior, transformations, and any logic that can be tested without live OpenMRS databases.

## What Not To Touch
- `.env` values, credentials, or environment-specific connection settings.
- Files in `resources/`.
- Do not change source data unless explicitly mentioned.
- `models/schema_models.py` broadly. There can be minimal changes only.
- Do not rename underscore-prefixed staging tables casually.
- Do not remove `INSERT IGNORE`, batching, or resume behavior without replacing it with an explicit rerun-safe strategy.
- Commented load groups in `main.py`.
- Do not resurrect or delete legacy migration files unless the task explicitly asks for cleanup.
- Destructive database operations such as `--hard-reset` are forbidden for you.

## Agent Workflow
Before changing ETL behavior:
1. Identify the source table, staging table, and target OpenMRS v2.8 table affected.
2. Check whether the table is populated from source DB, resource spreadsheets, generated values, or a combination.
3. Confirm ordering dependencies in `main.py` and the relevant `*_group()` functions.
4. Prefer narrow, table-specific changes over broad refactors.
5. Run `pytest` for local checks. If database behavior changed, document the manual DB validation needed.
