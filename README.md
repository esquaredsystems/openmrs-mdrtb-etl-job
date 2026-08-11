# OpenMRS MDR-TB ETL Job

This project is an ETL (Extract, Load) pipeline designed to migrate MDR-TB (Multidrug-resistant tuberculosis) data for OpenMRS. It extracts data from a source OpenMRS database and loads it into a target database. Any data reshaping (e.g. deriving provider records, populating encounter_provider, computing drug strength units, and updating lab attribute type descriptions) is embedded directly within the extract and load steps rather than a separate transform phase.

## Prerequisites

- Python 3.12+
- MySQL/MariaDB (source and target databases)
- [Optional] Docker

## Configuration

The application uses environment variables for configuration. Create a `.env` file in the project root based on the variables below.

### Database Configuration

| Variable | Description |
| --- | --- |
| `SOURCE_DB_HOST` | Hostname of the source database |
| `SOURCE_DB_PORT` | Port of the source database (default: 3306) |
| `SOURCE_DB_USER` | Username for the source database |
| `SOURCE_DB_PASS` | Password for the source database |
| `SOURCE_DB_NAME` | Name of the source database |
| `TARGET_DB_HOST` | Hostname of the target database |
| `TARGET_DB_PORT` | Port of the target database (default: 3306) |
| `TARGET_DB_USER` | Username for the target database |
| `TARGET_DB_PASS` | Password for the target database |
| `TARGET_DB_NAME` | Name of the target database |

### Other Configuration

| Variable | Description | Default |
| --- | --- | --- |
| `BATCH_SIZE` | Number of records to process in a single batch | 10000 |

## Installation

### Local Setup

1. Clone the repository.
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate       # Linux/macOS
   venv\Scripts\activate          # Windows
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Docker Setup

Build the Docker image:
```bash
docker build -t openmrs-mdrtb-etl-job .
```

## Usage

The ETL job is controlled via command-line arguments in `main.py`. Run all commands from inside the `openmrs-mdrtb-etl-job/` directory.

### Running the Full Pipeline

```bash
python main.py --extract --load
```

### Individual Steps

| Step | Command |
| --- | --- |
| Extraction only | `python main.py --extract` |
| Loading only | `python main.py --load` |

### Hard Reset

Drop and recreate all tables in the target database before extraction:
```bash
python main.py --extract --hard-reset
```

### Running with Docker

```bash
docker run --env-file .env openmrs-mdrtb-etl-job
```

## Foreign Key Violation Check

The ETL loads data with `FOREIGN_KEY_CHECKS = 0`, so rows can end up in the target
database whose foreign key values point at parent rows that do not exist. The
`--check-integrity` flag scans the target database and reports **every** such row.

```bash
python main.py --check-integrity
```

This is **read-only** — it issues `SELECT` statements only and never writes,
alters, or deletes anything. When run on its own it also skips `pre_etl_job()`
and `post_etl_job()`, so nothing at all is written to the database.

### How it works

1. Reads every declared foreign key from `information_schema` (no hand-maintained
   list, so it stays correct as the schema changes).
2. Scans each child table with one anti-join query, recording every offending
   row. There is no sampling and no row limit. Results are streamed in chunks of
   `BATCH_SIZE`, so a table with millions of orphan rows never has to fit in
   memory.
3. A row is only a violation if all of its foreign key columns are non-NULL and
   no matching parent row exists — this matches MySQL's own rule, which treats a
   foreign key containing a NULL as satisfied.

There is no resume state: if the check is interrupted, just run it again.

Run `--check-integrity` on its own to check only, or alongside `--extract` /
`--load` to check the database right after the ETL finishes.

### Output

Written to `fk_report/` (git-ignored, since it contains patient data):

| File | Contents |
| --- | --- |
| `summary.csv` | One row per constraint: child/parent tables, columns, violating row count, status, duration |
| `<table>__<constraint>.csv` | Every violating row — primary key, the offending foreign key values, plus `uuid`, `voided`, `retired`, `date_created` where the table has them. Only created when violations exist |

CSVs are written as UTF-8 with a BOM so Excel displays Cyrillic text correctly.

The scan takes as long as it takes on large tables (`obs`, `encounter`);
progress is logged to `etl.log`. If one constraint fails (for example a dropped
connection), the run logs the error, carries on with the remaining constraints,
and lists the failed ones at the end.

Examples:

```bash
# Check the target database
python main.py --check-integrity

# Run the load, then check what it produced
python main.py --load --check-integrity
```

## Testing

Tests are located in the `tests/` directory. There are three suites:

| Suite | File | Requires DB? | Description |
| --- | --- | --- | --- |
| Helper tests | `tests/test_helpers.py` | No | Unit tests for utility functions and Excel resource loading |
| FK check tests | `tests/test_fk_check.py` | No | Unit tests for the foreign key checker's SQL construction, identifier safety, CSV output, and resume state |
| Migration tests | `tests/test_migration.py` | Yes (both DBs) | Validates row counts between source and target after migration |

### Setup

Install test dependencies (if not already installed via `requirements.txt`):
```bash
pip install pytest
```

### Running All Tests

From inside the `openmrs-mdrtb-etl-job/` directory:
```bash
pytest
```

### Running Only Unit Tests (no DB required)

```bash
pytest tests/test_helpers.py
```

### Running Only Migration Validation Tests

These tests connect to both the source and target databases using the same `.env` configuration as the main job. Make sure both databases are reachable before running.

```bash
pytest tests/test_migration.py
```

### What the Migration Tests Check

- **`test_person_count`** — total row count in `person` matches within 0.1% tolerance
- **`test_patient_count`** — total row count in `patient` matches within 0.1% tolerance
- **`test_encounters_by_month_year`** — per-patient encounter counts match across both databases, grouped by month/year
- **`test_obs_by_encounter_by_month_year`** — per-encounter observation counts match across both databases

The tolerance threshold (0.1%) accounts for minor expected divergences (e.g., voided records). A test fails only if a count diverges beyond that threshold.

## Project Structure

```
openmrs-mdrtb-etl-job/
├── config/         # Database connection and app configuration
├── etl/            # Extract and load logic per entity
│   └── fk_check.py # Read-only foreign key violation detector (--check-integrity)
├── models/         # Database schema definitions
├── resources/      # Static data and mappings (Excel files)
├── tests/          # Test suite
├── utils/          # Helpers and logging utilities
├── main.py         # Entry point
└── pytest.ini      # Pytest configuration (sets pythonpath for imports)
```

## Data Entities Covered

The pipeline handles the following OpenMRS entities:

- Address Hierarchy
- Cohorts
- Concepts and Drugs
- Encounters and Observations
- Forms and HL7
- Locations
- Patients and Programs
- Orders and Lab Results
- Users
- Reports
