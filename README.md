# OpenMRS MDR-TB ETL Job

This project is an ETL (Extract, Transform, Load) pipeline designed to migrate and transform MDR-TB (Multidrug-resistant tuberculosis) data for OpenMRS. It extracts data from a source OpenMRS database, applies necessary transformations (specifically for encounters, concepts, drugs, and lab results), and loads it into a target database.

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
python main.py --extract --transform --load
```

### Individual Steps

| Step | Command |
| --- | --- |
| Extraction only | `python main.py --extract` |
| Transformation only | `python main.py --transform` |
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

## Testing

Tests are located in the `tests/` directory. There are two suites:

| Suite | File | Requires DB? | Description |
| --- | --- | --- | --- |
| Helper tests | `tests/test_helpers.py` | No | Unit tests for utility functions and Excel resource loading |
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
├── etl/            # Extract, transform, and load logic per entity
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
