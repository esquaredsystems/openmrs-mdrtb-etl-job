# OpenMRS MDR-TB ETL Job

This project is an ETL (Extract, Transform, Load) pipeline designed to migrate and transform MDR-TB (Multidrug-resistant tuberculosis) data for OpenMRS. It extracts data from a source OpenMRS database, applies necessary transformations (specifically for encounters, concepts, drugs, and lab results), and loads it into a target database.

## Prerequisites

- Python 3.12+
- MySQL/MariaDB (Source and Target databases)
- [Optional] Docker

## Configuration

The application uses environment variables for configuration. You can create a `.env` file in the root directory based on the following required variables:

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
2. Install dependencies using pip:
   ```bash
   pip install -r requirements.txt
   ```
   Or using Pipenv:
   ```bash
   pipenv install
   ```

### Docker Setup

You can build the Docker image using:
```bash
docker build -t openmrs-mdrtb-etl-job .
```

## Usage

The ETL job is controlled via command-line arguments in `main.py`.

### Running the Full Pipeline

To run the complete extraction, transformation, and load process:
```bash
python main.py --extract --transform --load
```

### Individual Steps

You can run individual parts of the pipeline:

- **Extraction only**: `python main.py --extract`
- **Transformation only**: `python main.py --transform`
- **Loading only**: `python main.py --load`

### Hard Reset

If you need to drop and recreate the tables in the target database before extraction, use the `--hard-reset` flag:
```bash
python main.py --extract --hard-reset
```

### Running with Docker

```bash
docker run --env-file .env openmrs-mdrtb-etl-job
```

## Project Structure

- `etl/`: Contains the logic for extracting, transforming, and loading various data entities (concepts, patients, encounters, etc.).
- `models/`: Database schema definitions and SQL scripts.
- `config/`: Database connection and application configuration.
- `utils/`: Helper functions and logging utilities.
- `resources/`: Static data and mappings (Excel files).
- `main.py`: Entry point for the ETL job.

## Data Entities Covered

The pipeline handles various OpenMRS entities, including:
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
