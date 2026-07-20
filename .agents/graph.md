# openmrs-mdrtb-etl-job — Module Knowledge Graph
# Python 3.12 ETL — migrates source DB → OpenMRS DB (upgrade to v2.8).
# Conventions, do-not-touch rules, and agent workflow: ./AGENTS.md (read it before changing ETL behavior)
# Shared facts (domain model, concept UUIDs, topology): ../.agents/graph.md
# Usage + notation legend + maintenance rules: ../.agents/instructions.md
# Last updated: 2026-07-02

## § MODULE LAYOUT
```
main.py                     # CLI orchestrator; --extract / --transform / --load / --hard-reset flags
etl/                        # 16 entity-group modules (patient.py 35KB, lab.py 48KB, concept.py 37KB…)
models/schema_models.py     # SQLAlchemy models / staging DDL for 46 OpenMRS tables (prefixed _)
config/database.py          # SQLAlchemy engine setup (source + target, PyMySQL)
config/config.py            # shared runtime config, incl. BATCH_SIZE
utils/helpers.py            # Excel/resource loaders (pandas + openpyxl)
utils/ (logger)             # loguru → etl.log; use instead of print()
resources/                  # migration input spreadsheets + SQL assets (do not modify)
tests/                      # pytest — test_helpers.py (unit, no DB) | test_migration.py (integration, both DBs)
.env                        # DB connection settings (python-dotenv; never commit real credentials)
```

## § ETL PIPELINE
```
Entry: main.py
CLI:   python main.py --extract --load    # full pipeline
       python main.py --extract --hard-reset   # drop + recreate staging tables (FORBIDDEN for agents)
       python main.py --transform         # targeted transformations for OpenMRS v2.8 schema
       python main.py --load              # load only

pre_etl:  disable FK checks, SET NAMES utf8mb4
post_etl: re-enable FK checks, fix provider privileges

Extract order (source DB → staging, prefixed _ tables):
  1 address_hierarchy  2 cohort  3 concept  4 drug  5 form  6 hl7
  7 location  8 orders  9 program  10 user  11 report  12 misc
  13 patient  14 encounter  15 lab                    # obs commented out

Load order (staging → target OpenMRS DB):
  1 user  2 address_hierarchy  3 cohort  4 concept  5 location  6 drug
  7 form  8 hl7  9 program  10 report  11 misc  12 patient  13 encounter
  14 obs  15 orders  16 lab
```

## § KEY PATTERNS (details + rules in AGENTS.md)
```
Function naming:   extract_<group>() / transform_<group>() / load_<group>() / <op>_<domain>_group()
Staging tables:    underscore-prefixed (_concept, _patient, …)
Idempotency:       INSERT IGNORE + explicit commits; reruns/resume depend on it
Large tables:      batching with BATCH_SIZE + yield_per (obs, patient, encounter, orders, drug, report)
SQL with values:   SQLAlchemy text() with bound parameters
```
