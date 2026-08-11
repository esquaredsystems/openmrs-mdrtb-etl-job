# openmrs-mdrtb-etl-job — Module Knowledge Graph
# Python 3.12 ETL — migrates source DB → OpenMRS DB (upgrade to v2.8).
# Conventions, do-not-touch rules, and agent workflow: ./AGENTS.md (read it before changing ETL behavior)
# Shared facts (domain model, concept UUIDs, topology): ../.agents/graph.md
# Usage + notation legend + maintenance rules: ../.agents/instructions.md
# Last updated: 2026-08-02

## § MODULE LAYOUT
```
main.py                     # CLI orchestrator; --extract / --load / --hard-reset / --check-integrity
etl/                        # 16 entity-group modules (patient.py 35KB, lab.py 48KB, concept.py 37KB…)
etl/fk_check.py             # READ-ONLY FK violation detector (not an extract/load group)
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
                   !! INSERT IGNORE only dedupes against a UNIQUE KEY. Where the
                      SELECT generates UUID() per row and `uuid` is the only unique
                      key (the attribute tables), IGNORE can NEVER fire and every
                      rerun appends duplicates. Such inserts need an explicit
                      NOT EXISTS guard — see etl/location.py
                      build_location_attribute_insert() for the pattern.
Large tables:      batching with BATCH_SIZE + yield_per (obs, patient, encounter, orders, drug, report)
SQL with values:   SQLAlchemy text() with bound parameters
```

## § FK VIOLATION CHECK (etl/fk_check.py)
```
Why:   load runs with FOREIGN_KEY_CHECKS=0 -> orphan rows land in target undetected
CLI:   python main.py --check-integrity          # the only flag; no sub-options
       alone -> skips source connect + pre_etl_job/post_etl_job, exits after scan
       with --extract/--load -> runs after post_etl_job()

Read-only: SELECT statements only. No writes, no DDL, no commits, no FK-check toggling.
Style:    plain functions + dicts only (no classes, no dataclasses, no resume state)

Flow:  information_schema.KEY_COLUMN_USAGE + REFERENTIAL_CONSTRAINTS -> list of fk dicts
       {constraint_name, child_table, child_columns[], parent_table, parent_columns[]}
       -> per constraint: ONE LEFT JOIN anti-join, streamed (stream_results + yield_per
          BATCH_SIZE) straight to CSV; no sampling, no row cap, nothing held in memory
Rule:  a row violates only if ALL its FK columns are non-NULL and no parent row matches
       (MySQL MATCH SIMPLE: any NULL in the key = constraint satisfied)
No resume: interrupted run is simply rerun from the start

Output (fk_report/, git-ignored - contains patient data):
       summary.csv                     one row per constraint + count/status/duration
       <table>__<constraint>.csv       every violating row; PK + FK cols + uuid/voided/
                                       retired/date_created when present; utf-8-sig (Excel/Cyrillic)
                                       written only when violations exist
Resilience: per-constraint connection + try/except -> one failure never aborts the run;
       failed constraints listed at end of etl.log
Tests: tests/test_fk_check.py (no DB required - SQL building, identifier safety, summary)
```
