# openmrs-mdrtb-etl-job — Module Knowledge Graph
# Python 3.12 ETL — migrates source DB → OpenMRS DB (upgrade to v2.8).
# Conventions, do-not-touch rules, and agent workflow: ./AGENTS.md (read it before changing ETL behavior)
# Shared facts (domain model, concept UUIDs, topology): ../.agents/graph.md
# Usage + notation legend + maintenance rules: ../.agents/instructions.md
# Last updated: 2026-10-27 (concept upsert from CSV; names matched by concept+locale+type; 864 removed)

## § MODULE LAYOUT
```
main.py                     # CLI orchestrator; --extract / --load / --hard-reset / --check-integrity
etl/                        # 16 entity-group modules (patient.py 35KB, lab.py 48KB, concept.py 37KB…)
etl/fk_check.py             # READ-ONLY FK violation detector (not an extract/load group)
models/schema_models.py     # SQLAlchemy models / staging DDL for 46 OpenMRS tables (prefixed _)
config/database.py          # SQLAlchemy engine setup (source + target, PyMySQL)
config/config.py            # shared runtime config, incl. BATCH_SIZE
utils/helpers.py            # CSV/resource loaders (pandas)
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

## § CONCEPT_ANSWER LOAD (etl/concept.py load_concept_answer)
```
resources/concept_concept_answer.csv is the source of truth for the concepts it lists.
Row identity = concept_answer_id. One transaction: delete rows of listed concepts whose id is not in the sheet,
  then upsert; ON DUPLICATE KEY UPDATE sets concept_id, answer_concept, sort_weight (never uuid/creator/date_created).
concept_id is in the update set since 2026-10-27: without it, target rows 945-952 kept concept 338
  (POPULATION CATEGORY) while taking XPERT ULTRA/XDR RESULT answers -> wrong options on Form 89.
```

## § CONCEPT LOAD (etl/concept.py load_concept, load_concept_name) — CSV files are the source of truth
```
load_concept: INSERT ... ON DUPLICATE KEY UPDATE every column concept_concept.csv has, uuid included
  (not short_name/description/form_text/version/date_retired/retire_reason: not in the sheet, left as they are).
  Stops BEFORE writing if a sheet uuid already belongs to another concept_id in the target (would merge 2 concepts).
  Concepts removed from the sheet are NOT deleted from the target (obs may use them).
  datatype_id goes through to_target_datatype_id() (source has no id 9: 10+ shift down by one), same as
  the concept_datatype load; without it 13 Complex hit an FK error and 10 Boolean landed on Rule.
load_concept_name: matched by (concept_id, locale, concept_name_type) + order inside that group, NOT by row number,
  so removing/inserting CSV rows never moves names onto another concept. Changed name -> UPDATE, new -> INSERT
  (AUTO_INCREMENT id). Target names the sheet lacks (for concepts it lists) are only reported in etl.log.
  Synonyms (concept_name_type NULL) are never touched. 7 concepts have 2 en SHORT names (16,17,42,76,190,322,547).
2026-10-27: 194 PATIENT PREGNANT retired (dup of 175; answer row 173 -> 175; post_etl_job sets date/reason,
  full runs move obs 194 -> 175). 864 INDETERMINATE (I) removed from all concept CSVs (dup of 143); answers
  960-975 (questions 848-853) now 143. 143 added as answer to 558-569 (rows 1006-1017).
```
