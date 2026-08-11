"""
Foreign-key violation detector for the TARGET OpenMRS database.
The ETL runs with `SET FOREIGN_KEY_CHECKS = 0` (see `main.py: pre_etl_job`), so rows can land in the target database
whose foreign-key values point at parent rows that do not exist. MySQL will not complain until something later tries to
enforce the constraint. This module finds every such orphan row.
1. Reads every DECLARED FK of target schema from `information_schema`.
2. For each constraint, scans the whole child table with an anti-join and records EVERY offending row.
3. Writes one CSV per violating constraint plus a `summary.csv`.

Read-only: SELECT statements only. No INSERT/UPDATE/DELETE/DDL, no commits.
"""

import csv
import os
import re
import time

from datetime import date, datetime, timedelta
from decimal import Decimal
from sqlalchemy import text
from config.config import BATCH_SIZE
from config.database import get_target_engine, _get_required_env
from utils.logger import info, warning, error

# Only these characters may appear in an identifier we interpolate into SQL.
# Identifiers come from information_schema (already trusted), but this is a
# healthcare migration - validate anyway rather than assume.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_$]+$")

REPORT_DIR = "fk_report"
SUMMARY_FILENAME = "summary.csv"

# Extra child-table columns copied into the CSV when present, so the team can
# triage a violation without going back to the database.
CONTEXT_COLUMNS = ("uuid", "voided", "retired", "date_created")

# Log a progress line every this many violating rows written.
PROGRESS_EVERY = 50000


def quote_identifier(identifier):
    """Backtick-quote a MySQL identifier after validating its characters."""
    if not isinstance(identifier, str) or not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Refusing to build SQL with unsafe identifier: {identifier!r}")
    return f"`{identifier}`"


def format_value(value):
    """Render a database value as a plain string for CSV output."""
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        # MySQL bit(1) columns (voided/retired) arrive as b'\x00' / b'\x01'.
        if len(value) <= 8:
            return str(int.from_bytes(value, "big"))
        return value.hex()
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (timedelta, Decimal)):
        return str(value)
    return str(value)


def safe_filename(*parts):
    """Build a filesystem-safe CSV name from table/constraint names."""
    cleaned = [re.sub(r"[^A-Za-z0-9_.-]", "_", str(part)) for part in parts]
    return "__".join(cleaned) + ".csv"


def describe_foreign_key(fk):
    """One-line description used in the log, e.g. 'obs(concept_id) -> concept(concept_id)'."""
    child = ", ".join(fk["child_columns"])
    parent = ", ".join(fk["parent_columns"])
    return f"{fk['child_table']}({child}) -> {fk['parent_table']}({parent})"


def fetch_foreign_keys(conn, database):
    """
    Return every declared foreign key in `database` as a list of dicts, ordered by child table.

    Multi-column constraints are collapsed into a single entry with the columns
    kept in ORDINAL_POSITION order so child/parent columns stay paired.
    """
    query = text(
        """
        SELECT kcu.CONSTRAINT_NAME     AS constraint_name,
               kcu.TABLE_NAME          AS child_table,
               kcu.COLUMN_NAME         AS child_column,
               kcu.REFERENCED_TABLE_NAME  AS parent_table,
               kcu.REFERENCED_COLUMN_NAME AS parent_column
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
          ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
         AND rc.CONSTRAINT_NAME   = kcu.CONSTRAINT_NAME
         AND rc.TABLE_NAME        = kcu.TABLE_NAME
        WHERE kcu.TABLE_SCHEMA = :database
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        """
    )
    foreign_keys = []
    by_key = {}
    for row in conn.execute(query, {"database": database}):
        key = (row.child_table, row.constraint_name)
        if key not in by_key:
            by_key[key] = {
                "constraint_name": row.constraint_name,
                "child_table": row.child_table,
                "child_columns": [],
                "parent_table": row.parent_table,
                "parent_columns": [],
            }
            foreign_keys.append(by_key[key])
        by_key[key]["child_columns"].append(row.child_column)
        by_key[key]["parent_columns"].append(row.parent_column)

    return foreign_keys


def fetch_table_columns(conn, database, table):
    rows = conn.execute(
        text(
            """
            SELECT COLUMN_NAME AS column_name
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table
            ORDER BY ORDINAL_POSITION
            """
        ),
        {"database": database, "table": table},
    )
    return [row.column_name for row in rows]


def fetch_primary_key_columns(conn, database, table):
    rows = conn.execute(
        text(
            """
            SELECT COLUMN_NAME AS column_name
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = :database
              AND TABLE_NAME = :table
              AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
            """
        ),
        {"database": database, "table": table},
    )
    return [row.column_name for row in rows]


def build_violation_sql(fk, select_columns):
    """
    Anti-join SQL selecting every child row whose foreign key has no parent row.

    NULL key columns are excluded: MySQL treats a foreign key with any NULL
    column as satisfied, so those rows are not violations.
    """
    child = quote_identifier(fk["child_table"])
    parent = quote_identifier(fk["parent_table"])
    projection = ", ".join(f"c.{quote_identifier(col)}" for col in select_columns)

    join_conditions = " AND ".join(
        f"p.{quote_identifier(parent_col)} = c.{quote_identifier(child_col)}"
        for child_col, parent_col in zip(fk["child_columns"], fk["parent_columns"])
    )

    conditions = [f"c.{quote_identifier(col)} IS NOT NULL" for col in fk["child_columns"]]
    # A NULL on the parent side of a LEFT JOIN means no parent row matched.
    conditions.append(f"p.{quote_identifier(fk['parent_columns'][0])} IS NULL")

    return (
        f"SELECT {projection} FROM {child} c "
        f"LEFT JOIN {parent} p ON {join_conditions} "
        f"WHERE {' AND '.join(conditions)}"
    )


def select_report_columns(fk, child_columns, pk_columns):
    """Columns copied into the CSV: primary key, then the FK columns, then context."""
    columns = []
    for col in list(pk_columns) + list(fk["child_columns"]):
        if col not in columns:
            columns.append(col)
    for col in CONTEXT_COLUMNS:
        if col in child_columns and col not in columns:
            columns.append(col)
    return columns


def check_foreign_key(engine, database, fk):
    """
    Scan one constraint and write every violating row to its own CSV.

    The result set is streamed in chunks of `BATCH_SIZE`, so a table with
    millions of orphan rows is never loaded into memory at once.

    Returns a dict with the violation count, duration and CSV name.
    """
    started = time.time()
    csv_path = os.path.join(REPORT_DIR, safe_filename(fk["child_table"], fk["constraint_name"]))
    violations = 0

    with engine.connect() as conn:
        child_columns = fetch_table_columns(conn, database, fk["child_table"])
        pk_columns = fetch_primary_key_columns(conn, database, fk["child_table"])
        report_columns = select_report_columns(fk, child_columns, pk_columns)

        query = text(build_violation_sql(fk, report_columns)).execution_options(
            stream_results=True, yield_per=BATCH_SIZE
        )
        # utf-8-sig so Excel opens Cyrillic text correctly for the team.
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(report_columns)
            for row in conn.execute(query):
                writer.writerow([format_value(value) for value in row])
                violations += 1
                if violations % PROGRESS_EVERY == 0:
                    info(f"  {fk['child_table']}.{fk['constraint_name']}: {violations} violation(s) so far...")

    if violations == 0:
        os.remove(csv_path)

    return {
        "violations": violations,
        "seconds": round(time.time() - started, 2),
        "csv": os.path.basename(csv_path) if violations else "",
        "status": "done",
    }


def write_summary(results):
    """Write one summary row per constraint. `results` is a list of (fk, result) pairs."""
    path = os.path.join(REPORT_DIR, SUMMARY_FILENAME)
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "child_table", "constraint_name", "child_columns", "parent_table", "parent_columns",
            "violating_rows", "status", "seconds", "csv_file",
        ])
        for fk, result in results:
            if result.get("violations", 0) > 0:
                writer.writerow([
                    fk["child_table"], fk["constraint_name"], ",".join(fk["child_columns"]),
                    fk["parent_table"], ",".join(fk["parent_columns"]),
                    result.get("violations", 0), result.get("status", ""),
                    result.get("seconds", ""), result.get("csv", ""),
                ])
    return path


def run_fk_check():
    """
    Detect every foreign-key violation in the target database.

    Returns:
        (total_violations, summary_csv_path)
    """
    start_time = time.time()
    database = _get_required_env("TARGET_DB_NAME")
    engine = get_target_engine()
    os.makedirs(REPORT_DIR, exist_ok=True)

    with engine.connect() as conn:
        foreign_keys = fetch_foreign_keys(conn, database)

    if not foreign_keys:
        warning(f"No declared foreign keys found in database '{database}' - nothing to check")
        return 0, write_summary([])

    info(
        f"Foreign key check starting: {len(foreign_keys)} constraint(s) in '{database}', "
        f"report directory '{os.path.abspath(REPORT_DIR)}'"
    )

    results = []
    total_violations = 0
    failed = []
    for index, fk in enumerate(foreign_keys, start=1):
        info(f"[{index}/{len(foreign_keys)}] Checking {describe_foreign_key(fk)}")
        try:
            result = check_foreign_key(engine, database, fk)
        except Exception as exc:  # keep scanning the remaining constraints
            result = {"violations": 0, "status": "error", "seconds": "", "csv": ""}
            failed.append(f"{fk['child_table']}.{fk['constraint_name']}")
            error(f"  {fk['child_table']}.{fk['constraint_name']}: check failed - {exc}")
        else:
            total_violations += result["violations"]
            if result["violations"]:
                warning(f"  {result['violations']} violating row(s) -> {result['csv']}")
            else:
                info(f"  clean ({result['seconds']}s)")
        results.append((fk, result))

    summary_path = write_summary(results)
    info(
        f"Foreign key check completed: {total_violations} violating row(s) across "
        f"{len(foreign_keys)} constraint(s) (Total Time: {time.time() - start_time:.2f} seconds)"
    )
    info(f"Summary written to {os.path.abspath(summary_path)}")
    if failed:
        error(f"{len(failed)} constraint(s) could not be checked: {', '.join(failed)}")
    return total_violations, summary_path
