"""
Unit tests for etl/fk_check.py.

These cover the pure logic only (SQL construction, identifier safety, value
formatting, column selection, summary output). No database connection required.
"""

import csv
from datetime import datetime

import pytest

from etl import fk_check
from etl.fk_check import (
    build_violation_sql,
    describe_foreign_key,
    format_value,
    quote_identifier,
    safe_filename,
    select_report_columns,
    write_summary,
)


def make_fk(**overrides):
    fk = {
        "constraint_name": "encounter_patient",
        "child_table": "encounter",
        "child_columns": ["patient_id"],
        "parent_table": "patient",
        "parent_columns": ["patient_id"],
    }
    fk.update(overrides)
    return fk


# --------------------------------------------------------------------------- #
# Identifier safety
# --------------------------------------------------------------------------- #
def test_quote_identifier_wraps_in_backticks():
    assert quote_identifier("patient_id") == "`patient_id`"


@pytest.mark.parametrize(
    "bad",
    ["patient_id`; DROP TABLE patient; --", "patient id", "", "pa'tient", None, 5],
)
def test_quote_identifier_rejects_unsafe_input(bad):
    with pytest.raises(ValueError):
        quote_identifier(bad)


# --------------------------------------------------------------------------- #
# SQL construction
# --------------------------------------------------------------------------- #
def test_violation_sql_is_an_anti_join_excluding_nulls():
    sql = build_violation_sql(make_fk(), ["encounter_id", "patient_id"])
    assert "LEFT JOIN `patient` p ON p.`patient_id` = c.`patient_id`" in sql
    # NULL foreign keys are not violations in MySQL and must be filtered out.
    assert "c.`patient_id` IS NOT NULL" in sql
    # No matching parent row is what makes a violation.
    assert "p.`patient_id` IS NULL" in sql


def test_violation_sql_selects_the_requested_columns():
    sql = build_violation_sql(make_fk(), ["encounter_id", "patient_id", "uuid"])
    assert sql.startswith("SELECT c.`encounter_id`, c.`patient_id`, c.`uuid` FROM `encounter` c")


def test_violation_sql_pairs_composite_key_columns_in_order():
    fk = make_fk(
        constraint_name="composite_fk",
        child_table="child_tbl",
        child_columns=["a_id", "b_id"],
        parent_table="parent_tbl",
        parent_columns=["x_id", "y_id"],
    )
    sql = build_violation_sql(fk, ["a_id", "b_id"])
    assert "p.`x_id` = c.`a_id`" in sql
    assert "p.`y_id` = c.`b_id`" in sql
    assert "c.`a_id` IS NOT NULL AND c.`b_id` IS NOT NULL" in sql


def test_violation_sql_handles_self_referencing_foreign_key():
    fk = make_fk(
        constraint_name="location_parent",
        child_table="location",
        child_columns=["parent_location"],
        parent_table="location",
        parent_columns=["location_id"],
    )
    sql = build_violation_sql(fk, ["location_id", "parent_location"])
    assert "FROM `location` c LEFT JOIN `location` p" in sql


def test_describe_foreign_key_is_readable():
    fk = make_fk(child_table="obs", child_columns=["concept_id"],
                 parent_table="concept", parent_columns=["concept_id"])
    assert describe_foreign_key(fk) == "obs(concept_id) -> concept(concept_id)"


# --------------------------------------------------------------------------- #
# Report column selection
# --------------------------------------------------------------------------- #
def test_report_columns_start_with_pk_then_fk_then_context():
    child_columns = ["obs_id", "person_id", "concept_id", "uuid", "voided", "date_created"]
    columns = select_report_columns(
        make_fk(child_table="obs", child_columns=["concept_id"]),
        child_columns,
        ["obs_id"],
    )
    assert columns == ["obs_id", "concept_id", "uuid", "voided", "date_created"]


def test_report_columns_do_not_duplicate_when_fk_is_the_pk():
    columns = select_report_columns(
        make_fk(child_table="patient", child_columns=["patient_id"]),
        ["patient_id", "uuid"],
        ["patient_id"],
    )
    assert columns == ["patient_id", "uuid"]


def test_report_columns_work_without_a_primary_key():
    columns = select_report_columns(
        make_fk(child_table="cohort_member", child_columns=["cohort_id"]),
        ["cohort_id", "patient_id"],
        [],
    )
    assert columns == ["cohort_id"]


# --------------------------------------------------------------------------- #
# Value formatting
# --------------------------------------------------------------------------- #
def test_format_value_renders_bit_columns_as_integers():
    assert format_value(b"\x00") == "0"
    assert format_value(b"\x01") == "1"


def test_format_value_renders_none_as_empty_string():
    assert format_value(None) == ""


def test_format_value_renders_datetime_readably():
    assert format_value(datetime(2026, 7, 31, 9, 5, 0)) == "2026-07-31 09:05:00"


def test_safe_filename_strips_unsafe_characters():
    assert safe_filename("obs", "fk/name:1") == "obs__fk_name_1.csv"


# --------------------------------------------------------------------------- #
# Summary output
# --------------------------------------------------------------------------- #
def test_write_summary_lists_every_constraint(tmp_path, monkeypatch):
    monkeypatch.setattr(fk_check, "REPORT_DIR", str(tmp_path))
    results = [
        (make_fk(), {"violations": 0, "status": "done", "seconds": 1.5, "csv": ""}),
        (
            make_fk(constraint_name="obs_concept", child_table="obs", child_columns=["concept_id"],
                    parent_table="concept", parent_columns=["concept_id"]),
            {"violations": 3, "status": "done", "seconds": 9.1, "csv": "obs__obs_concept.csv"},
        ),
    ]

    path = write_summary(results)
    with open(path, newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert rows[0]["child_table"] == "encounter"
    assert rows[0]["violating_rows"] == "0"
    assert rows[1]["child_table"] == "obs"
    assert rows[1]["parent_table"] == "concept"
    assert rows[1]["violating_rows"] == "3"
    assert rows[1]["csv_file"] == "obs__obs_concept.csv"


def test_write_summary_handles_no_constraints(tmp_path, monkeypatch):
    monkeypatch.setattr(fk_check, "REPORT_DIR", str(tmp_path))
    path = write_summary([])
    with open(path, newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.reader(handle))
    assert len(rows) == 1  # header only
