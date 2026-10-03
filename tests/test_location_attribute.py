# tests/test_location_attribute.py
#
# Unit coverage for the LEVEL location-attribute insert. No database required —
# these assert on the compiled SQL, because the rerun-safety of this statement
# is the thing that regressed and it is cheap to protect.
#
# Background: location_attribute's only unique key is `uuid` and the SELECT
# generates UUID() per row, so INSERT IGNORE can never dedupe. Before the fix
# every ETL rerun appended another LEVEL attribute to every location, which is
# invalid (LEVEL has maxOccurs=1) and showed as doubled badges in the web admin.

import re

from etl.location import (
    LEVEL_ATTRIBUTE_VALUES,
    build_location_attribute_insert,
    build_location_attribute_update,
    build_location_upserts,
)


def _sql():
    return str(build_location_attribute_insert())


def _normalised():
    return re.sub(r"\s+", " ", _sql()).upper()


def test_insert_is_guarded_against_duplicates():
    """The NOT EXISTS guard is what makes a rerun a no-op — it must not be lost."""
    sql = _normalised()
    assert "NOT EXISTS" in sql
    assert "EXISTING.LOCATION_ID = L.LOCATION_ID" in sql
    assert "EXISTING.ATTRIBUTE_TYPE_ID = LAT.LOCATION_ATTRIBUTE_TYPE_ID" in sql


def test_guard_ignores_voided_attributes():
    """A voided attribute should not block a fresh one from being inserted."""
    assert "EXISTING.VOIDED = 0" in _normalised()


def test_insert_ignore_is_retained():
    """AGENTS.md: keep INSERT IGNORE where the ETL already used it."""
    assert "INSERT IGNORE INTO LOCATION_ATTRIBUTE" in _normalised()


def test_levels_are_bound_not_interpolated():
    """AGENTS.md: use bound parameters for SQL that includes values."""
    sql = _sql()
    for level in LEVEL_ATTRIBUTE_VALUES:
        assert f"'{level}'" not in sql, f"{level} should be bound, not inlined"
    # An expanding bindparam renders as __[POSTCOMPILE_levels] until values are
    # supplied; binding them must produce one parameter per level.
    compiled = (
        build_location_attribute_insert()
        .bindparams(levels=LEVEL_ATTRIBUTE_VALUES)
        .compile(compile_kwargs={"render_postcompile": True})
    )
    assert sorted(compiled.params.values()) == sorted(LEVEL_ATTRIBUTE_VALUES)


def test_statement_compiles_for_mysql():
    """Guards against a malformed statement or a mis-declared bind parameter."""
    from sqlalchemy.dialects import mysql

    compiled = (
        build_location_attribute_insert()
        .bindparams(levels=LEVEL_ATTRIBUTE_VALUES)
        .compile(dialect=mysql.dialect(), compile_kwargs={"render_postcompile": True})
    )
    assert "NOT EXISTS" in str(compiled).upper()


def test_all_four_levels_are_covered():
    assert LEVEL_ATTRIBUTE_VALUES == ["REGION", "SUBREGION", "DISTRICT", "FACILITY"]


def test_value_reference_comes_from_the_staging_level():
    """
    value_reference must be l.level itself. That equivalence is what let four
    near-identical queries collapse into one, and it keeps value and level from
    drifting apart.
    """
    assert "L.LEVEL, UUID()" in _normalised()


def test_district_is_not_inserted_twice():
    """
    The old code ran two statements for level='DISTRICT' (the second one's WHERE
    was a strict subset of the first), so DISTRICT was duplicated even within a
    single run. There must be exactly one INSERT now.
    """
    assert _normalised().count("INSERT IGNORE") == 1


# --- LEVEL update: locations.xlsx is the source of truth, so a changed level is
# corrected in place on the next --load instead of needing a manual patch.


def _update_normalised():
    return re.sub(r"\s+", " ", str(build_location_attribute_update())).upper()


def test_update_only_touches_active_rows():
    assert "LA.VOIDED = 0" in _update_normalised()


def test_update_only_changes_differing_values():
    """Keeps a rerun a no-op: rows already at the right level are not rewritten."""
    assert "LA.VALUE_REFERENCE <> L.LEVEL" in _update_normalised()


def test_update_matches_on_level_attribute_type():
    sql = _update_normalised()
    assert "LAT.LOCATION_ATTRIBUTE_TYPE_ID = LA.ATTRIBUTE_TYPE_ID" in sql
    assert "LAT.NAME = 'LEVEL'" in sql
    assert "L.LOCATION_ID = LA.LOCATION_ID" in sql


def test_update_is_in_place_and_audited():
    """Update in place (no void and re-insert) and record who/when."""
    sql = _update_normalised()
    assert sql.startswith("UPDATE LOCATION_ATTRIBUTE")
    assert "LA.VALUE_REFERENCE = L.LEVEL" in sql
    assert "LA.CHANGED_BY = 1" in sql
    assert "LA.DATE_CHANGED =" in sql
    assert "INSERT" not in sql and "VOIDED = 1" not in sql


def test_update_levels_are_bound_not_interpolated():
    sql = str(build_location_attribute_update())
    for level in LEVEL_ATTRIBUTE_VALUES:
        assert f"'{level}'" not in sql, f"{level} should be bound, not inlined"
    compiled = (
        build_location_attribute_update()
        .bindparams(levels=LEVEL_ATTRIBUTE_VALUES)
        .compile(compile_kwargs={"render_postcompile": True})
    )
    assert sorted(compiled.params.values()) == sorted(LEVEL_ATTRIBUTE_VALUES)


def test_update_compiles_for_mysql():
    from sqlalchemy.dialects import mysql

    compiled = (
        build_location_attribute_update()
        .bindparams(levels=LEVEL_ATTRIBUTE_VALUES)
        .compile(dialect=mysql.dialect(), compile_kwargs={"render_postcompile": True})
    )
    assert "UPDATE LOCATION_ATTRIBUTE" in str(compiled).upper()


# --- location upserts


def test_every_location_statement_is_an_upsert():
    for query in build_location_upserts():
        assert "INSERT IGNORE" not in query.upper()
        assert "ON DUPLICATE KEY UPDATE" in query.upper()


def test_upserts_never_rewrite_identity():
    for query in build_location_upserts()[1:]:
        update_clause = query.upper().split("ON DUPLICATE KEY UPDATE", 1)[1]
        assert "UUID =" not in update_clause
        assert "LOCATION_ID =" not in update_clause


def test_catch_all_statements_keep_the_parent():
    """
    The last three statements select parent_location = NULL and also match rows the
    level statements already loaded (e.g. every retired district has a parent). If
    they updated parent_location they would orphan those rows.
    """
    for query in build_location_upserts()[-3:]:
        update_clause = query.upper().split("ON DUPLICATE KEY UPDATE", 1)[1]
        assert "PARENT_LOCATION" not in update_clause


def test_level_statements_update_the_parent():
    for query in build_location_upserts()[1:5]:
        update_clause = query.upper().split("ON DUPLICATE KEY UPDATE", 1)[1]
        assert "PARENT_LOCATION = VALUES(PARENT_LOCATION)" in update_clause
