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

from etl.location import LEVEL_ATTRIBUTE_VALUES, build_location_attribute_insert


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
