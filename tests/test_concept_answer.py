# tests/test_concept_answer.py
#
# Unit coverage for loading concept_answer from concept_*.csv. No database required — the SQL is
# asserted on once compiled, and load_concept_answer() runs against a recording stand-in for the engine.
#
# Background: load_concept_answer() used to be a plain INSERT IGNORE, so answers dropped from the sheet stayed
# in the target and a new sort_weight never reached an answer that already existed (the RESISTANCE TYPE
# answers of concept 435 hit both).

import re

import pytest
from sqlalchemy.dialects import mysql

from etl import concept
from etl.concept import (
    build_concept_answer_changed_count,
    build_concept_answer_delete,
    build_concept_answer_upsert,
    load_concept_answer,
)


def _normalised(statement):
    return re.sub(r"\s+", " ", str(statement)).upper()


def test_delete_is_limited_to_concepts_listed_in_the_sheet():
    """Answers of concepts that are not in _concept_answer must be left alone."""
    sql = _normalised(build_concept_answer_delete())
    assert sql.startswith("DELETE CA FROM CONCEPT_ANSWER AS CA")
    assert "CA.CONCEPT_ID IN (SELECT S.CONCEPT_ID FROM _CONCEPT_ANSWER AS S)" in sql


def test_delete_keeps_answers_that_are_in_the_sheet():
    sql = _normalised(build_concept_answer_delete())
    assert "NOT EXISTS (SELECT 1 FROM _CONCEPT_ANSWER AS S WHERE S.CONCEPT_ANSWER_ID = CA.CONCEPT_ANSWER_ID)" in sql


def test_delete_has_no_hardcoded_ids():
    """AGENTS.md: the sheet decides what is removed, not ids written into the code."""
    assert not re.search(r"\d", str(build_concept_answer_delete()).replace("SELECT 1", ""))


def test_upsert_updates_answer_concept_and_sort_weight_on_duplicate_key():
    sql = _normalised(build_concept_answer_upsert())
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "ANSWER_CONCEPT = VALUES(ANSWER_CONCEPT)" in sql
    assert "SORT_WEIGHT = VALUES(SORT_WEIGHT)" in sql


def test_upsert_does_not_rewrite_identity_columns():
    """uuid, creator and date_created of an existing answer must stay as they are in the target."""
    update_clause = _normalised(build_concept_answer_upsert()).split("ON DUPLICATE KEY UPDATE")[1]
    assert update_clause.strip() == "ANSWER_CONCEPT = VALUES(ANSWER_CONCEPT), SORT_WEIGHT = VALUES(SORT_WEIGHT)"


def test_insert_ignore_is_retained():
    """AGENTS.md: keep INSERT IGNORE where the ETL already used it."""
    assert "INSERT IGNORE INTO CONCEPT_ANSWER" in _normalised(build_concept_answer_upsert())


def test_changed_count_compares_both_columns_null_safely():
    sql = _normalised(build_concept_answer_changed_count())
    assert "NOT (CA.ANSWER_CONCEPT <=> S.ANSWER_CONCEPT)" in sql
    assert "NOT (CA.SORT_WEIGHT <=> S.SORT_WEIGHT)" in sql
    assert "S.CONCEPT_ANSWER_ID = CA.CONCEPT_ANSWER_ID" in sql


@pytest.mark.parametrize("build", [
    build_concept_answer_delete,
    build_concept_answer_changed_count,
    build_concept_answer_upsert,
])
def test_statements_compile_for_mysql(build):
    assert str(build().compile(dialect=mysql.dialect())).strip()


class _Result:
    def __init__(self, scalar=None, rowcount=0):
        self._scalar = scalar
        self.rowcount = rowcount

    def scalar(self):
        return self._scalar


class _FakeConnection:
    """Records what load_concept_answer() runs, and answers the three reads/writes it depends on."""

    def __init__(self, staged, changed=0, deleted=0):
        self.staged, self.changed, self.deleted = staged, changed, deleted
        self.executed = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        sql = _normalised(statement)
        self.executed.append(sql)
        if sql.startswith("DELETE"):
            return _Result(rowcount=self.deleted)
        if sql.startswith("SELECT COUNT(*) FROM CONCEPT_ANSWER"):
            return _Result(scalar=self.changed)
        if sql.startswith("SELECT COUNT(*) FROM _CONCEPT_ANSWER"):
            return _Result(scalar=self.staged)
        return _Result()

    def commit(self):
        self.commits += 1


class _FakeEngine:
    def __init__(self, connection):
        self._connection = connection

    def connect(self):
        return self._connection


@pytest.fixture
def run_load(monkeypatch):
    def _run(staged, changed=0, deleted=0):
        connection = _FakeConnection(staged, changed=changed, deleted=deleted)
        logged = {"info": [], "warning": []}
        monkeypatch.setattr(concept, "get_target_engine", lambda: _FakeEngine(connection))
        monkeypatch.setattr(concept, "info", logged["info"].append)
        monkeypatch.setattr(concept, "warning", logged["warning"].append)
        load_concept_answer()
        return connection, logged

    return _run


def test_load_deletes_updates_and_commits_once(run_load):
    connection, logged = run_load(staged=911, changed=6, deleted=3)

    statements = [sql.split(" ")[0] + " " + sql.split(" ")[1] for sql in connection.executed]
    assert statements == ["SELECT COUNT(*)", "DELETE CA", "SELECT COUNT(*)", "INSERT IGNORE"]
    assert connection.commits == 1
    assert not logged["warning"]
    assert "concept_answer row(s): 3 deleted, 6 updated" in logged["info"]


def test_load_deletes_before_it_upserts(run_load):
    connection, _ = run_load(staged=911)
    kinds = ["DELETE" if sql.startswith("DELETE") else "INSERT" if sql.startswith("INSERT") else "OTHER"
             for sql in connection.executed]
    assert kinds.index("DELETE") < kinds.index("INSERT")


def test_empty_staging_table_skips_the_delete_and_warns(run_load):
    """A failed or empty workbook read must never be able to wipe the answers in the target."""
    connection, logged = run_load(staged=0)

    assert not any(sql.startswith(("DELETE", "INSERT")) for sql in connection.executed)
    assert connection.commits == 0
    assert len(logged["warning"]) == 1
    assert "_concept_answer" in logged["warning"][0]
    assert not any("completed successfully" in message for message in logged["info"])
