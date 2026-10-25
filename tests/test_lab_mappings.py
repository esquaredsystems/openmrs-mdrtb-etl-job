import re

import pandas as pd
import pytest

from etl import lab
from utils.helpers import get_labtest_attribute_type_data


def test_dst_drugs_map_each_field_and_drug_once():
    # A copy-paste slip once mapped levofloxacin (101) to ETHAMBUTOL RESISTANCE.
    names = [name for name, _ in lab.DST_DRUGS]
    drugs = [drug for _, drug in lab.DST_DRUGS]
    assert len(names) == len(set(names))
    assert len(drugs) == len(set(drugs))


def test_dst_results_use_the_susceptible_and_resistant_answers():
    # 118 SUSCEPTIBLE TO TB DRUG -> 543 SUSCEPTIBLE, 132 RESISTANT TO TB DRUG -> 183 RESISTANT
    assert "WHEN 118 THEN 543" in lab.DST_RESULT_ANSWER_SQL
    assert "WHEN 132 THEN 183" in lab.DST_RESULT_ANSWER_SQL


# The load functions build their SQL inline, so they run against an engine that only records what it is sent.
class _RecordingConnection:
    def __init__(self, statements):
        self.statements = statements

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, *args):
        self.statements.append(re.sub(r"\s+", " ", str(statement)).strip())

    def commit(self):
        pass


class _RecordingEngine:
    def __init__(self):
        self.statements = []

    def connect(self):
        return _RecordingConnection(self.statements)


def _record(monkeypatch, load):
    engine = _RecordingEngine()
    monkeypatch.setattr(lab, "get_target_engine", lambda: engine)
    monkeypatch.setattr(lab, "info", lambda message: None)
    load()
    return engine.statements


ATTRIBUTE_QUERY = re.compile(
    r"tat\.test_attribute_type_id, (?P<value>.+?), o\.creator.*"
    r"tat\.test_type_id = 5(?: and tat\.group_name = '(?P<group>[^']+)')? and tat\.name = '(?P<name>[^']+)'"
    r" where o\.concept_id = (?P<concept>\d+) "
)


@pytest.fixture
def attribute_queries(monkeypatch):
    # (group, attribute name, concept id, value column) for each insert in load_labtest_attribute()
    queries = []
    for sql in _record(monkeypatch, lab.load_labtest_attribute):
        match = ATTRIBUTE_QUERY.search(sql)
        if match:
            queries.append((match["group"], match["name"], int(match["concept"]), match["value"]))
    return queries


@pytest.mark.parametrize("group, name, concept_id, value", [
    ("XPERT", "SENT TO DST", 609, "c.uuid"),
    ("HAIN", "SENT TO DST", 609, "c.uuid"),
    ("HAIN", "RIFAMPICIN RESISTANCE", 546, "c.uuid"),
    ("HAIN2", "КМ/АМ/СМ RESULT", 412, "c.uuid"),
    # Once also loaded from 545 (isoniazid resistance)
    ("HAIN2", "ISONIAZID RESULT", 322, "c.uuid"),
    ("HAIN2", "RIFAMPICIN RESULT", 317, "c.uuid"),
    ("CULTURE", "TUBERCULOSIS CULTURE METHOD", 151, "c.uuid"),
    ("SMEAR", "TUBERCULOSIS SMEAR RESULT", 54, "c.uuid"),
    ("SMEAR", "BACILLI", 43, "o.value_numeric"),
])
def test_attribute_reads_its_concept_and_value_column(attribute_queries, group, name, concept_id, value):
    assert [(c, v) for g, n, c, v in attribute_queries if (g, n) == (group, name)] == [(concept_id, value)]


def test_each_attribute_is_loaded_by_one_query(attribute_queries):
    fields = [(group, name) for group, name, _, _ in attribute_queries]
    assert sorted(f for f in set(fields) if fields.count(f) > 1) == []


def test_each_attribute_exists_in_commonlab(attribute_queries):
    # tat.name and tat.group_name must match the attribute_types sheet exactly, or the insert silently loads nothing
    df = get_labtest_attribute_type_data()
    df = df[df["test_type_id"] == 5]
    known = {(None if pd.isna(group) else group, name) for group, name in zip(df["group_name"], df["name"])}
    assert [(group, name) for group, name, _, _ in attribute_queries if (group, name) not in known] == []


def test_dst_mgit_results_only_go_to_dst_mgit_tests(monkeypatch):
    # Without the test type filter every DST result was copied onto the COMMON TEST (63,449 rows), voided obs included
    queries = [sql for sql in _record(monkeypatch, lab.load_labtest_attribute_for_drugs) if "cat.test_type_id = 7" in sql]
    assert len(queries) == len(lab.DST_DRUGS)
    assert all("ct.test_order_id = o2.order_id and ct.test_type_id = 7" in sql for sql in queries)
    assert all(sql.endswith(" and o.voided = 0;") for sql in queries)


def test_common_test_dst_results_only_use_the_dst_group(monkeypatch):
    # The DST field names also exist ungrouped and in HAIN, which used to get a copy of every result
    queries = [sql for sql in _record(monkeypatch, lab.load_labtest_attribute_for_drugs) if "cat.test_type_id = 5" in sql]
    assert len(queries) == len(lab.DST_DRUGS)
    assert all("cat.test_type_id = 5 and cat.group_name = 'DST' and" in sql for sql in queries)
