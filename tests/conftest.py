import pytest
from config.database import get_source_engine, get_target_engine

TOLERANCE = 0.001


def within_tolerance(src, tgt):
    if src == 0:
        return tgt == 0
    return abs(src - tgt) / src <= TOLERANCE


@pytest.fixture(scope="session")
def source_conn():
    engine = get_source_engine()
    with engine.connect() as conn:
        yield conn


@pytest.fixture(scope="session")
def target_conn():
    engine = get_target_engine()
    with engine.connect() as conn:
        yield conn
