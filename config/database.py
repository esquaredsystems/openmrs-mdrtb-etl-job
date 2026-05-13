from sqlalchemy import URL, create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()


def _get_required_env(name):
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _mysql_url(prefix):
    return URL.create(
        "mysql+pymysql",
        username=_get_required_env(f"{prefix}_DB_USER"),
        password=_get_required_env(f"{prefix}_DB_PASS"),
        host=_get_required_env(f"{prefix}_DB_HOST"),
        port=int(_get_required_env(f"{prefix}_DB_PORT")),
        database=_get_required_env(f"{prefix}_DB_NAME"),
        query={"charset": "utf8mb4"},
    )


def get_source_engine():
    return create_engine(_mysql_url("SOURCE"))


def get_target_engine():
    return create_engine(_mysql_url("TARGET"))


def set_foreign_key_checks(engine, enabled):
    value = 1 if enabled else 0
    with engine.connect() as conn:
        conn.execute(text(f"SET FOREIGN_KEY_CHECKS = {value}"))
        conn.commit()
