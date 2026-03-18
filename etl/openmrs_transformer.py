import time
from sqlalchemy import text
from config.database import get_source_engine, get_target_engine
from utils.logger import info, warning


def transform_provider():
    start_time = time.time()
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    select_sql = "SELECT user_id, username, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid() as uuid FROM users"
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text(select_sql)).fetchall()

    if source_data:
        info("Transforming data from users to fill _provider table...")
        insert_sql= text("""
            INSERT IGNORE INTO _provider (provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) 
            VALUES (:provider_id, :person_id, :name, :identifier, :creator, :date_created, :changed_by, :date_changed, :retired, :retired_by, :date_retired, :retire_reason, :uuid)
        """)
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_sql, {
                    "provider_id": row.user_id,
                    "person_id": row.person_id,
                    "name": row.username,
                    "identifier": row.username,
                    "creator": row.creator,
                    "date_created": row.date_created,
                    "changed_by": row.changed_by,
                    "date_changed": row.date_changed,
                    "retired": row.retired,
                    "retired_by": row.retired_by,
                    "date_retired": row.date_retired,
                    "retire_reason": row.retire_reason,
                    "uuid": row.uuid
                })
            target_conn.commit()
        info(f"Transform _provider completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")
    else:
        warning("No data found in source _provider table.")

def transform_encounter_provider():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO _encounter_provider (encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid) 
    SELECT e.encounter_id, p.provider_id AS provider_id, 1 AS encounter_role_id, e.creator, e.date_created, e.changed_by, e.date_changed, e.voided, e.date_voided, e.voided_by, e.void_reason, uuid() AS uuid FROM _encounter e 
    INNER JOIN _provider p ON p.person_id = e.provider_id
    """
    with target_engine.connect() as conn:
        info("Transforming data to fill _encounter_provider table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Transform _encounter_provider completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")
