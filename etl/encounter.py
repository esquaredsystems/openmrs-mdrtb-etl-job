import time

from sqlalchemy import text

from config.config import BATCH_SIZE
from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_encounter_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_encounter_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source encounter_type table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _encounter_type"))
        target_conn.commit()

    insert_query = text(
        "INSERT INTO _encounter_type (encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:encounter_type_id, :name, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM encounter_type")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _encounter_type table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "encounter_type_id": row.encounter_type_id, "name": row.name, "description": row.description,
                    "creator": row.creator, "date_created": row.date_created, "retired": row.retired,
                    "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason,
                    "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source encounter_type table.")


def extract_encounter(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_encounter_table(target_engine, drop_create=drop_create)
    info("Fetching data from source encounter table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _encounter"))
        target_conn.commit()

    insert_query = text(
        "INSERT INTO _encounter (encounter_id, encounter_type, patient_id, provider_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) VALUES (:encounter_id, :encounter_type, :patient_id, :provider_id, :location_id, :form_id, :encounter_datetime, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :changed_by, :date_changed, :uuid)")

    with source_engine.connect() as source_conn:
        # Using execution_options(yield_per=BATCH_SIZE) for batching
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM encounter"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "encounter_id": row.encounter_id, "encounter_type": row.encounter_type,
                    "patient_id": row.patient_id, "provider_id": row.provider_id, "location_id": row.location_id,
                    "form_id": row.form_id, "encounter_datetime": row.encounter_datetime, "creator": row.creator,
                    "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by,
                    "date_voided": row.date_voided, "void_reason": row.void_reason, "changed_by": row.changed_by,
                    "date_changed": row.date_changed, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _encounter table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _encounter table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()

    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source encounter table.")


def extract_encounter_provider(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_encounter_provider_table(target_engine, drop_create=drop_create)


def extract_encounter_group(drop_create):
    start_time = time.time()
    extract_encounter_type(drop_create=drop_create)
    info("Encounter type table created successfully")
    extract_encounter(drop_create=drop_create)
    info("Encounter table created successfully")
    extract_encounter_provider(drop_create=drop_create)
    info("Encounter provider table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")


##### Transformation functions #####
def transform_provider():
    start_time = time.time()
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    select_sql = "SELECT user_id, username, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid() as uuid FROM users"
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text(select_sql)).fetchall()

    if source_data:
        info("Transforming data from users to fill _provider table...")
        insert_sql = text("""
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


def transform_encounter_group():
    transform_provider()
    transform_encounter_provider()


##### Loading functions #####
def load_encounter_type():
    start_time = time.time()
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading data for encounter_type table...")
        # Insert the default encounter_type
        conn.execute(text("""
            INSERT IGNORE INTO encounter_type (name, description, creator, date_created, retired, uuid) 
            VALUES ('Drug Order', 'Created to attach with Orders for Openmrs 2x.', 1, current_timestamp(), 0, uuid())
        """))
        # Load from staging table
        conn.execute(text("""
            INSERT IGNORE INTO encounter_type (encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid)
            SELECT encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM _encounter_type
        """))
        conn.commit()
    info(f"Load encounter_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_encounter():
    start_time = time.time()
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading data for encounter table...")
        # INSERT IGNORE for records with date_created NOT in current year
        conn.execute(text("""
            INSERT IGNORE INTO encounter (encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid)
            SELECT encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid FROM _encounter
            WHERE YEAR(date_created) < YEAR(CURDATE())
        """))
        # UPSERT for records with date_created in current year (but do NOT reset uuid)
        conn.execute(text("""
            INSERT INTO encounter (encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid)
            SELECT encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid FROM _encounter
            WHERE YEAR(date_created) => YEAR(CURDATE())
            ON DUPLICATE KEY UPDATE
                encounter_type = VALUES(encounter_type),
                patient_id = VALUES(patient_id),
                location_id = VALUES(location_id),
                form_id = VALUES(form_id),
                encounter_datetime = VALUES(encounter_datetime),
                creator = VALUES(creator),
                date_created = VALUES(date_created),
                voided = VALUES(voided),
                voided_by = VALUES(voided_by),
                date_voided = VALUES(date_voided),
                void_reason = VALUES(void_reason),
                changed_by = VALUES(changed_by),
                date_changed = VALUES(date_changed)
        """))
        conn.commit()
    info(f"Load encounter completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_encounter_provider():
    start_time = time.time()
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading data for encounter_provider table...")
        conn.execute(text("""
            INSERT IGNORE INTO encounter_provider (encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid)
            SELECT encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid FROM _encounter_provider
        """))
        conn.commit()
    info(f"Load encounter_provider completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_encounter_group():
    load_encounter_type()
    load_encounter()
    load_encounter_provider()
