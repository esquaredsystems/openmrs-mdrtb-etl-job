import time

from config.config import BATCH_SIZE, CUTOFF_DATE
from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_encounter_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create or not table_exists(target_engine, '_encounter_type'):
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
    if drop_create or not table_exists(target_engine, '_encounter'):
        create_encounter_table(target_engine, drop_create=drop_create)
    info("Fetching data from source encounter table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _encounter"))
        target_conn.commit()

    insert_query = text(
        "INSERT INTO _encounter (encounter_id, encounter_type, patient_id, provider_id, location_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) VALUES (:encounter_id, :encounter_type, :patient_id, :provider_id, :location_id, :encounter_datetime, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :changed_by, :date_changed, :uuid)")

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
                    "encounter_datetime": row.encounter_datetime, "creator": row.creator,
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
    start_time = time.time()
    target_engine = get_target_engine()
    if drop_create or not table_exists(target_engine, '_encounter_provider'):
        create_encounter_provider_table(target_engine, drop_create=drop_create)
    select_insert_sql = """
    INSERT IGNORE INTO _encounter_provider (encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid)
    SELECT e.encounter_id, p.provider_id AS provider_id, 1 AS encounter_role_id, e.creator, e.date_created, e.changed_by, e.date_changed, e.voided, e.date_voided, e.voided_by, e.void_reason, uuid() AS uuid FROM _encounter e
    INNER JOIN _provider p ON p.person_id = e.provider_id
    """
    with target_engine.connect() as conn:
        info("Inserting data into _encounter_provider table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Extract _encounter_provider completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def extract_encounter_group(drop_create):
    start_time = time.time()
    extract_encounter_type(drop_create=drop_create)
    info("Encounter type table created successfully")
    extract_encounter(drop_create=drop_create)
    info("Encounter table created successfully")
    extract_encounter_provider(drop_create=drop_create)
    info("Encounter provider table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")


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
    info("Loading data for encounter table...")
    # Cutoff: only keep encounters created, changed, voided or held (encounter_datetime) on/after CUTOFF_DATE.
    # Anything entirely before the cutoff is excluded from the target `encounter` table.
    cutoff_clause = """
        (YEAR(date_created) >= YEAR(:cutoff_date)
         OR YEAR(date_changed) >= YEAR(:cutoff_date)
         OR YEAR(date_voided) >= YEAR(:cutoff_date)
         OR YEAR(encounter_datetime) >= YEAR(:cutoff_date)
         OR encounter_id IN (SELECT encounter_id FROM _recent_obs_encounter))
    """
    with target_engine.connect() as conn:
        # obs are never edited in place (an edit inserts a new obs row), so an encounter with a recent obs must be kept
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS _recent_obs_encounter"))
        conn.execute(text("""
            CREATE TEMPORARY TABLE _recent_obs_encounter (encounter_id INT NOT NULL PRIMARY KEY)
            AS SELECT DISTINCT encounter_id FROM _obs
               WHERE encounter_id IS NOT NULL AND (date_created >= :cutoff_date OR date_voided >= :cutoff_date)
        """), {"cutoff_date": CUTOFF_DATE})
        # INSERT IGNORE for records with date_created NOT in current year
        total_old = conn.execute(text(f"""
            SELECT COUNT(*) FROM _encounter
            WHERE YEAR(date_created) < YEAR(CURRENT_TIMESTAMP()) AND {cutoff_clause}
        """), {"cutoff_date": CUTOFF_DATE}).scalar()
        offset = 0
        batch_number = 1
        while offset < total_old:
            conn.execute(text(f"""
                INSERT IGNORE INTO encounter (encounter_id, encounter_type, patient_id, location_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid)
                SELECT encounter_id, encounter_type, patient_id, location_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid
                FROM _encounter
                WHERE YEAR(date_created) < YEAR(CURRENT_TIMESTAMP()) AND {cutoff_clause}
                ORDER BY encounter_id
                LIMIT :limit OFFSET :offset
            """), {"limit": BATCH_SIZE, "offset": offset, "cutoff_date": CUTOFF_DATE})
            conn.commit()
            info(f"[historical] Batch {batch_number}: inserted up to {min(offset + BATCH_SIZE, total_old)} of {total_old} records")
            offset += BATCH_SIZE
            batch_number += 1

        # UPSERT for records with date_created in current year (but do NOT reset uuid)
        total_current = conn.execute(text(f"""
            SELECT COUNT(*) FROM _encounter
            WHERE YEAR(date_created) >= YEAR(CURRENT_TIMESTAMP()) AND {cutoff_clause}
        """), {"cutoff_date": CUTOFF_DATE}).scalar()
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        conn.commit()
        offset = 0
        batch_number = 1
        while offset < total_current:
            conn.execute(text(f"""
                INSERT INTO encounter (encounter_id, encounter_type, patient_id, location_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid)
                SELECT encounter_id, encounter_type, patient_id, location_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid
                FROM (
                    SELECT encounter_id, encounter_type, patient_id, location_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid
                    FROM _encounter
                    WHERE YEAR(date_created) >= YEAR(CURRENT_TIMESTAMP()) AND {cutoff_clause}
                    ORDER BY encounter_id
                    LIMIT :limit OFFSET :offset
                ) batch
                ON DUPLICATE KEY UPDATE
                    encounter_type = VALUES(encounter_type),
                    patient_id = VALUES(patient_id),
                    location_id = VALUES(location_id),
                    location_id = VALUES(location_id),
                    encounter_datetime = VALUES(encounter_datetime),
                    creator = VALUES(creator),
                    date_created = VALUES(date_created),
                    voided = VALUES(voided),
                    voided_by = VALUES(voided_by),
                    date_voided = VALUES(date_voided),
                    void_reason = VALUES(void_reason),
                    changed_by = VALUES(changed_by),
                    date_changed = VALUES(date_changed)
            """), {"limit": BATCH_SIZE, "offset": offset, "cutoff_date": CUTOFF_DATE})
            conn.commit()
            info(f"[current year] Batch {batch_number}: upserted up to {min(offset + BATCH_SIZE, total_current)} of {total_current} records")
            offset += BATCH_SIZE
            batch_number += 1
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        conn.commit()
    info(f"Load encounter completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_encounter_provider():
    start_time = time.time()
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading data for encounter_provider table...")
        # Join against the already-cutoff-filtered target `encounter` table so rows for
        # encounters excluded by the cutoff are not inserted (would otherwise violate the FK).
        conn.execute(text("""
            INSERT IGNORE INTO encounter_provider (encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid)
            SELECT ep.encounter_id, ep.provider_id, ep.encounter_role_id, ep.creator, ep.date_created, ep.changed_by, ep.date_changed, ep.voided, ep.date_voided, ep.voided_by, ep.void_reason, ep.uuid
            FROM _encounter_provider ep
            INNER JOIN encounter e ON e.encounter_id = ep.encounter_id
        """))
        conn.commit()
    info(f"Load encounter_provider completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_encounter_group():
    load_encounter_type()
    load_encounter()
    load_encounter_provider()
