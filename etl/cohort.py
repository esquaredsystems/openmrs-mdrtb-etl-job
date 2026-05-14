import time

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_cohort(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if drop_create:
        create_cohort_table(target_engine, drop_create=drop_create)

    info("Fetching data from source cohort table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _cohort"))
        target_conn.commit()

    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM cohort")).fetchall()

    if source_data:
        info(f"Inserting {len(source_data)} records into target _cohort table...")
        insert_query = text("""
        INSERT INTO _cohort (cohort_id, name, description, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) 
        VALUES (:cohort_id, :name, :description, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :changed_by, :date_changed, :uuid)
        """)
        
        with target_engine.connect() as target_conn:
            for row in source_data:
                # Handle bit field for MySQL/SQLAlchemy if necessary
                target_conn.execute(insert_query, {
                    "cohort_id": row.cohort_id, "name": row.name, "description": row.description, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source cohort table.")

def extract_cohort_member(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_cohort_member_table(target_engine, drop_create=drop_create)
    info("Fetching data from source cohort_member table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _cohort_member"))
        target_conn.commit()

    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM cohort_member")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _cohort_member table...")
        insert_query = text("INSERT INTO _cohort_member (cohort_id, patient_id) VALUES (:cohort_id, :patient_id)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "cohort_id": row.cohort_id, "patient_id": row.patient_id
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source cohort_member table.")

def extract_cohort_group(drop_create):
    start_time = time.time()
    extract_cohort(drop_create=drop_create)
    info("Cohort table created successfully")
    extract_cohort_member(drop_create=drop_create)
    info("Cohort member table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")

##### Loading functions #####
def load_cohort():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO cohort (cohort_id, name, description, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) 
    SELECT cohort_id, name, description, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid FROM _cohort
    """
    with target_engine.connect() as conn:
        info("Loading data for cohort table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load cohort completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_cohort_group():
    load_cohort()

