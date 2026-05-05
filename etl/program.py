import time

from sqlalchemy import text

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning

##### Extraction functions #####
def extract_program(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_program_table(target_engine, drop_create=drop_create)
    info("Fetching data from source program table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _program"))
        target_conn.commit()

    insert_query = text("INSERT INTO _program (program_id, concept_id, creator, date_created, changed_by, date_changed, retired, name, description, uuid) VALUES (:program_id, :concept_id, :creator, :date_created, :changed_by, :date_changed, :retired, :name, :description, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM program")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _program table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "program_id": row.program_id, "concept_id": row.concept_id, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "name": row.name, "description": row.description, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source program table.")

def extract_program_workflow(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_program_workflow_table(target_engine, drop_create=drop_create)
    info("Fetching data from source program_workflow table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _program_workflow"))
        target_conn.commit()

    insert_query = text("INSERT INTO _program_workflow (program_workflow_id, program_id, concept_id, creator, date_created, retired, changed_by, date_changed, uuid) VALUES (:program_workflow_id, :program_id, :concept_id, :creator, :date_created, :retired, :changed_by, :date_changed, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM program_workflow")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _program_workflow table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "program_workflow_id": row.program_workflow_id, "program_id": row.program_id, "concept_id": row.concept_id, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source program_workflow table.")

def extract_program_workflow_state(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_program_workflow_state_table(target_engine, drop_create=drop_create)
    info("Fetching data from source program_workflow_state table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _program_workflow_state"))
        target_conn.commit()

    insert_query = text("INSERT INTO _program_workflow_state (program_workflow_state_id, program_workflow_id, concept_id, initial, terminal, creator, date_created, retired, changed_by, date_changed, uuid) VALUES (:program_workflow_state_id, :program_workflow_id, :concept_id, :initial, :terminal, :creator, :date_created, :retired, :changed_by, :date_changed, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM program_workflow_state")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _program_workflow_state table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "program_workflow_state_id": row.program_workflow_state_id, "program_workflow_id": row.program_workflow_id, "concept_id": row.concept_id, "initial": row.initial, "terminal": row.terminal, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source program_workflow_state table.")

def extract_program_group(drop_create):
    start_time = time.time()
    extract_program(drop_create=drop_create)
    info("Program table created successfully")
    extract_program_workflow(drop_create=drop_create)
    info("Program workflow table created successfully")
    extract_program_workflow_state(drop_create=drop_create)
    info(f"Program workflow state table created successfully (Time: {time.time() - start_time:.2f} seconds)")

##### Loading functions #####
def load_program():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO program (program_id, concept_id, outcomes_concept_id, creator, date_created, changed_by, date_changed, retired, name, description, uuid) 
    SELECT program_id, concept_id, outcomes_concept_id, creator, date_created, changed_by, date_changed, retired, name, description, uuid FROM _program
    """
    with target_engine.connect() as conn:
        info("Loading data for program table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load program completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_program_workflow():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO program_workflow (program_workflow_id, program_id, concept_id, creator, date_created, retired, changed_by, date_changed, uuid) 
    SELECT program_workflow_id, program_id, concept_id, creator, date_created, retired, changed_by, date_changed, uuid FROM _program_workflow
    """
    with target_engine.connect() as conn:
        info("Loading data for program_workflow table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load program_workflow completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_program_workflow_state():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO program_workflow_state (program_workflow_state_id, program_workflow_id, concept_id, `initial`, terminal, creator, date_created, retired, changed_by, date_changed, uuid) 
    SELECT program_workflow_state_id, program_workflow_id, concept_id, `initial`, terminal, creator, date_created, retired, changed_by, date_changed, uuid FROM _program_workflow_state
    """
    with target_engine.connect() as conn:
        info("Loading data for program_workflow_state table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load program_workflow_state completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_program_group():
    load_program()
    load_program_workflow()
    load_program_workflow_state()

