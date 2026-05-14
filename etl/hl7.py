import time

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_hl7_in_error(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_hl7_in_error_table(target_engine, drop_create=drop_create)
    info("Fetching data from source hl7_in_error table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _hl7_in_error"))
        target_conn.commit()

    insert_query = text("INSERT INTO _hl7_in_error (hl7_in_error_id, hl7_source, hl7_source_key, hl7_data, error, error_details, date_created, uuid) VALUES (:hl7_in_error_id, :hl7_source, :hl7_source_key, :hl7_data, :error, :error_details, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM hl7_in_error")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _hl7_in_error table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "hl7_in_error_id": row.hl7_in_error_id, "hl7_source": row.hl7_source, "hl7_source_key": row.hl7_source_key, "hl7_data": row.hl7_data, "error": row.error, "error_details": row.error_details, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source hl7_in_error table.")

def extract_hl7_in_queue(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_hl7_in_queue_table(target_engine, drop_create=drop_create)
    info("Fetching data from source hl7_in_queue table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _hl7_in_queue"))
        target_conn.commit()

    insert_query = text("INSERT INTO _hl7_in_queue (hl7_in_queue_id, hl7_source, hl7_source_key, hl7_data, message_state, date_processed, error_msg, date_created, uuid) VALUES (:hl7_in_queue_id, :hl7_source, :hl7_source_key, :hl7_data, :message_state, :date_processed, :error_msg, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM hl7_in_queue")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _hl7_in_queue table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "hl7_in_queue_id": row.hl7_in_queue_id, "hl7_source": row.hl7_source, "hl7_source_key": row.hl7_source_key, "hl7_data": row.hl7_data, "message_state": row.message_state, "date_processed": row.date_processed, "error_msg": row.error_msg, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source hl7_in_queue table.")

def extract_hl7_source(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_hl7_source_table(target_engine, drop_create=drop_create)
    info("Fetching data from source hl7_source table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _hl7_source"))
        target_conn.commit()

    insert_query = text("INSERT INTO _hl7_source (hl7_source_id, name, description, creator, date_created, uuid) VALUES (:hl7_source_id, :name, :description, :creator, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM hl7_source")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _hl7_source table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "hl7_source_id": row.hl7_source_id, "name": row.name, "description": row.description, "creator": row.creator, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source hl7_source table.")

def extract_hl7_group(drop_create):
    start_time = time.time()
    extract_hl7_in_error(drop_create=drop_create)
    info("HL7 in error table created successfully")
    extract_hl7_in_queue(drop_create=drop_create)
    info("HL7 in queue table created successfully")
    extract_hl7_source(drop_create=drop_create)
    info("HL7 source table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")

##### Loading functions #####
def load_hl7_source():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO hl7_source (hl7_source_id, name, description, creator, date_created, uuid) 
    SELECT hl7_source_id, name, description, creator, date_created, uuid FROM _hl7_source
    """
    with target_engine.connect() as conn:
        info("Loading data for hl7_source table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load hl7_source completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_hl7_error():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO hl7_in_error (hl7_in_error_id, hl7_source, hl7_source_key, hl7_data, error, error_details, date_created, uuid) 
    SELECT hl7_in_error_id, hl7_source, hl7_source_key, hl7_data, error, error_details, date_created, uuid FROM _hl7_in_error
    """
    with target_engine.connect() as conn:
        info("Loading data for hl7_in_error table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load hl7_error completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_hl7_queue():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO hl7_in_queue (hl7_in_queue_id, hl7_source, hl7_source_key, hl7_data, message_state, date_processed, error_msg, date_created, uuid) 
    SELECT hl7_in_queue_id, hl7_source, hl7_source_key, hl7_data, message_state, date_processed, error_msg, date_created, uuid FROM _hl7_in_queue
    """
    with target_engine.connect() as conn:
        info("Loading data for hl7_in_queue table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load hl7_queue completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_hl7_group():
    load_hl7_source()
    load_hl7_error()
    load_hl7_queue()

