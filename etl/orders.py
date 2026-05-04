import time

from sqlalchemy import text

from config.config import BATCH_SIZE
from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning

##### Extraction functions #####
def extract_orders(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_orders_table(target_engine, drop_create=drop_create)
    info("Fetching data from source orders table...")
    insert_query = text("INSERT IGNORE INTO _orders (order_id, order_type_id, concept_id, orderer, encounter_id, instructions, start_date, auto_expire_date, discontinued, discontinued_date, discontinued_by, discontinued_reason, discontinued_reason_non_coded, creator, date_created, voided, voided_by, date_voided, void_reason, patient_id, accession_number, uuid) VALUES (:order_id, :order_type_id, :concept_id, :orderer, :encounter_id, :instructions, :start_date, :auto_expire_date, :discontinued, :discontinued_date, :discontinued_by, :discontinued_reason, :discontinued_reason_non_coded, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :patient_id, :accession_number, :uuid)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM orders"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "order_id": row.order_id, "order_type_id": row.order_type_id, "concept_id": row.concept_id, "orderer": row.orderer, "encounter_id": row.encounter_id, "instructions": row.instructions, "start_date": row.start_date, "auto_expire_date": row.auto_expire_date, "discontinued": row.discontinued, "discontinued_date": row.discontinued_date, "discontinued_by": row.discontinued_by, "discontinued_reason": row.discontinued_reason, "discontinued_reason_non_coded": row.discontinued_reason_non_coded, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "patient_id": row.patient_id, "accession_number": row.accession_number, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _orders table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _orders table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source orders table.")

def extract_order_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_order_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source order_type table...")
    insert_query = text("INSERT IGNORE INTO _order_type (order_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:order_type_id, :name, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM order_type")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _order_type table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "order_type_id": row.order_type_id, "name": row.name, "description": row.description, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source order_type table.")

def extract_orders_group(drop_create):
    start_time = time.time()
    extract_orders(drop_create=drop_create)
    info("Orders table created successfully")
    extract_order_type(drop_create=drop_create)
    info(f"Order type table created successfully (Time: {time.time() - start_time:.2f} seconds)")

##### Loading functions #####
def load_orders_group():
    pass

