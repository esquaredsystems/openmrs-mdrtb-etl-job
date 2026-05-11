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
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _orders"))
        target_conn.commit()

    insert_query = text("INSERT INTO _orders (order_id, order_type_id, concept_id, orderer, encounter_id, instructions, start_date, auto_expire_date, discontinued, discontinued_date, discontinued_by, discontinued_reason, discontinued_reason_non_coded, creator, date_created, voided, voided_by, date_voided, void_reason, patient_id, accession_number, uuid) VALUES (:order_id, :order_type_id, :concept_id, :orderer, :encounter_id, :instructions, :start_date, :auto_expire_date, :discontinued, :discontinued_date, :discontinued_by, :discontinued_reason, :discontinued_reason_non_coded, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :patient_id, :accession_number, :uuid)")
    
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
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _order_type"))
        target_conn.commit()

    insert_query = text("INSERT INTO _order_type (order_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:order_type_id, :name, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
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
    info("Order type table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")

##### Loading functions #####
def load_order_type():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO order_type (order_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT order_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid
    FROM _order_type
    """
    with target_engine.connect() as conn:
        info("Loading data for order_type table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load order_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_order():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT INTO orders (
        order_id, order_type_id, concept_id, orderer, encounter_id, instructions, 
        date_activated, auto_expire_date, date_stopped,
        order_reason, order_reason_non_coded, 
        creator, date_created, voided, voided_by, date_voided, void_reason, patient_id,
        accession_number, uuid, urgency, order_number, order_action, care_setting
    )
    SELECT
        o.order_id, o.order_type_id, o.concept_id, COALESCE(p.provider_id, o.orderer) AS orderer, o.encounter_id, o.instructions,
        o.start_date AS date_activated, o.auto_expire_date, o.discontinued_date AS date_stopped,
        o.discontinued_reason AS order_reason, o.discontinued_reason_non_coded AS order_reason_non_coded,
        o.creator, o.date_created, o.voided, o.voided_by, o.date_voided, o.void_reason, o.patient_id,
        o.accession_number, o.uuid, 'ROUTINE' AS urgency, CONCAT('ORD-', o.order_id) AS order_number,
        (CASE WHEN o.discontinued = 1 THEN 'DISCONTINUE' ELSE 'NEW' END) AS order_action,
        COALESCE((SELECT MIN(care_setting_id) FROM care_setting), 1) AS care_setting
    FROM _orders o
    LEFT JOIN users u ON u.user_id = o.orderer
    LEFT JOIN provider p ON p.person_id = u.person_id AND p.retired = 0
    """
    with target_engine.connect() as conn:
        info("Loading data for orders table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load orders completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_orders_group():
    start_time = time.time()
    load_order_type()
    load_order()
    info(f"Load orders group completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

