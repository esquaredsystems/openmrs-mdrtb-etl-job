import time

from config.config import BATCH_SIZE
from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_orders(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create or not table_exists(target_engine, '_orders'):
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
    if drop_create or not table_exists(target_engine, '_order_type'):
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
        SELECT order_type_id, name, description, 1, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM _order_type
    """
    with target_engine.connect() as conn:
        info("Loading data for order_type table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load order_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_order():
    start_time = time.time()
    target_engine = get_target_engine()
    truncate_orders_sql = "TRUNCATE orders"
    insert_seed_order_sql = """
        INSERT INTO orders (
            order_type_id, concept_id, orderer, encounter_id, instructions, date_activated, auto_expire_date, date_stopped, order_reason, order_reason_non_coded, creator, date_created, voided, voided_by, date_voided, void_reason, patient_id, accession_number, uuid, urgency, order_number, previous_order_id, order_action, comment_to_fulfiller, care_setting, scheduled_date, order_group_id, sort_weight, fulfiller_comment, fulfiller_status
        ) VALUES (
            1, 410, 3, 147521, NULL, '2017-05-04 00:00:00', NULL, NULL, NULL, NULL, 1, '2023-04-09 07:33:26', 0, NULL, NULL, NULL, 32072, NULL, '90ef1459-8b33-4a18-936e-2cfdfbe99649', 'ROUTINE', 'ORD-1', NULL, 'NEW', NULL, 1, NULL, NULL, NULL, NULL, NULL
        )
    """
    insert_encounter_orders_sql = """
        INSERT INTO orders (
            order_type_id, concept_id, orderer, encounter_id, date_activated, creator, date_created, voided, voided_by, date_voided, void_reason, patient_id, uuid, urgency, order_number, order_action, care_setting
        )
        SELECT 3 AS order_type_id, cn.concept_id, p.provider_id AS orderer, e.encounter_id, e.encounter_datetime, e.creator, e.date_created, e.voided, e.voided_by, e.date_voided, e.voided_by, e.patient_id, UUID(), 'ROUTINE' AS urgency, CONCAT('ORD-', e.encounter_id) AS order_number, 'NEW' AS order_action, 1 AS care_setting FROM encounter AS e
        INNER JOIN users u ON u.user_id = e.creator
        INNER JOIN patient pt ON pt.patient_id = e.patient_id
        INNER JOIN provider p ON p.person_id = u.person_id
        INNER JOIN concept_name cn ON cn.name = 'MICROSCOPY TEST CONSTRUCT' AND cn.locale = 'en' AND cn.concept_name_type = 'FULLY_SPECIFIED' AND cn.voided = 0
        WHERE e.encounter_type IN (5, 11)
    """
    update_orders_creator_sql = """
        UPDATE orders
        SET creator = 1
        WHERE creator NOT IN (SELECT user_id FROM users)
    """
    update_orders_voided_by_sql = """
        UPDATE orders
        SET voided_by = 1
        WHERE voided_by IS NOT NULL AND voided_by NOT IN (SELECT user_id FROM users)
    """
    with target_engine.connect() as conn:
        info("Loading data for orders table...")
        conn.execute(text(f"SET FOREIGN_KEY_CHECKS = 0"))
        conn.commit()
        conn.execute(text(truncate_orders_sql))
        conn.execute(text(insert_seed_order_sql))
        conn.execute(text(insert_encounter_orders_sql))
        conn.execute(text(update_orders_creator_sql))
        conn.execute(text(update_orders_voided_by_sql))
        conn.commit()
        conn.execute(text(f"SET FOREIGN_KEY_CHECKS = 1"))
        conn.commit()
    info(f"Load orders completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_orders_group():
    start_time = time.time()
    load_order_type()
    load_order()
    info(f"Load orders group completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")