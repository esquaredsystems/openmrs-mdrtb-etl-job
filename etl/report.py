import time

from sqlalchemy import text

from config.config import BATCH_SIZE
from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning

##### Extraction functions #####
def extract_report_object(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_report_object_table(target_engine, drop_create=drop_create)
    info("Fetching data from source report_object table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _report_object"))
        target_conn.commit()

    insert_query = text("INSERT INTO _report_object (report_object_id, name, description, report_object_type, report_object_sub_type, xml_data, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (:report_object_id, :name, :description, :report_object_type, :report_object_sub_type, :xml_data, :creator, :date_created, :changed_by, :date_changed, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM report_object")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _report_object table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "report_object_id": row.report_object_id, "name": row.name, "description": row.description, "report_object_type": row.report_object_type, "report_object_sub_type": row.report_object_sub_type, "xml_data": row.xml_data, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source report_object table.")

def extract_report_schema_xml(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_report_schema_xml_table(target_engine, drop_create=drop_create)
    info("Fetching data from source report_schema_xml table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _report_schema_xml"))
        target_conn.commit()

    insert_query = text("INSERT INTO _report_schema_xml (report_schema_id, name, description, xml_data, uuid) VALUES (:report_schema_id, :name, :description, :xml_data, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM report_schema_xml")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _report_schema_xml table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "report_schema_id": row.report_schema_id, "name": row.name, "description": row.description, "xml_data": row.xml_data, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source report_schema_xml table.")

def extract_serialized_object(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_serialized_object_table(target_engine, drop_create=drop_create)
    info("Fetching data from source serialized_object table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _serialized_object"))
        target_conn.commit()

    insert_query = text("INSERT INTO _serialized_object (serialized_object_id, name, description, type, subtype, serialization_class, serialized_data, date_created, creator, date_changed, changed_by, retired, date_retired, retired_by, retire_reason, uuid) VALUES (:serialized_object_id, :name, :description, :type, :subtype, :serialization_class, :serialized_data, :date_created, :creator, :date_changed, :changed_by, :retired, :date_retired, :retired_by, :retire_reason, :uuid)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM serialized_object"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "serialized_object_id": row.serialized_object_id, "name": row.name, "description": row.description, "type": row.type, "subtype": row.subtype, "serialization_class": row.serialization_class, "serialized_data": row.serialized_data, "date_created": row.date_created, "creator": row.creator, "date_changed": row.date_changed, "changed_by": row.changed_by, "retired": row.retired, "date_retired": row.date_retired, "retired_by": row.retired_by, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _serialized_object table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _serialized_object table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source serialized_object table.")

def extract_report_group(drop_create):
    start_time = time.time()
    extract_report_object(drop_create=drop_create)
    info("Report object table created successfully")
    extract_report_schema_xml(drop_create=drop_create)
    info("Report schema XML table created successfully")
    extract_serialized_object(drop_create=drop_create)
    info(f"Serialized object table created successfully (Time: {time.time() - start_time:.2f} seconds)")

##### Loading functions #####
def load_report_group():
    pass

