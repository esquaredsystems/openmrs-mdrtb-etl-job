import time

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_global_property(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_global_property_table(target_engine, drop_create=drop_create)
    info("Fetching data from source global_property table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _global_property"))
        target_conn.commit()

    insert_query = text("INSERT INTO _global_property (property, property_value, description, uuid) VALUES (:property, :property_value, :description, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM global_property")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _global_property table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "property": row.property, "property_value": row.property_value, "description": row.description, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source global_property table.")

def extract_note(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_note_table(target_engine, drop_create=drop_create)
    info("Fetching data from source note table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _note"))
        target_conn.commit()

    insert_query = text("INSERT INTO _note (note_id, note_type, patient_id, obs_id, encounter_id, text, priority, parent, creator, date_created, changed_by, date_changed, uuid) VALUES (:note_id, :note_type, :patient_id, :obs_id, :encounter_id, :text, :priority, :parent, :creator, :date_created, :changed_by, :date_changed, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM note")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _note table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "note_id": row.note_id, "note_type": row.note_type, "patient_id": row.patient_id, "obs_id": row.obs_id, "encounter_id": row.encounter_id, "text": row.text, "priority": row.priority, "parent": row.parent, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source note table.")

def extract_notification_alert(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_notification_alert_table(target_engine, drop_create=drop_create)
    info("Fetching data from source notification_alert table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _notification_alert"))
        target_conn.commit()

    insert_query = text("INSERT INTO _notification_alert (alert_id, text, satisfied_by_any, alert_read, date_to_expire, creator, date_created, changed_by, date_changed, uuid) VALUES (:alert_id, :text, :satisfied_by_any, :alert_read, :date_to_expire, :creator, :date_created, :changed_by, :date_changed, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM notification_alert")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _notification_alert table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "alert_id": row.alert_id, "text": row.text, "satisfied_by_any": row.satisfied_by_any, "alert_read": row.alert_read, "date_to_expire": row.date_to_expire, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source notification_alert table.")

def extract_notification_alert_recipient(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_notification_alert_recipient_table(target_engine, drop_create=drop_create)
    info("Fetching data from source notification_alert_recipient table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _notification_alert_recipient"))
        target_conn.commit()

    insert_query = text("INSERT INTO _notification_alert_recipient (alert_id, user_id, alert_read, date_changed, uuid) VALUES (:alert_id, :user_id, :alert_read, :date_changed, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM notification_alert_recipient")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _notification_alert_recipient table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "alert_id": row.alert_id, "user_id": row.user_id, "alert_read": row.alert_read, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source notification_alert_recipient table.")

def extract_relationship_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_relationship_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source relationship_type table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _relationship_type"))
        target_conn.commit()

    insert_query = text("INSERT INTO _relationship_type (relationship_type_id, a_is_to_b, b_is_to_a, preferred, weight, description, creator, date_created, uuid, retired, retired_by, date_retired, retire_reason) VALUES (:relationship_type_id, :a_is_to_b, :b_is_to_a, :preferred, :weight, :description, :creator, :date_created, :uuid, :retired, :retired_by, :date_retired, :retire_reason)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM relationship_type")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _relationship_type table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "relationship_type_id": row.relationship_type_id, "a_is_to_b": row.a_is_to_b, "b_is_to_a": row.b_is_to_a, "preferred": row.preferred, "weight": row.weight, "description": row.description, "creator": row.creator, "date_created": row.date_created, "uuid": row.uuid, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source relationship_type table.")

def extract_misc_group(drop_create):
    start_time = time.time()
    extract_global_property(drop_create=drop_create)
    info("Global property table created successfully")
    extract_relationship_type(drop_create=drop_create)
    info("Relationship type table created successfully")
    extract_note(drop_create=drop_create)
    info("Note table created successfully")
    extract_notification_alert(drop_create=drop_create)
    info("Notification alert table created successfully")
    extract_notification_alert_recipient(drop_create=drop_create)
    info("Notification alert recipient table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")

##### Loading functions #####
def backup_global_property():
    start_time = time.time()
    target_engine = get_target_engine()
    # Create backup table with drop-create
    create_global_property_bak_table(target_engine, drop_create=True)

    # Copy current global_property data to backup
    copy_query = """
    INSERT INTO _global_property_bak (property, property_value, description, uuid, datatype, datatype_config, preferred_handler, handler_config) 
    SELECT property, property_value, description, uuid, datatype, datatype_config, preferred_handler, handler_config FROM global_property
    """
    with target_engine.connect() as conn:
        info("Creating backup of global_property table...")
        conn.execute(text(copy_query))
        conn.commit()
    info(f"Backup global_property completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_global_property():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO global_property (property, property_value, description, uuid, datatype, datatype_config, preferred_handler, handler_config) 
    SELECT property, property_value, description, uuid, datatype, datatype_config, preferred_handler, handler_config FROM _global_property
    """
    with target_engine.connect() as conn:
        info("Loading data for global_property table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load global_property completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_relationship_type():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO relationship_type (relationship_type_id, a_is_to_b, b_is_to_a, preferred, weight, description, creator, date_created, uuid, retired, retired_by, date_retired, retire_reason) 
    SELECT relationship_type_id, a_is_to_b, b_is_to_a, preferred, weight, description, creator, date_created, uuid, retired, retired_by, date_retired, retire_reason FROM _relationship_type
    """
    with target_engine.connect() as conn:
        info("Loading data for relationship_type table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load relationship_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_misc_group():
    backup_global_property()
    load_global_property()
    load_relationship_type()
