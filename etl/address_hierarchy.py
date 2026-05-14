import time

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_address_hierarchy_level(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if drop_create:
        create_address_hierarchy_level_table(target_engine, drop_create=drop_create)

    info("Fetching data from source address_hierarchy_level table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _address_hierarchy_level"))
        target_conn.commit()

    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM address_hierarchy_level")).fetchall()

    if source_data:
        info(f"Inserting {len(source_data)} records into target _address_hierarchy_level table...")
        insert_query = text("""
        INSERT INTO _address_hierarchy_level (address_hierarchy_level_id, name, parent_level_id, address_field, uuid, required)
        VALUES (:address_hierarchy_level_id, :name, :parent_level_id, :address_field, :uuid, :required)
        """)

        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "address_hierarchy_level_id": row.address_hierarchy_level_id, "name": row.name, "parent_level_id": row.parent_level_id, "address_field": row.address_field, "uuid": row.uuid, "required": row.required
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source address_hierarchy_level table.")

def extract_address_hierarchy_entry(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if drop_create:
        create_address_hierarchy_entry_table(target_engine, drop_create=drop_create)

    info("Fetching data from source address_hierarchy_entry table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _address_hierarchy_entry"))
        target_conn.commit()

    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM address_hierarchy_entry")).fetchall()

    if source_data:
        info(f"Inserting {len(source_data)} records into target _address_hierarchy_entry table...")
        insert_query = text("""
        INSERT INTO _address_hierarchy_entry (address_hierarchy_entry_id, name, level_id, parent_id, user_generated_id, latitude, longitude, elevation, uuid)
        VALUES (:address_hierarchy_entry_id, :name, :level_id, :parent_id, :user_generated_id, :latitude, :longitude, :elevation, :uuid)
        """)
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "address_hierarchy_entry_id": row.address_hierarchy_entry_id, "name": row.name, "level_id": row.level_id, "parent_id": row.parent_id, "user_generated_id": row.user_generated_id, "latitude": row.latitude, "longitude": row.longitude, "elevation": row.elevation, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source address_hierarchy_entry table.")

def extract_address_hierarchy_group(drop_create):
    start_time = time.time()
    extract_address_hierarchy_level(drop_create=drop_create)
    info("Extracting data from source database and inserting into target database...")
    extract_address_hierarchy_entry(drop_create=drop_create)
    info("Address hierarchy level and entry tables created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")

##### Loading functions #####
def load_address_hierarchy_level():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO address_hierarchy_level (address_hierarchy_level_id, name, parent_level_id, address_field, uuid, required)
    SELECT address_hierarchy_level_id, name, parent_level_id, address_field, uuid, required FROM _address_hierarchy_level
    """
    with target_engine.connect() as conn:
        info("Loading data for address_hierarchy_level table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load address_hierarchy_level completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_address_hierarchy_entry():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO address_hierarchy_entry (address_hierarchy_entry_id, name, level_id, parent_id, user_generated_id, latitude, longitude, elevation, uuid)
    SELECT address_hierarchy_entry_id, name, level_id, parent_id, user_generated_id, latitude, longitude, elevation, uuid FROM _address_hierarchy_entry
    """
    with target_engine.connect() as conn:
        info("Loading data for address_hierarchy_entry table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load address_hierarchy_entry completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_address_hierarchy_group():
    load_address_hierarchy_level()
    load_address_hierarchy_entry()

