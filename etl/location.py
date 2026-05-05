import time

from sqlalchemy import text

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning

##### Extraction functions #####
def extract_location(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_location_table(target_engine, drop_create=drop_create)
    info("Fetching data from source location table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _location"))
        target_conn.commit()

    insert_query = text("INSERT INTO _location (location_id, name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, county_district, address3, address4, address5, address6, retired, retired_by, date_retired, retire_reason, parent_location, uuid) VALUES (:location_id, :name, :description, :address1, :address2, :city_village, :state_province, :postal_code, :country, :latitude, :longitude, :creator, :date_created, :county_district, :address3, :address4, :address5, :address6, :retired, :retired_by, :date_retired, :retire_reason, :parent_location, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM location")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _location table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "location_id": row.location_id, "name": row.name, "description": row.description, "address1": row.address1, "address2": row.address2, "city_village": row.city_village, "state_province": row.state_province, "postal_code": row.postal_code, "country": row.country, "latitude": row.latitude, "longitude": row.longitude, "creator": row.creator, "date_created": row.date_created, "county_district": row.county_district, "address3": row.neighborhood_cell, "address4": row.region, "address5": row.subregion, "address6": row.township_division, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "parent_location": row.parent_location, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source location table.")

def extract_location_group(drop_create):
    start_time = time.time()
    extract_location(drop_create=drop_create)
    info(f"Location table created successfully (Time: {time.time() - start_time:.2f} seconds)")

##### Loading functions #####
def load_location_group():
    pass

