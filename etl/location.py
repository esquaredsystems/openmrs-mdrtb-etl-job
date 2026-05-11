import time

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.helpers import read_excel_sheet
from utils.logger import info, warning

##### Extraction functions #####
def extract_location(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_location_table(target_engine, drop_create=drop_create)
    info("Fetching data from locations.xlsx location sheet...")

    # Read from Excel
    df = read_excel_sheet('locations.xlsx', 'locations')
    if not df.empty:
        # Truncate the target table
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _location"))
            target_conn.commit()

        # Process the DataFrame (handle NaN/NaT values)
        df = df.replace({np.nan: None, pd.NaT: None})

        # Prepare data for insertion (matching the _location table columns as described)
        source_data = df[[
            "location_id", "name", "level", "parent_location", "description", "state_province", "county_district", "date_created", "retired", "retired_by", "date_retired", "retire_reason", "uuid"
        ]].to_dict(orient='records')

        # Insert into target
        info(f"Inserting {len(source_data)} records from locations.xlsx location sheet into target _location table...")
        insert_query = text(
            "INSERT INTO _location (location_id, name, level, parent_location, description, state_province, county_district, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:location_id, :name, :level, :parent_location, :description, :state_province, :county_district, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import from locations.xlsx location sheet completed successfully.")
    else:
        warning("No data found in locations.xlsx location sheet.")


def extract_location_group(drop_create):
    start_time = time.time()
    extract_location(drop_create=drop_create)
    info("Location table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")


##### Load functions #####
def load_location_attribute_type():
    start_time = time.time()
    target_engine = get_target_engine()
    insert_queries = [
        "INSERT IGNORE INTO location_attribute_type (name,description,datatype,datatype_config,preferred_handler,handler_config,min_occurs,max_occurs,creator,date_created,changed_by,date_changed,retired,retired_by,date_retired,retire_reason,uuid) VALUES ('LEVEL','The geographical hierarchy level of the location','org.openmrs.customdatatype.datatype.SpecifiedTextOptionsDatatype',NULL,'org.openmrs.web.attribute.handler.SpecifiedTextOptionsDropdownHandler','UNKNOWN,REGION,SUBREGION,DISTRICT,FACILITY',0,1,1,'2023-01-01',1,'2023-01-17 18:50:28',0,NULL,NULL,NULL,'6b738ed1-78b3-4cdb-81f6-7fdc5da20a3d');",
    ]
    with target_engine.connect() as conn:
        info("Loading data for location_attribute_type table...")
        for i, query in enumerate(insert_queries, 1):
            conn.execute(text(query))
            conn.commit()
    info(f"Load location_attribute_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_location():
    start_time = time.time()
    target_engine = get_target_engine()
    columns = "(location_id, name, description, country, state_province, county_district, creator, date_created, retired, retired_by, date_retired, retire_reason, parent_location, uuid)"
    insert_queries = [
        # UPSERT Tajikistan (parent location)
        f"INSERT INTO location {columns} select location_id, name, description, 'Точикистон (Таджикистан)' as country, state_province, county_district, 1 as creator, date_created, retired, retired_by, date_retired, retire_reason, parent_location, uuid from _location l where location_id = 1 ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), country = VALUES(country), state_province = VALUES(state_province), county_district = VALUES(county_district), creator = VALUES(creator), date_created = VALUES(date_created), retired = VALUES(retired), retired_by = VALUES(retired_by), date_retired = VALUES(date_retired), retire_reason = VALUES(retire_reason), parent_location = VALUES(parent_location), uuid = VALUES(uuid)",
        # Insert all Regions
        f"INSERT IGNORE INTO location {columns} select location_id, name, description, 'Точикистон (Таджикистан)' as country, state_province, county_district, 1 as creator, date_created, retired, retired_by, date_retired, retire_reason, parent_location, uuid from _location l where `level` = 'REGION'",
        # Insert all Subregions
        f"INSERT IGNORE INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, NULL, NULL, NULL, p.location_id as parent_location, l.uuid from _location l inner join _location as p on p.location_id = l.parent_location where l.`level` = 'SUBREGION'",
        # Insert all Districts
        f"INSERT IGNORE INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, p.location_id as parent_location, l.uuid from _location l inner join _location as p on p.location_id = l.parent_location where l.`level` = 'DISTRICT'",
        # Insert all Facilities
        f"INSERT IGNORE INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, p.location_id as parent_location, l.uuid from _location l inner join _location as p on p.location_id = l.parent_location where l.`level` = 'FACILITY'",
        # Insert all locations without parent
        f"INSERT IGNORE INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, NULL, l.uuid from _location l where l.parent_location is null and l.parent_location not in (select location_id from location)",
        # Insert all retired locations without parent
        f"INSERT IGNORE INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, NULL, l.uuid from _location l where l.parent_location is null and l.retired = 1",
    ]
    with target_engine.connect() as conn:
        info("Loading data for location table...")
        for i, query in enumerate(insert_queries, 1):
            conn.execute(text(query))
            conn.commit()
    info(f"Load location completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_location_attribute():
    start_time = time.time()
    target_engine = get_target_engine()
    columns = "(location_id, attribute_type_id, value_reference, uuid, creator, date_created) "
    insert_queries = [
        f"INSERT IGNORE INTO location_attribute {columns} select l.location_id, lat.location_attribute_type_id as attribute_type_id, 'REGION', UUID(), 1 as creator, current_timestamp() as date_created from _location as l inner join location_attribute_type as lat on lat.name = 'LEVEL' where l.level = 'REGION'",
        f"INSERT IGNORE INTO location_attribute {columns} select l.location_id, lat.location_attribute_type_id as attribute_type_id, 'SUBREGION', UUID(), 1 as creator, current_timestamp() as date_created from _location as l inner join location_attribute_type as lat on lat.name = 'LEVEL' where l.level = 'SUBREGION'",
        f"INSERT IGNORE INTO location_attribute {columns} select l.location_id, lat.location_attribute_type_id as attribute_type_id, 'DISTRICT', UUID(), 1 as creator, current_timestamp() as date_created from _location as l inner join location_attribute_type as lat on lat.name = 'LEVEL' where l.level = 'DISTRICT'",
        f"INSERT IGNORE INTO location_attribute {columns} select l.location_id, lat.location_attribute_type_id as attribute_type_id, 'FACILITY', UUID(), 1 as creator, current_timestamp() as date_created from _location as l inner join location_attribute_type as lat on lat.name = 'LEVEL' where l.level = 'FACILITY'",
        f"INSERT IGNORE INTO location_attribute {columns} select l.location_id, lat.location_attribute_type_id as attribute_type_id, 'DISTRICT', UUID(), 1 as creator, current_timestamp() as date_created from _location as l inner join location_attribute_type as lat on lat.name = 'LEVEL' where l.level = 'DISTRICT' and l.parent_location not in (select location_id from location)",
    ]
    with target_engine.connect() as conn:
        info("Loading data for location_attribute table...")
        for i, query in enumerate(insert_queries, 1):
            conn.execute(text(query))
            conn.commit()
    info(f"Load location_attribute completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


##### Loading functions #####
def load_location_group():
    load_location_attribute_type()
    load_location()
    load_location_attribute()

    # Load location tags and tag map
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading location tags...")
        conn.execute(text("INSERT IGNORE INTO location_tag (name,description,creator,date_created,retired,retired_by,date_retired,retire_reason,uuid,changed_by,date_changed) values ('DOTS Facility','Location allows DOTS patient enrollment',1,'2023-01-17 21:39:09',0,NULL,NULL,NULL,'cabd6ef3-db2d-4e4f-9136-8beb70360ac6',1,'2023-01-17 21:46:27'), ('MDRTB Facility','Location allows MDR-TB patient enrollment',1,'2023-01-17 21:39:31',0,NULL,NULL,NULL,'53ceaf16-22ff-41a8-be31-8e43651c70e5',1,'2023-01-17 21:46:36'), ('Login Location','Allow user to Login from this location',1,'2023-01-17 21:41:10',0,NULL,NULL,NULL,'a68911ad-21a2-4590-9967-c2bdaf4a22c2',1,'2023-01-17 21:43:24'), ('Admission Location','Patients may only be admitted to inpatient care',1,'2023-01-17 21:41:35',0,NULL,NULL,NULL,'6af78c25-1246-4b04-9c38-0d8d274a4893',NULL,NULL), ('Transfer Location','Patients can be transferred into this location',1,'2023-01-17 21:42:10',0,NULL,NULL,NULL,'7711ecfc-8a58-44cf-b670-8783b8e633d5',1,'2023-01-17 21:43:41'), ('Laboratory','If this is a Laboratory',1,'2023-01-17 21:42:55',0,NULL,NULL,NULL,'663f8ed3-18cf-48e9-a887-a4665fbd249a',NULL,NULL), ('Culture Lab','This is a Laboratory providing Culture tests',1,'2023-01-17 21:44:14',0,NULL,NULL,NULL,'4703369f-3d6f-433f-b31c-9cf46035b96b',NULL,NULL), ('Prison','This location represents a Prison or Jail',1,'2023-01-17 21:46:54',0,NULL,NULL,NULL,'83922a9c-3a75-4acd-bdaa-24329611230f',NULL,NULL)"))
        conn.commit()
        info("Loading location tag map...")
        conn.execute(text("INSERT IGNORE INTO location_tag_map (location_id, location_tag_id) select 1, location_tag_id FROM location_tag"))
        conn.commit()

    info("Load location group completed successfully.")
