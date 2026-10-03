import time

import numpy as np
import pandas as pd
from sqlalchemy import bindparam

from config.database import get_target_engine
from models.schema_models import *
from utils.helpers import get_location_data
from utils.logger import info, warning


##### Extraction functions #####
def extract_location(drop_create=False):
    target_engine = get_target_engine()
    if drop_create or not table_exists(target_engine, '_location'):
        create_location_table(target_engine, drop_create=drop_create)
    info("Fetching data from locations.xlsx location sheet...")

    # Read from Excel
    df = get_location_data()
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


# Columns refreshed from locations.xlsx when a location already exists. location_id and uuid identify
# the row and are never rewritten.
LOCATION_UPSERT_COLUMNS = [
    "name", "description", "country", "state_province", "county_district", "creator", "date_created",
    "retired", "retired_by", "date_retired", "retire_reason", "parent_location",
]


def _on_duplicate_update(columns):
    return " ON DUPLICATE KEY UPDATE " + ", ".join(f"{c} = VALUES({c})" for c in columns)


def build_location_upserts():
    """
    Upserts for the location table; locations.xlsx is the source of truth, so a rerun applies edits made there.
    The level statements set the real parent. The catch-all statements at the end match many of the same
    rows with parent_location = NULL, so they refresh every column except parent_location.
    """
    columns = "(location_id, name, description, country, state_province, county_district, creator, date_created, retired, retired_by, date_retired, retire_reason, parent_location, uuid)"
    upsert = _on_duplicate_update(LOCATION_UPSERT_COLUMNS)
    upsert_keep_parent = _on_duplicate_update([c for c in LOCATION_UPSERT_COLUMNS if c != "parent_location"])
    return [
        # UPSERT Tajikistan (parent location)
        f"INSERT INTO location {columns} select location_id, name, description, 'Точикистон (Таджикистан)' as country, state_province, county_district, 1 as creator, date_created, retired, retired_by, date_retired, retire_reason, parent_location, uuid from _location l where location_id = 1 ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), country = VALUES(country), state_province = VALUES(state_province), county_district = VALUES(county_district), creator = VALUES(creator), date_created = VALUES(date_created), retired = VALUES(retired), retired_by = VALUES(retired_by), date_retired = VALUES(date_retired), retire_reason = VALUES(retire_reason), parent_location = VALUES(parent_location), uuid = VALUES(uuid)",
        # Upsert all Regions
        f"INSERT INTO location {columns} select location_id, name, description, 'Точикистон (Таджикистан)' as country, state_province, county_district, 1 as creator, date_created, retired, retired_by, date_retired, retire_reason, parent_location, uuid from _location l where `level` = 'REGION'{upsert}",
        # Upsert all Subregions
        f"INSERT INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, NULL, NULL, NULL, p.location_id as parent_location, l.uuid from _location l inner join _location as p on p.location_id = l.parent_location where l.`level` = 'SUBREGION'{upsert}",
        # Upsert all Districts
        f"INSERT INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, p.location_id as parent_location, l.uuid from _location l inner join _location as p on p.location_id = l.parent_location where l.`level` = 'DISTRICT'{upsert}",
        # Upsert all Facilities
        f"INSERT INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, p.location_id as parent_location, l.uuid from _location l inner join _location as p on p.location_id = l.parent_location where l.`level` = 'FACILITY'{upsert}",
        # Upsert all locations without parent
        f"INSERT INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, NULL, l.uuid from _location l where l.parent_location is null and l.parent_location not in (select location_id from location){upsert_keep_parent}",
        # Upsert all retired locations without parent
        f"INSERT INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, NULL, l.uuid from _location l where l.parent_location is null and l.retired = 1{upsert_keep_parent}",
        # Upsert all retired locations with parent
        f"INSERT INTO location {columns} select l.location_id, l.name, l.description, 'Точикистон (Таджикистан)' as country, l.state_province, l.county_district, 1 as creator, l.date_created, l.retired, l.retired_by, l.date_retired, l.retire_reason, NULL, l.uuid from _location l where l.parent_location is not null and l.retired = 1{upsert_keep_parent}",
    ]


def load_location():
    start_time = time.time()
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading data for location table...")
        for i, query in enumerate(build_location_upserts(), 1):
            conn.execute(text(query))
            conn.commit()
    info(f"Load location completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


# Levels that get a LEVEL location attribute. value_reference is the level itself,
# which is why one statement replaces what used to be four near-identical queries.
LEVEL_ATTRIBUTE_VALUES = ["REGION", "SUBREGION", "DISTRICT", "FACILITY"]


def build_location_attribute_update():
    """
    Brings the active LEVEL attribute in line with locations.xlsx, in place (no void and re-insert).
    location_attribute has no unique key on (location_id, attribute_type_id), so this cannot be an ON DUPLICATE KEY UPDATE.
    """
    return text(
        "UPDATE location_attribute AS la "
        "INNER JOIN location_attribute_type AS lat "
        "    ON lat.location_attribute_type_id = la.attribute_type_id AND lat.name = 'LEVEL' "
        "INNER JOIN _location AS l ON l.location_id = la.location_id "
        "SET la.value_reference = l.level, la.changed_by = 1, la.date_changed = current_timestamp() "
        "WHERE la.voided = 0 "
        "AND l.level IN :levels "
        "AND la.value_reference <> l.level"
    ).bindparams(bindparam("levels", expanding=True))


def build_location_attribute_insert():
    """
    Rerun-safe INSERT for the LEVEL location attribute, for locations that have no active LEVEL yet.
    Existing ones are corrected by build_location_attribute_update().
    """
    columns = (
        "(location_id, attribute_type_id, value_reference, uuid, creator, date_created) "
    )
    return text(
        f"INSERT IGNORE INTO location_attribute {columns}"
        "SELECT l.location_id, lat.location_attribute_type_id, l.level, UUID(), 1, current_timestamp() "
        "FROM _location AS l "
        "INNER JOIN location_attribute_type AS lat ON lat.name = 'LEVEL' "
        "WHERE l.level IN :levels "
        "AND NOT EXISTS ("
        "    SELECT 1 FROM location_attribute AS existing "
        "    WHERE existing.location_id = l.location_id "
        "    AND existing.attribute_type_id = lat.location_attribute_type_id "
        "    AND existing.voided = 0"
        ")"
    ).bindparams(bindparam("levels", expanding=True))


def load_location_attribute():
    start_time = time.time()
    target_engine = get_target_engine()
    params = {"levels": LEVEL_ATTRIBUTE_VALUES}
    with target_engine.connect() as conn:
        info("Loading data for location_attribute table...")
        # One transaction: the update and the insert are committed together
        updated = conn.execute(build_location_attribute_update(), params).rowcount
        inserted = conn.execute(build_location_attribute_insert(), params).rowcount
        conn.commit()
        info(f"LEVEL attribute(s): {updated} updated, {inserted} inserted")
    info(f"Load location_attribute completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def verify_location_attribute():
    """
    Warn when a staged location has no active LEVEL attribute, or one that differs from locations.xlsx.
    The web app builds its Region/District/Facility dropdowns from LEVEL, so a gap here shows up as empty dropdowns.
    After load_location_attribute() this should report nothing.
    """
    target_engine = get_target_engine()
    query = text(
        "SELECT l.location_id, l.name, l.level, la.value_reference "
        "FROM _location AS l "
        "INNER JOIN location_attribute_type AS lat ON lat.name = 'LEVEL' "
        "LEFT JOIN location_attribute AS la ON la.location_id = l.location_id "
        "    AND la.attribute_type_id = lat.location_attribute_type_id AND la.voided = 0 "
        "WHERE l.level IN :levels AND (la.value_reference IS NULL OR la.value_reference <> l.level)"
    ).bindparams(bindparam("levels", expanding=True))
    with target_engine.connect() as conn:
        rows = conn.execute(query, {"levels": LEVEL_ATTRIBUTE_VALUES}).fetchall()
    missing = [r for r in rows if r.value_reference is None]
    stale = [r for r in rows if r.value_reference is not None]
    if missing:
        warning(f"{len(missing)} location(s) have no LEVEL attribute, e.g. {[(r.location_id, r.level) for r in missing[:10]]}")
    if stale:
        warning(f"{len(stale)} location(s) have a LEVEL that differs from locations.xlsx, e.g. {[(r.location_id, r.value_reference, r.level) for r in stale[:10]]}")
    if not rows:
        info("All staged locations have the expected LEVEL attribute")


##### Loading functions #####
def load_location_group():
    load_location_attribute_type()
    load_location()
    load_location_attribute()
    verify_location_attribute()

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
