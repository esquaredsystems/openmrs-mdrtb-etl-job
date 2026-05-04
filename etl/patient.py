import time

from sqlalchemy import text

from config.config import BATCH_SIZE
from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning

##### Extraction functions #####
def extract_patient(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_patient_table(target_engine, drop_create=drop_create)
    info("Fetching data from source patient table...")
    insert_query = text("INSERT IGNORE INTO _patient (patient_id, tribe, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) VALUES (:patient_id, :tribe, :creator, :date_created, :changed_by, :date_changed, :voided, :voided_by, :date_voided, :void_reason)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM patient"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "patient_id": row.patient_id, "tribe": row.tribe, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _patient table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _patient table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source patient table.")

def extract_patient_identifier(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_patient_identifier_table(target_engine, drop_create=drop_create)
    info("Fetching data from source patient_identifier table...")
    insert_query = text("INSERT IGNORE INTO _patient_identifier (patient_identifier_id, patient_id, identifier, identifier_type, preferred, location_id, creator, date_created, voided, voided_by, date_voided, void_reason, uuid) VALUES (:patient_identifier_id, :patient_id, :identifier, :identifier_type, :preferred, :location_id, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM patient_identifier"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "patient_identifier_id": row.patient_identifier_id, "patient_id": row.patient_id, "identifier": row.identifier, "identifier_type": row.identifier_type, "preferred": row.preferred, "location_id": row.location_id, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _patient_identifier table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _patient_identifier table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source patient_identifier table.")

def extract_patient_identifier_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_patient_identifier_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source patient_identifier_type table...")
    insert_query = text("INSERT IGNORE INTO _patient_identifier_type (patient_identifier_type_id, name, description, format, check_digit, creator, date_created, required, format_description, validator, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:patient_identifier_type_id, :name, :description, :format, :check_digit, :creator, :date_created, :required, :format_description, :validator, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM patient_identifier_type")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _patient_identifier_type table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "patient_identifier_type_id": row.patient_identifier_type_id, "name": row.name, "description": row.description, "format": row.format, "check_digit": row.check_digit, "creator": row.creator, "date_created": row.date_created, "required": row.required, "format_description": row.format_description, "validator": row.validator, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source patient_identifier_type table.")

def extract_patient_program(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_patient_program_table(target_engine, drop_create=drop_create)
    info("Fetching data from source patient_program table...")
    insert_query = text("INSERT IGNORE INTO _patient_program (patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (:patient_program_id, :patient_id, :program_id, :date_enrolled, :date_completed, :location_id, :creator, :date_created, :changed_by, :date_changed, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM patient_program"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "patient_program_id": row.patient_program_id, "patient_id": row.patient_id, "program_id": row.program_id, "date_enrolled": row.date_enrolled, "date_completed": row.date_completed, "location_id": row.location_id, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _patient_program table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _patient_program table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source patient_program table.")

def extract_person(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_person_table(target_engine, drop_create=drop_create)
    info("Fetching data from source person table...")
    insert_query = text("INSERT IGNORE INTO _person (person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (:person_id, :gender, :birthdate, :birthdate_estimated, :dead, :death_date, :cause_of_death, :creator, :date_created, :changed_by, :date_changed, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM person"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "person_id": row.person_id, "gender": row.gender, "birthdate": row.birthdate, "birthdate_estimated": row.birthdate_estimated, "dead": row.dead, "death_date": row.death_date, "cause_of_death": row.cause_of_death, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _person table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _person table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source person table.")

def extract_person_address(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_person_address_table(target_engine, drop_create=drop_create)
    info("Fetching data from source person_address table...")
    insert_query = text("INSERT IGNORE INTO _person_address (person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, uuid) VALUES (:person_address_id, :person_id, :preferred, :address1, :address2, :city_village, :state_province, :postal_code, :country, :latitude, :longitude, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :county_district, :address3, :address4, :address5, :address6, :uuid)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM person_address"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "person_address_id": row.person_address_id, "person_id": row.person_id, "preferred": row.preferred, "address1": row.address1, "address2": row.address2, "city_village": row.city_village, "state_province": row.state_province, "postal_code": row.postal_code, "country": row.country, "latitude": row.latitude, "longitude": row.longitude, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "county_district": row.county_district, "address3": row.neighborhood_cell, "address4": row.region, "address5": row.subregion, "address6": row.township_division, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _person_address table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _person_address table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source person_address table.")

def extract_person_attribute(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_person_attribute_table(target_engine, drop_create=drop_create)
    info("Fetching data from source person_attribute table...")
    insert_query = text("INSERT IGNORE INTO _person_attribute (person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (:person_attribute_id, :person_id, :value, :person_attribute_type_id, :creator, :date_created, :changed_by, :date_changed, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM person_attribute"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "person_attribute_id": row.person_attribute_id, "person_id": row.person_id, "value": row.value, "person_attribute_type_id": row.person_attribute_type_id, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _person_attribute table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _person_attribute table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source person_attribute table.")

def extract_person_attribute_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_person_attribute_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source person_attribute_type table...")
    insert_query = text("INSERT IGNORE INTO _person_attribute_type (person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid) VALUES (:person_attribute_type_id, :name, :description, :format, :foreign_key, :searchable, :creator, :date_created, :changed_by, :date_changed, :retired, :retired_by, :date_retired, :retire_reason, :edit_privilege, :sort_weight, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM person_attribute_type")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _person_attribute_type table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "person_attribute_type_id": row.person_attribute_type_id, "name": row.name, "description": row.description, "format": row.format, "foreign_key": row.foreign_key, "searchable": row.searchable, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "edit_privilege": row.edit_privilege, "sort_weight": row.sort_weight, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source person_attribute_type table.")

def extract_person_name(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_person_name_table(target_engine, drop_create=drop_create)
    info("Fetching data from source person_name table...")
    insert_query = text("INSERT IGNORE INTO _person_name (person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) VALUES (:person_name_id, :preferred, :person_id, :prefix, :given_name, :middle_name, :family_name_prefix, :family_name, :family_name2, :family_name_suffix, :degree, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :changed_by, :date_changed, :uuid)")
    
    with source_engine.connect() as source_conn:
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM person_name"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "person_name_id": row.person_name_id, "preferred": row.preferred, "person_id": row.person_id, "prefix": row.prefix, "given_name": row.given_name, "middle_name": row.middle_name, "family_name_prefix": row.family_name_prefix, "family_name": row.family_name, "family_name2": row.family_name2, "family_name_suffix": row.family_name_suffix, "degree": row.degree, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _person_name table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _person_name table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source person_name table.")

def extract_patient_group(drop_create):
    start_time = time.time()
    extract_patient(drop_create=drop_create)
    info("Patient table created successfully")
    extract_patient_identifier(drop_create=drop_create)
    info("Patient identifier table created successfully")
    extract_patient_identifier_type(drop_create=drop_create)
    info("Patient identifier type table created successfully")
    extract_patient_program(drop_create=drop_create)
    info("Patient program table created successfully")
    extract_person(drop_create=drop_create)
    info("Person table created successfully")
    extract_person_address(drop_create=drop_create)
    info("Person address table created successfully")
    extract_person_attribute(drop_create=drop_create)
    info("Person attribute table created successfully")
    extract_person_attribute_type(drop_create=drop_create)
    info("Person attribute type table created successfully")
    extract_person_name(drop_create=drop_create)
    info(f"Person name table created successfully (Time: {time.time() - start_time:.2f} seconds)")

##### Loading functions #####
def load_patient_group():
    pass

