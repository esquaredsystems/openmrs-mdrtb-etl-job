import numpy as np
import pandas as pd
from uuid import uuid4
from config.database import get_source_engine, get_target_engine
from config.config import BATCH_SIZE
from utils.logger import info, warning
from utils.helpers import read_excel_sheet
from models.schema_models import *


def extract_address_hierarchy_entry(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if drop_create:
        create_address_hierarchy_entry_table(target_engine, drop_create=drop_create)

    info("Fetching data from source address_hierarchy_entry table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM address_hierarchy_entry")).fetchall()

    if source_data:
        info(f"Inserting {len(source_data)} records into target _address_hierarchy_entry table...")
        insert_query = text("""
        INSERT IGNORE INTO _address_hierarchy_entry (address_hierarchy_entry_id, name, level_id, parent_id, user_generated_id, latitude, longitude, elevation, uuid)
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

def extract_address_hierarchy_level(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if drop_create:
        create_address_hierarchy_level_table(target_engine, drop_create=drop_create)

    info("Fetching data from source address_hierarchy_level table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM address_hierarchy_level")).fetchall()

    if source_data:
        info(f"Inserting {len(source_data)} records into target _address_hierarchy_level table...")
        insert_query = text("""
        INSERT IGNORE INTO _address_hierarchy_level (address_hierarchy_level_id, name, parent_level_id, address_field, uuid, required)
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

def extract_cohort(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()

    if drop_create:
        create_cohort_table(target_engine, drop_create=drop_create)

    info("Fetching data from source cohort table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM cohort")).fetchall()

    if source_data:
        info(f"Inserting {len(source_data)} records into target _cohort table...")
        insert_query = text("""
        INSERT IGNORE INTO _cohort (cohort_id, name, description, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) 
        VALUES (:cohort_id, :name, :description, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :changed_by, :date_changed, :uuid)
        """)
        
        with target_engine.connect() as target_conn:
            for row in source_data:
                # Handle bit field for MySQL/SQLAlchemy if necessary
                target_conn.execute(insert_query, {
                    "cohort_id": row.cohort_id, "name": row.name, "description": row.description, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source cohort table.")

def extract_cohort_member(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_cohort_member_table(target_engine, drop_create=drop_create)
    info("Fetching data from source cohort_member table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM cohort_member")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _cohort_member table...")
        insert_query = text("INSERT IGNORE INTO _cohort_member (cohort_id, patient_id) VALUES (:cohort_id, :patient_id)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "cohort_id": row.cohort_id, "patient_id": row.patient_id
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source cohort_member table.")

def extract_concept(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept table...")
        insert_query = text("INSERT IGNORE INTO _concept (concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_id, :retired, :short_name, :description, :form_text, :datatype_id, :class_id, :is_set, :creator, :date_created, :version, :changed_by, :date_changed, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_id": row.concept_id, "retired": row.retired, "short_name": row.short_name, "description": row.description, "form_text": row.form_text, "datatype_id": row.datatype_id, "class_id": row.class_id, "is_set": row.is_set, "creator": row.creator, "date_created": row.date_created, "version": row.version, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept table.")

def extract_concept_answer(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_answer_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_answer table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_answer")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_answer table...")
        insert_query = text("INSERT IGNORE INTO _concept_answer (concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, sort_weight, uuid) VALUES (:concept_answer_id, :concept_id, :answer_concept, :answer_drug, :creator, :date_created, :sort_weight, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_answer_id": row.concept_answer_id, "concept_id": row.concept_id, "answer_concept": row.answer_concept, "answer_drug": row.answer_drug, "creator": row.creator, "date_created": row.date_created, "sort_weight": '1', "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_answer table.")

def extract_concept_class(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_class_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_class table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_class")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_class table...")
        insert_query = text("INSERT IGNORE INTO _concept_class (concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_class_id, :name, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_class_id": row.concept_class_id, "name": row.name, "description": row.description, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_class table.")

def extract_concept_complex(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_complex_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_complex table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_complex")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_complex table...")
        insert_query = text("INSERT IGNORE INTO _concept_complex (concept_id, handler) VALUES (:concept_id, :handler)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_id": row.concept_id, "handler": row.handler
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_complex table.")

def extract_concept_datatype(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_datatype_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_datatype table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_datatype")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_datatype table...")
        insert_query = text("INSERT IGNORE INTO _concept_datatype (concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_datatype_id, :name, :hl7_abbreviation, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_datatype_id": row.concept_datatype_id, "name": row.name, "hl7_abbreviation": row.hl7_abbreviation, "description": row.description, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_datatype table.")

def extract_concept_description(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_description_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_description table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_description")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_description table...")
        insert_query = text("INSERT IGNORE INTO _concept_description (concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid) VALUES (:concept_description_id, :concept_id, :description, :locale, :creator, :date_created, :changed_by, :date_changed, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_description_id": row.concept_description_id, "concept_id": row.concept_id, "description": row.description, "locale": row.locale, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_description table.")

def extract_concept_map(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_map_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_map table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_map")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_map table...")
        insert_query = text("INSERT IGNORE INTO _concept_map (concept_map_id, source, source_code, comment, creator, date_created, concept_id, uuid) VALUES (:concept_map_id, :source, :source_code, :comment, :creator, :date_created, :concept_id, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_map_id": row.concept_map_id, "source": row.source, "source_code": row.source_code, "comment": row.comment, "creator": row.creator, "date_created": row.date_created, "concept_id": row.concept_id, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_map table.")

def extract_concept_name(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_concept_name_table(target_engine, drop_create=drop_create)

    df = read_excel_sheet('concept_mapping.xlsx', 'concept_name')
    df = df.where(pd.notna(df), None)
    if not df.empty:
        info(f"Inserting {len(df)} records from concept_mapping.xlsx concept_name sheet into target _concept_name table...")
        df = df.rename(columns={"type": "concept_name_type"})
        insert_query = text("INSERT IGNORE INTO _concept_name (concept_name_id, concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, voided, voided_by, date_voided, void_reason, uuid) VALUES (:concept_name_id, :concept_id, :name, :locale, :locale_preferred, :creator, :date_created, :concept_name_type, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
        with target_engine.connect() as target_conn:
            max_id = target_conn.execute(text("SELECT COALESCE(MAX(concept_name_id), 0) FROM _concept_name")).scalar()
            df["concept_name_id"] = range(max_id + 1, max_id + len(df) + 1)
            df["locale_preferred"] = 0
            df["creator"] = 1
            df["date_created"] = pd.Timestamp.now()
            df["voided"] = 0
            df["voided_by"] = None
            df["date_voided"] = None
            df["void_reason"] = None
            df["uuid"] = [str(uuid4()) for _ in range(len(df))]
            source_data = df[[
                "concept_name_id", "concept_id", "name", "locale", "locale_preferred", "creator",
                "date_created", "concept_name_type", "voided", "voided_by", "date_voided",
                "void_reason", "uuid"
            ]].to_dict(orient='records')
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import from concept_mapping.xlsx concept_name sheet completed successfully.")

def extract_concept_name_tag(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_name_tag_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_name_tag table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_name_tag")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_name_tag table...")
        insert_query = text("INSERT IGNORE INTO _concept_name_tag (concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid) VALUES (:concept_name_tag_id, :tag, :description, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_name_tag_id": row.concept_name_tag_id, "tag": row.tag, "description": row.description, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_name_tag table.")

def extract_concept_name_tag_map(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_name_tag_map_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_name_tag_map table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_name_tag_map")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_name_tag_map table...")
        insert_query = text("INSERT IGNORE INTO _concept_name_tag_map (concept_name_id, concept_name_tag_id) VALUES (:concept_name_id, :concept_name_tag_id)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_name_id": row.concept_name_id, "concept_name_tag_id": row.concept_name_tag_id
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_name_tag_map table.")

def extract_concept_numeric(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_numeric_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_numeric table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_numeric")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_numeric table...")
        insert_query = text("INSERT IGNORE INTO _concept_numeric (concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise) VALUES (:concept_id, :hi_absolute, :hi_critical, :hi_normal, :low_absolute, :low_critical, :low_normal, :units, :precise)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_id": row.concept_id, "hi_absolute": row.hi_absolute, "hi_critical": row.hi_critical, "hi_normal": row.hi_normal, "low_absolute": row.low_absolute, "low_critical": row.low_critical, "low_normal": row.low_normal, "units": row.units, "precise": row.precise
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_numeric table.")

def extract_concept_reference_source(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_concept_reference_source_table(target_engine, drop_create=drop_create)

    df = read_excel_sheet('concept_mapping.xlsx', 'concept_source')
    if not df.empty:
        info(f"Inserting {len(df)} records into target _concept_reference_source table...")
        df = df.replace({np.nan: None})  # critical line
        insert_query = text("INSERT IGNORE INTO _concept_reference_source (concept_source_id, name, description, hl7_code, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_source_id, :name, :description, :hl7_code, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        source_data = df.to_dict(orient='records')

        with target_engine.connect() as target_conn:
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import completed successfully.")

def extract_concept_reference_term_table(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_concept_reference_term_table(target_engine, drop_create=drop_create)
    # Note! The data for this table will be populated in the transformation step.


def extract_concept_set(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_set_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_set table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_set")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_set table...")
        insert_query = text("INSERT IGNORE INTO _concept_set (concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) VALUES (:concept_set_id, :concept_id, :concept_set, :sort_weight, :creator, :date_created, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_set_id": row.concept_set_id, "concept_id": row.concept_id, "concept_set": row.concept_set, "sort_weight": row.sort_weight, "creator": row.creator, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_set table.")

def extract_concept_word(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_word_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_word table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_word")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_word table...")
        insert_query = text("INSERT IGNORE INTO _concept_word (concept_word_id, concept_id, word, locale, concept_name_id, weight) VALUES (:concept_word_id, :concept_id, :word, :locale, :concept_name_id, :weight)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_word_id": row.concept_word_id, "concept_id": row.concept_id, "word": row.word, "locale": row.locale, "concept_name_id": row.concept_name_id, "weight": 1
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_word table.")

def extract_drug(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_drug_table(target_engine, drop_create=drop_create)
    info("Fetching data from source drug table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM drug")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _drug table...")
        insert_query = text("INSERT IGNORE INTO _drug (drug_id, concept_id, name, combination, dosage_form, dose_strength, maximum_daily_dose, minimum_daily_dose, route, units, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:drug_id, :concept_id, :name, :combination, :dosage_form, :dose_strength, :maximum_daily_dose, :minimum_daily_dose, :route, :units, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "drug_id": row.drug_id, "concept_id": row.concept_id, "name": row.name, "combination": row.combination, "dosage_form": row.dosage_form, "dose_strength": row.dose_strength, "maximum_daily_dose": row.maximum_daily_dose, "minimum_daily_dose": row.minimum_daily_dose, "route": row.route, "units": row.units, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source drug table.")

def extract_drug_ingredient(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_drug_ingredient_table(target_engine, drop_create=drop_create)
    info("Fetching data from source drug_ingredient table...")
    insert_query = text("INSERT IGNORE INTO _drug_ingredient (concept_id, ingredient_id, uuid) VALUES (:concept_id, :ingredient_id, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM drug_ingredient")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _drug_ingredient table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_id": row.concept_id, "ingredient_id": row.ingredient_id, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source drug_ingredient table.")

def extract_drug_order(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_drug_order_table(target_engine, drop_create=drop_create)
    info("Fetching data from source drug_order table...")
    insert_query = text("INSERT IGNORE INTO _drug_order (order_id, drug_inventory_id, dose, equivalent_daily_dose, units, frequency, prn, complex, quantity) VALUES (:order_id, :drug_inventory_id, :dose, :equivalent_daily_dose, :units, :frequency, :prn, :complex, :quantity)")
    
    with source_engine.connect() as source_conn:
        # Using execution_options(yield_per=BATCH_SIZE) for batching
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM drug_order"))
        
        batch = []
        count = 0
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "order_id": row.order_id, "drug_inventory_id": row.drug_inventory_id, "dose": row.dose, "equivalent_daily_dose": row.equivalent_daily_dose, "units": row.units, "frequency": row.frequency, "prn": row.prn, "complex": row.complex, "quantity": row.quantity
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch of {len(batch)} records into target _drug_order table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _drug_order table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source drug_order table.")

def extract_encounter(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_encounter_table(target_engine, drop_create=drop_create)
    info("Fetching data from source encounter table...")
    insert_query = text("INSERT IGNORE INTO _encounter (encounter_id, encounter_type, patient_id, provider_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) VALUES (:encounter_id, :encounter_type, :patient_id, :provider_id, :location_id, :form_id, :encounter_datetime, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :changed_by, :date_changed, :uuid)")
    
    with source_engine.connect() as source_conn:
        # Using execution_options(yield_per=BATCH_SIZE) for batching
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM encounter"))
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "encounter_id": row.encounter_id, "encounter_type": row.encounter_type, "patient_id": row.patient_id, "provider_id": row.provider_id, "location_id": row.location_id, "form_id": row.form_id, "encounter_datetime": row.encounter_datetime, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _encounter table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _encounter table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source encounter table.")

def extract_encounter_provider(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_encounter_provider_table(target_engine, drop_create=drop_create)
    # Note! The data for this table will be populated in the transformation step.

def extract_encounter_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_encounter_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source encounter_type table...")
    insert_query = text("INSERT IGNORE INTO _encounter_type (encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:encounter_type_id, :name, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM encounter_type")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _encounter_type table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "encounter_type_id": row.encounter_type_id, "name": row.name, "description": row.description, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source encounter_type table.")

def extract_field(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_field_table(target_engine, drop_create=drop_create)
    info("Fetching data from source field table...")
    insert_query = text("INSERT IGNORE INTO _field (field_id, name, description, field_type, concept_id, table_name, attribute_name, default_value, select_multiple, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:field_id, :name, :description, :field_type, :concept_id, :table_name, :attribute_name, :default_value, :select_multiple, :creator, :date_created, :changed_by, :date_changed, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM field")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _field table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "field_id": row.field_id, "name": row.name, "description": row.description, "field_type": row.field_type, "concept_id": row.concept_id, "table_name": row.table_name, "attribute_name": row.attribute_name, "default_value": row.default_value, "select_multiple": row.select_multiple, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source field table.")

def extract_field_answer(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_field_answer_table(target_engine, drop_create=drop_create)
    info("Fetching data from source field_answer table...")
    insert_query = text("INSERT IGNORE INTO _field_answer (field_id, answer_id, creator, date_created, uuid) VALUES (:field_id, :answer_id, :creator, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM field_answer")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _field_answer table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "field_id": row.field_id, "answer_id": row.answer_id, "creator": row.creator, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source field_answer table.")

def extract_field_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_field_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source field_type table...")
    insert_query = text("INSERT IGNORE INTO _field_type (field_type_id, name, description, is_set, creator, date_created, uuid) VALUES (:field_type_id, :name, :description, :is_set, :creator, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM field_type")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _field_type table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "field_type_id": row.field_type_id, "name": row.name, "description": row.description, "is_set": row.is_set, "creator": row.creator, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source field_type table.")

def extract_form(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_form_table(target_engine, drop_create=drop_create)
    info("Fetching data from source form table...")
    insert_query = text("INSERT IGNORE INTO _form (form_id, name, version, build, published, xslt, template, description, encounter_type, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retired_reason, uuid) VALUES (:form_id, :name, :version, :build, :published, :xslt, :template, :description, :encounter_type, :creator, :date_created, :changed_by, :date_changed, :retired, :retired_by, :date_retired, :retired_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM form")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _form table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "form_id": row.form_id, "name": row.name, "version": row.version, "build": row.build, "published": row.published, "xslt": row.xslt, "template": row.template, "description": row.description, "encounter_type": row.encounter_type, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retired_reason": row.retired_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source form table.")

def extract_form_field(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_form_field_table(target_engine, drop_create=drop_create)
    info("Fetching data from source form_field table...")
    insert_query = text("INSERT IGNORE INTO _form_field (form_field_id, form_id, field_id, field_number, field_part, page_number, parent_form_field, min_occurs, max_occurs, required, changed_by, date_changed, creator, date_created, sort_weight, uuid) VALUES (:form_field_id, :form_id, :field_id, :field_number, :field_part, :page_number, :parent_form_field, :min_occurs, :max_occurs, :required, :changed_by, :date_changed, :creator, :date_created, :sort_weight, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM form_field")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _form_field table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "form_field_id": row.form_field_id, "form_id": row.form_id, "field_id": row.field_id, "field_number": row.field_number, "field_part": row.field_part, "page_number": row.page_number, "parent_form_field": row.parent_form_field, "min_occurs": row.min_occurs, "max_occurs": row.max_occurs, "required": row.required, "changed_by": row.changed_by, "date_changed": row.date_changed, "creator": row.creator, "date_created": row.date_created, "sort_weight": row.sort_weight, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source form_field table.")

def extract_global_property(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_global_property_table(target_engine, drop_create=drop_create)
    info("Fetching data from source global_property table...")
    insert_query = text("INSERT IGNORE INTO _global_property (property, property_value, description, uuid) VALUES (:property, :property_value, :description, :uuid)")
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

def extract_hl7_in_error(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_hl7_in_error_table(target_engine, drop_create=drop_create)
    info("Fetching data from source hl7_in_error table...")
    insert_query = text("INSERT IGNORE INTO _hl7_in_error (hl7_in_error_id, hl7_source, hl7_source_key, hl7_data, error, error_details, date_created, uuid) VALUES (:hl7_in_error_id, :hl7_source, :hl7_source_key, :hl7_data, :error, :error_details, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM hl7_in_error")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _hl7_in_error table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "hl7_in_error_id": row.hl7_in_error_id, "hl7_source": row.hl7_source, "hl7_source_key": row.hl7_source_key, "hl7_data": row.hl7_data, "error": row.error, "error_details": row.error_details, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source hl7_in_error table.")

def extract_hl7_in_queue(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_hl7_in_queue_table(target_engine, drop_create=drop_create)
    info("Fetching data from source hl7_in_queue table...")
    insert_query = text("INSERT IGNORE INTO _hl7_in_queue (hl7_in_queue_id, hl7_source, hl7_source_key, hl7_data, message_state, date_processed, error_msg, date_created, uuid) VALUES (:hl7_in_queue_id, :hl7_source, :hl7_source_key, :hl7_data, :message_state, :date_processed, :error_msg, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM hl7_in_queue")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _hl7_in_queue table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "hl7_in_queue_id": row.hl7_in_queue_id, "hl7_source": row.hl7_source, "hl7_source_key": row.hl7_source_key, "hl7_data": row.hl7_data, "message_state": row.message_state, "date_processed": row.date_processed, "error_msg": row.error_msg, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source hl7_in_queue table.")

def extract_hl7_source(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_hl7_source_table(target_engine, drop_create=drop_create)
    info("Fetching data from source hl7_source table...")
    insert_query = text("INSERT IGNORE INTO _hl7_source (hl7_source_id, name, description, creator, date_created, uuid) VALUES (:hl7_source_id, :name, :description, :creator, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM hl7_source")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _hl7_source table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "hl7_source_id": row.hl7_source_id, "name": row.name, "description": row.description, "creator": row.creator, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source hl7_source table.")

def extract_htmlformentry_html_form(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_htmlformentry_html_form(target_engine, drop_create=drop_create)
    info("Fetching data from source htmlformentry_html_form table...")
    insert_query = text("INSERT IGNORE INTO _htmlformentry_html_form (id, form_id, name, xml_data, creator, date_created, changed_by, date_changed, retired, uuid, description, retired_by, date_retired, retire_reason) VALUES (:id, :form_id, :name, :xml_data, :creator, :date_created, :changed_by, :date_changed, :retired, :uuid, :description, :retired_by, :date_retired, :retire_reason)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM htmlformentry_html_form")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _htmlformentry_html_form table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "id": row.id, "form_id": row.form_id, "name": row.name, "xml_data": row.xml_data, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "uuid": row.uuid, "description": row.description, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source htmlformentry_html_form table.")

def extract_location(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_location_table(target_engine, drop_create=drop_create)
    info("Fetching data from source location table...")
    insert_query = text("INSERT IGNORE INTO _location (location_id, name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, county_district, address3, address4, address5, address6, retired, retired_by, date_retired, retire_reason, parent_location, uuid) VALUES (:location_id, :name, :description, :address1, :address2, :city_village, :state_province, :postal_code, :country, :latitude, :longitude, :creator, :date_created, :county_district, :address3, :address4, :address5, :address6, :retired, :retired_by, :date_retired, :retire_reason, :parent_location, :uuid)")
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

def extract_note(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_note_table(target_engine, drop_create=drop_create)
    info("Fetching data from source note table...")
    insert_query = text("INSERT IGNORE INTO _note (note_id, note_type, patient_id, obs_id, encounter_id, text, priority, parent, creator, date_created, changed_by, date_changed, uuid) VALUES (:note_id, :note_type, :patient_id, :obs_id, :encounter_id, :text, :priority, :parent, :creator, :date_created, :changed_by, :date_changed, :uuid)")
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
    insert_query = text("INSERT IGNORE INTO _notification_alert (alert_id, text, satisfied_by_any, alert_read, date_to_expire, creator, date_created, changed_by, date_changed, uuid) VALUES (:alert_id, :text, :satisfied_by_any, :alert_read, :date_to_expire, :creator, :date_created, :changed_by, :date_changed, :uuid)")
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
    insert_query = text("INSERT IGNORE INTO _notification_alert_recipient (alert_id, user_id, alert_read, date_changed, uuid) VALUES (:alert_id, :user_id, :alert_read, :date_changed, :uuid)")
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

def extract_obs(drop_create=False, resume=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_obs_table(target_engine, drop_create=drop_create)
    info("Fetching data from source obs table...")
    insert_query = text("""
        INSERT IGNORE INTO _obs 
               (obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid) 
        VALUES (:obs_id, :person_id, :concept_id, :encounter_id, :order_id, :obs_datetime, :location_id, :obs_group_id, :accession_number, :value_group_id, :value_boolean, :value_coded, :value_coded_name_id, :value_drug, :value_datetime, :value_numeric, :value_modifier, :value_text, :value_complex, :comments, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :uuid)
    """)

    with source_engine.connect() as source_conn:
        last_date = '1900-01-01'
        if resume:
            with target_engine.connect() as target_conn:
                fetch_latest_sql = text("SELECT DATE(COALESCE(MAX(date_created), '1900-01-01')) AS latest FROM _obs WHERE date_created <= CURRENT_DATE()")
                last_entry = target_conn.execute(fetch_latest_sql).fetchone()
                last_date = last_entry[0] if last_entry and last_entry[0] is not None else "1900-01-01"
                info(f"Resume enabled. Using latest target _obs date_created: {last_date}")

        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM obs WHERE date_created <= CURRENT_DATE() AND date_created >= :latest_date"), {
            "latest_date": last_date
        })
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "obs_id": row.obs_id, "person_id": row.person_id, "concept_id": row.concept_id, "encounter_id": row.encounter_id, "order_id": row.order_id, "obs_datetime": row.obs_datetime, "location_id": row.location_id, "obs_group_id": row.obs_group_id, "accession_number": row.accession_number, "value_group_id": row.value_group_id, "value_boolean": row.value_boolean, "value_coded": row.value_coded, "value_coded_name_id": row.value_coded_name_id, "value_drug": row.value_drug, "value_datetime": row.value_datetime, "value_numeric": row.value_numeric, "value_modifier": row.value_modifier, "value_text": row.value_text, "value_complex": row.value_complex, "comments": row.comments, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch {batch_number} of {len(batch)} records into target _obs table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
                    batch_number += 1
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _obs table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source obs table.")

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

def extract_privilege(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_privilege_table(target_engine, drop_create=drop_create)
    info("Fetching data from source privilege table...")
    insert_query = text("INSERT IGNORE INTO _privilege (privilege, description, uuid) VALUES (:privilege, :description, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM privilege")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _privilege table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "privilege": row.privilege, "description": row.description, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source privilege table.")

def extract_program(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_program_table(target_engine, drop_create=drop_create)
    info("Fetching data from source program table...")
    insert_query = text("INSERT IGNORE INTO _program (program_id, concept_id, creator, date_created, changed_by, date_changed, retired, name, description, uuid) VALUES (:program_id, :concept_id, :creator, :date_created, :changed_by, :date_changed, :retired, :name, :description, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM program")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _program table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "program_id": row.program_id, "concept_id": row.concept_id, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "name": row.name, "description": row.description, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source program table.")

def extract_program_workflow(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_program_workflow_table(target_engine, drop_create=drop_create)
    info("Fetching data from source program_workflow table...")
    insert_query = text("INSERT IGNORE INTO _program_workflow (program_workflow_id, program_id, concept_id, creator, date_created, retired, changed_by, date_changed, uuid) VALUES (:program_workflow_id, :program_id, :concept_id, :creator, :date_created, :retired, :changed_by, :date_changed, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM program_workflow")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _program_workflow table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "program_workflow_id": row.program_workflow_id, "program_id": row.program_id, "concept_id": row.concept_id, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source program_workflow table.")

def extract_program_workflow_state(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_program_workflow_state_table(target_engine, drop_create=drop_create)
    info("Fetching data from source program_workflow_state table...")
    insert_query = text("INSERT IGNORE INTO _program_workflow_state (program_workflow_state_id, program_workflow_id, concept_id, initial, terminal, creator, date_created, retired, changed_by, date_changed, uuid) VALUES (:program_workflow_state_id, :program_workflow_id, :concept_id, :initial, :terminal, :creator, :date_created, :retired, :changed_by, :date_changed, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM program_workflow_state")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _program_workflow_state table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "program_workflow_state_id": row.program_workflow_state_id, "program_workflow_id": row.program_workflow_id, "concept_id": row.concept_id, "initial": row.initial, "terminal": row.terminal, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source program_workflow_state table.")

def extract_provider(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_provider_table(target_engine, drop_create=drop_create)
    # Note! The data for this table will be populated in the transformation step.

def extract_relationship_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_relationship_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source relationship_type table...")
    insert_query = text("INSERT IGNORE INTO _relationship_type (relationship_type_id, a_is_to_b, b_is_to_a, preferred, weight, description, creator, date_created, uuid, retired, retired_by, date_retired, retire_reason) VALUES (:relationship_type_id, :a_is_to_b, :b_is_to_a, :preferred, :weight, :description, :creator, :date_created, :uuid, :retired, :retired_by, :date_retired, :retire_reason)")
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

def extract_report_object(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_report_object_table(target_engine, drop_create=drop_create)
    info("Fetching data from source report_object table...")
    insert_query = text("INSERT IGNORE INTO _report_object (report_object_id, name, description, report_object_type, report_object_sub_type, xml_data, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) VALUES (:report_object_id, :name, :description, :report_object_type, :report_object_sub_type, :xml_data, :creator, :date_created, :changed_by, :date_changed, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
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
    insert_query = text("INSERT IGNORE INTO _report_schema_xml (report_schema_id, name, description, xml_data, uuid) VALUES (:report_schema_id, :name, :description, :xml_data, :uuid)")
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

def extract_role(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_role_table(target_engine, drop_create=drop_create)
    info("Fetching data from source role table...")
    insert_query = text("INSERT IGNORE INTO _role (role, description, uuid) VALUES (:role, :description, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM role")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _role table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "role": row.role, "description": row.description, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source role table.")

def extract_role_privilege(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_role_privilege_table(target_engine, drop_create=drop_create)
    info("Fetching data from source role_privilege table...")
    insert_query = text("INSERT IGNORE INTO _role_privilege (role, privilege) VALUES (:role, :privilege)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM role_privilege")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _role_privilege table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "role": row.role, "privilege": row.privilege
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source role_privilege table.")

def extract_role_role(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_role_role_table(target_engine, drop_create=drop_create)
    info("Fetching data from source role_role table...")
    insert_query = text("INSERT IGNORE INTO _role_role (parent_role, child_role) VALUES (:parent_role, :child_role)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM role_role")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _role_role table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "parent_role": row.parent_role, "child_role": row.child_role
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source role_role table.")

def extract_serialized_object(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_serialized_object_table(target_engine, drop_create=drop_create)
    info("Fetching data from source serialized_object table...")
    insert_query = text("INSERT IGNORE INTO _serialized_object (serialized_object_id, name, description, type, subtype, serialization_class, serialized_data, date_created, creator, date_changed, changed_by, retired, date_retired, retired_by, retire_reason, uuid) VALUES (:serialized_object_id, :name, :description, :type, :subtype, :serialization_class, :serialized_data, :date_created, :creator, :date_changed, :changed_by, :retired, :date_retired, :retired_by, :retire_reason, :uuid)")
    
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

def extract_users(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_users_table(target_engine, drop_create=drop_create)
    info("Fetching data from source users table...")
    insert_query = text("INSERT IGNORE INTO _users (user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:user_id, :system_id, :username, :password, :salt, :secret_question, :secret_answer, :creator, :date_created, :changed_by, :date_changed, :person_id, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM users")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _users table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "user_id": row.user_id, "system_id": row.system_id, "username": row.username, "password": row.password, "salt": row.salt, "secret_question": row.secret_question, "secret_answer": row.secret_answer, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "person_id": row.person_id, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source users table.")

def extract_user_property(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_user_property_table(target_engine, drop_create=drop_create)
    info("Fetching data from source user_property table...")
    insert_query = text("INSERT IGNORE INTO _user_property (user_id, property, property_value) VALUES (:user_id, :property, :property_value)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM user_property")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _user_property table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "user_id": row.user_id, "property": row.property, "property_value": row.property_value
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source user_property table.")

def extract_user_role(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_user_role_table(target_engine, drop_create=drop_create)
    info("Fetching data from source user_role table...")
    insert_query = text("INSERT IGNORE INTO _user_role (user_id, role) VALUES (:user_id, :role)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM user_role")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _user_role table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "user_id": row.user_id, "role": row.role
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source user_role table.")
