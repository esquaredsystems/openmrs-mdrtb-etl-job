from sqlalchemy import text, false
from config.database import get_source_engine, get_target_engine
from utils.logger import info, warning
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
        INSERT INTO _cohort (cohort_id, name, description, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) 
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
        insert_query = text("INSERT INTO _cohort_member (cohort_id, patient_id) VALUES (:cohort_id, :patient_id)")
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
        insert_query = text("INSERT INTO _concept (concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_id, :retired, :short_name, :description, :form_text, :datatype_id, :class_id, :is_set, :creator, :date_created, :version, :changed_by, :date_changed, :retired_by, :date_retired, :retire_reason, :uuid)")
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
        insert_query = text("INSERT INTO _concept_answer (concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, sort_weight, uuid) VALUES (:concept_answer_id, :concept_id, :answer_concept, :answer_drug, :creator, :date_created, :sort_weight, :uuid)")
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
        insert_query = text("INSERT INTO _concept_class (concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_class_id, :name, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
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
        insert_query = text("INSERT INTO _concept_complex (concept_id, handler) VALUES (:concept_id, :handler)")
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
        insert_query = text("INSERT INTO _concept_datatype (concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_datatype_id, :name, :hl7_abbreviation, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_datatype_id": row.concept_datatype_id, "name": row.name, "hl7_abbreviation": row.hl7_abbreviation, "description": row.description, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_datatype table.")


def extract_concept_derived(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_derived_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_derived table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_derived")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_derived table...")
        insert_query = text("INSERT INTO _concept_derived (concept_id, rule, compile_date, compile_status, class_name) VALUES (:concept_id, :rule, :compile_date, :compile_status, :class_name)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_id": row.concept_id, "rule": row.rule, "compile_date": row.compile_date, "compile_status": row.compile_status, "class_name": row.class_name
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_derived table.")


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
        insert_query = text("INSERT INTO _concept_description (concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid) VALUES (:concept_description_id, :concept_id, :description, :locale, :creator, :date_created, :changed_by, :date_changed, :uuid)")
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
        insert_query = text("INSERT INTO _concept_map (concept_map_id, source, source_code, comment, creator, date_created, concept_id, uuid) VALUES (:concept_map_id, :source, :source_code, :comment, :creator, :date_created, :concept_id, :uuid)")
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
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_name_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_name table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_name")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_name table...")
        insert_query = text("INSERT INTO _concept_name (concept_name_id, concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, voided, voided_by, date_voided, void_reason, uuid) VALUES (:concept_name_id, :concept_id, :name, :locale, :locale_preferred, :creator, :date_created, :concept_name_type, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_name_id": row.concept_name_id, "concept_id": row.concept_id, "name": row.name, "locale": row.locale, "locale_preferred": 0, "creator": row.creator, "date_created": row.date_created, "concept_name_type": "FULLY_SPECIFIED", "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_name table.")


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
        insert_query = text("INSERT INTO _concept_name_tag (concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid) VALUES (:concept_name_tag_id, :tag, :description, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
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
        insert_query = text("INSERT INTO _concept_name_tag_map (concept_name_id, concept_name_tag_id) VALUES (:concept_name_id, :concept_name_tag_id)")
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
        insert_query = text("INSERT INTO _concept_numeric (concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise) VALUES (:concept_id, :hi_absolute, :hi_critical, :hi_normal, :low_absolute, :low_critical, :low_normal, :units, :precise)")
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
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_reference_source_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_reference_source table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_source")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _concept_reference_source table...")
        insert_query = text("INSERT INTO _concept_reference_source (concept_source_id, name, description, hl7_code, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_source_id, :name, :description, :hl7_code, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_source_id": row.concept_source_id, "name": row.name, "description": row.description, "hl7_code": row.hl7_code, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_reference_source table.")


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
        insert_query = text("INSERT INTO _concept_set (concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) VALUES (:concept_set_id, :concept_id, :concept_set, :sort_weight, :creator, :date_created, :uuid)")
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
        insert_query = text("INSERT INTO _concept_word (concept_word_id, concept_id, word, locale, concept_name_id, weight) VALUES (:concept_word_id, :concept_id, :word, :locale, :concept_name_id, :weight)")
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
        insert_query = text("INSERT INTO _drug (drug_id, concept_id, name, combination, dosage_form, dose_strength, maximum_daily_dose, minimum_daily_dose, route, units, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:drug_id, :concept_id, :name, :combination, :dosage_form, :dose_strength, :maximum_daily_dose, :minimum_daily_dose, :route, :units, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
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
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT concept_id, ingredient_id, uuid() as uuid FROM drug_ingredient")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _drug_ingredient table...")
        insert_query = text("INSERT INTO _drug_ingredient (concept_id, ingredient_id, uuid) VALUES (:concept_id, :ingredient_id, :uuid)")
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
    insert_query = text("INSERT INTO _drug_order (order_id, drug_inventory_id, dose, equivalent_daily_dose, units, frequency, prn, complex, quantity) VALUES (:order_id, :drug_inventory_id, :dose, :equivalent_daily_dose, :units, :frequency, :prn, :complex, :quantity)")
    
    with source_engine.connect() as source_conn:
        # Using execution_options(yield_per=10000) for batching
        result = source_conn.execution_options(yield_per=10000).execute(text("SELECT * FROM drug_order"))
        
        batch = []
        count = 0
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "order_id": row.order_id, "drug_inventory_id": row.drug_inventory_id, "dose": row.dose, "equivalent_daily_dose": row.equivalent_daily_dose, "units": row.units, "frequency": row.frequency, "prn": row.prn, "complex": row.complex, "quantity": row.quantity
                })
                count += 1
                if len(batch) >= 10000:
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
    insert_query = text("INSERT INTO _encounter (encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) VALUES (:encounter_id, :encounter_type, :patient_id, :location_id, :form_id, :encounter_datetime, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :changed_by, :date_changed, :uuid)")
    
    with source_engine.connect() as source_conn:
        # Using execution_options(yield_per=10000) for batching
        result = source_conn.execution_options(yield_per=10000).execute(text("SELECT * FROM encounter"))
        
        batch = []
        count = 0
        batch_number = 1
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "encounter_id": row.encounter_id, "encounter_type": row.encounter_type, "patient_id": row.patient_id, "location_id": row.location_id, "form_id": row.form_id, "encounter_datetime": row.encounter_datetime, "creator": row.creator, "date_created": row.date_created, "voided": row.voided, "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason, "changed_by": row.changed_by, "date_changed": row.date_changed, "uuid": row.uuid
                })
                count += 1
                if len(batch) >= 10000:
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
