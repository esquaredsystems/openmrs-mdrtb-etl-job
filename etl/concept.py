import time
from uuid import uuid4

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.helpers import read_excel_sheet
from utils.logger import info, warning


##### Extraction functions #####
def extract_concept(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_concept_table(target_engine, drop_create=drop_create)

    df = read_excel_sheet('concept_mapping.xlsx', 'concept')
    if not df.empty:
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept"))
            target_conn.commit()

        info(f"Inserting {len(df)} records from concept_mapping.xlsx concept sheet into target _concept table...")
        df = df.replace({np.nan: None, pd.NaT: None})
        for column in ["short_name", "description", "form_text", "version", "date_retired", "retire_reason"]:
            if column not in df.columns:
                df[column] = None
        source_data = df[[
            "concept_id", "retired", "short_name", "description", "form_text",
            "datatype_id", "class_id", "is_set", "creator", "date_created",
            "version", "changed_by", "date_changed", "retired_by",
            "date_retired", "retire_reason", "uuid"
        ]].to_dict(orient='records')
        insert_query = text(
            "INSERT INTO _concept (concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_id, :retired, :short_name, :description, :form_text, :datatype_id, :class_id, :is_set, :creator, :date_created, :version, :changed_by, :date_changed, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import from concept_mapping.xlsx concept sheet completed successfully.")


def extract_concept_answer(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_concept_answer_table(target_engine, drop_create=drop_create)
    info("Fetching data from source concept_answer table...")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM concept_answer")).fetchall()
    if source_data:
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_answer"))
            target_conn.commit()

        info(f"Inserting {len(source_data)} records into target _concept_answer table...")
        insert_query = text(
            "INSERT INTO _concept_answer (concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, sort_weight, uuid) VALUES (:concept_answer_id, :concept_id, :answer_concept, :answer_drug, :creator, :date_created, :sort_weight, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_answer_id": row.concept_answer_id, "concept_id": row.concept_id,
                    "answer_concept": row.answer_concept, "answer_drug": row.answer_drug, "creator": row.creator,
                    "date_created": row.date_created, "sort_weight": '1', "uuid": row.uuid
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
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_class"))
            target_conn.commit()

        info(f"Inserting {len(source_data)} records into target _concept_class table...")
        insert_query = text(
            "INSERT INTO _concept_class (concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_class_id, :name, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_class_id": row.concept_class_id, "name": row.name, "description": row.description,
                    "creator": row.creator, "date_created": row.date_created, "retired": row.retired,
                    "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason,
                    "uuid": row.uuid
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
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_complex"))
            target_conn.commit()

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
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_datatype"))
            target_conn.commit()

        info(f"Inserting {len(source_data)} records into target _concept_datatype table...")
        insert_query = text(
            "INSERT INTO _concept_datatype (concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_datatype_id, :name, :hl7_abbreviation, :description, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_datatype_id": row.concept_datatype_id, "name": row.name,
                    "hl7_abbreviation": row.hl7_abbreviation, "description": row.description, "creator": row.creator,
                    "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by,
                    "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
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
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_description"))
            target_conn.commit()

        info(f"Inserting {len(source_data)} records into target _concept_description table...")
        insert_query = text(
            "INSERT INTO _concept_description (concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid) VALUES (:concept_description_id, :concept_id, :description, :locale, :creator, :date_created, :changed_by, :date_changed, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_description_id": row.concept_description_id, "concept_id": row.concept_id,
                    "description": row.description, "locale": row.locale, "creator": row.creator,
                    "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed,
                    "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source concept_description table.")


def extract_concept_map(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_concept_map_table(target_engine, drop_create=drop_create)

    df = read_excel_sheet('concept_mapping.xlsx', 'concept_map')
    if not df.empty:
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_map"))
            target_conn.commit()

        info(
            f"Inserting {len(df)} records from concept_mapping.xlsx concept_map sheet into target _concept_map table...")
        df = df.replace({np.nan: None, pd.NaT: None})
        for column in ["comment"]:
            if column not in df.columns:
                df[column] = None
        source_data = df[[
            "concept_map_id", "source_id", "source_code", "comment", "creator", "date_created", "concept_id", "uuid"
        ]].to_dict(orient='records')
        insert_query = text("""
            INSERT INTO _concept_map (concept_map_id, source, source_code, comment, creator, date_created, concept_id, uuid) 
            VALUES (:concept_map_id, :source_id, :source_code, :comment, :creator, :date_created, :concept_id, :uuid)""")
        with target_engine.connect() as target_conn:
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import from concept_mapping.xlsx concept_map sheet completed successfully.")


def extract_concept_name(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_concept_name_table(target_engine, drop_create=drop_create)

    df = read_excel_sheet('concept_mapping.xlsx', 'concept_name')
    df = df.where(pd.notna(df), None)
    if not df.empty:
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_name"))
            target_conn.commit()

        info(
            f"Inserting {len(df)} records from concept_mapping.xlsx concept_name sheet into target _concept_name table...")
        df = df.rename(columns={"type": "concept_name_type"})
        insert_query = text(
            "INSERT INTO _concept_name (concept_name_id, concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, voided, voided_by, date_voided, void_reason, uuid) VALUES (:concept_name_id, :concept_id, :name, :locale, :locale_preferred, :creator, :date_created, :concept_name_type, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
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
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_name_tag"))
            target_conn.commit()

        info(f"Inserting {len(source_data)} records into target _concept_name_tag table...")
        insert_query = text(
            "INSERT INTO _concept_name_tag (concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid) VALUES (:concept_name_tag_id, :tag, :description, :creator, :date_created, :voided, :voided_by, :date_voided, :void_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_name_tag_id": row.concept_name_tag_id, "tag": row.tag, "description": row.description,
                    "creator": row.creator, "date_created": row.date_created, "voided": row.voided,
                    "voided_by": row.voided_by, "date_voided": row.date_voided, "void_reason": row.void_reason,
                    "uuid": row.uuid
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
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_name_tag_map"))
            target_conn.commit()

        info(f"Inserting {len(source_data)} records into target _concept_name_tag_map table...")
        insert_query = text(
            "INSERT INTO _concept_name_tag_map (concept_name_id, concept_name_tag_id) VALUES (:concept_name_id, :concept_name_tag_id)")
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
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_numeric"))
            target_conn.commit()

        info(f"Inserting {len(source_data)} records into target _concept_numeric table...")
        insert_query = text(
            "INSERT INTO _concept_numeric (concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise) VALUES (:concept_id, :hi_absolute, :hi_critical, :hi_normal, :low_absolute, :low_critical, :low_normal, :units, :precise)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_id": row.concept_id, "hi_absolute": row.hi_absolute, "hi_critical": row.hi_critical,
                    "hi_normal": row.hi_normal, "low_absolute": row.low_absolute, "low_critical": row.low_critical,
                    "low_normal": row.low_normal, "units": row.units, "precise": row.precise
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
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_reference_source"))
            target_conn.commit()

        info(f"Inserting {len(df)} records into target _concept_reference_source table...")
        df = df.replace({np.nan: None})  # critical line
        insert_query = text(
            "INSERT INTO _concept_reference_source (concept_source_id, name, description, hl7_code, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:concept_source_id, :name, :description, :hl7_code, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        source_data = df.to_dict(orient='records')

        with target_engine.connect() as target_conn:
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import completed successfully.")


def extract_concept_reference_term_table(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_concept_reference_term_table(target_engine, drop_create=drop_create)


def extract_concept_set(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_concept_set_table(target_engine, drop_create=drop_create)

    df = read_excel_sheet('concept_mapping.xlsx', 'concept_set')
    if not df.empty:
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _concept_set"))
            target_conn.commit()

        info(
            f"Inserting {len(df)} records from concept_mapping.xlsx concept_set sheet into target _concept_set table...")
        df = df.replace({np.nan: None, pd.NaT: None})
        source_data = df[[
            "concept_set_id", "concept_id", "concept_set", "sort_weight", "creator", "date_created", "uuid"
        ]].to_dict(orient='records')
        insert_query = text(
            "INSERT INTO _concept_set (concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) VALUES (:concept_set_id, :concept_id, :concept_set, :sort_weight, :creator, :date_created, :uuid)")
        with target_engine.connect() as target_conn:
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import from concept_mapping.xlsx concept_set sheet completed successfully.")


def extract_concept_word(drop_create=False):
    info("Concept word comes prefilled.")


def extract_concept_group(drop_create):
    start_time = time.time()
    extract_concept(drop_create=drop_create)
    info("Concept table created successfully")
    extract_concept_answer(drop_create=drop_create)
    info("Concept answer table created successfully")
    extract_concept_class(drop_create=drop_create)
    info("Concept class table created successfully")
    extract_concept_complex(drop_create=drop_create)
    info("Concept complex table created successfully")
    extract_concept_datatype(drop_create=drop_create)
    info("Concept datatype table created successfully")
    extract_concept_description(drop_create=drop_create)
    info("Concept description table created successfully")
    extract_concept_map(drop_create=drop_create)
    info("Concept map table created successfully")
    extract_concept_name(drop_create=drop_create)
    info("Concept name table created successfully")
    extract_concept_name_tag(drop_create=drop_create)
    info("Concept name tag table created successfully")
    extract_concept_numeric(drop_create=drop_create)
    info("Concept numeric table created successfully")
    extract_concept_reference_source(drop_create=drop_create)
    info("Concept reference source table created successfully")
    extract_concept_set(drop_create=drop_create)
    info("Concept set table created successfully")
    extract_concept_word(drop_create=drop_create)
    info("Concept word table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")


##### Transformation functions #####
def transform_concept_reference_term():
    start_time = time.time()
    target_engine = get_target_engine()
    select_sql = """
    SELECT DISTINCT cm.concept_map_id AS concept_reference_term_id, cm.source AS concept_source_id, NULL AS name, 
    cm.concept_id AS code, 1.0 AS version, cm.comment AS description, cm.creator, cm.date_created, 
    NULL AS date_changed, NULL AS changed_by, 0 AS retired, NULL AS retired_by, NULL AS date_retired, NULL AS retire_reason, cm.uuid  
    FROM _concept_map AS cm 
    INNER JOIN _concept_reference_source AS cs ON cs.concept_source_id = cm.source 
    INNER JOIN _concept_name AS cn ON cn.concept_id = cm.concept_id AND cn.locale = 'en'
    ORDER BY concept_reference_term_id
    """
    with target_engine.connect() as target_conn:
        source_data = target_conn.execute(text(select_sql)).fetchall()
    if source_data:
        info("(Re)creating _concept_reference_term table...")
        create_concept_reference_term_table(target_engine, drop_create=True)
        info("Transforming data to fill _concept_map table...")
        insert_sql = text("""
            INSERT INTO _concept_reference_term (concept_reference_term_id, concept_source_id, name, code, version, description, creator, date_created, date_changed, changed_by, retired, retired_by, date_retired, retire_reason, uuid) 
            VALUES (:concept_reference_term_id, :concept_source_id, :name, :code, :version, :description, :creator, :date_created, :date_changed, :changed_by, :retired, :retired_by, :date_retired, :retire_reason, :uuid)
        """)
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_sql, {
                    "concept_reference_term_id": row.concept_reference_term_id,
                    "concept_source_id": row.concept_source_id,
                    "name": row.name,
                    "code": row.code,
                    "version": row.version,
                    "description": row.description,
                    "creator": row.creator,
                    "date_created": row.date_created,
                    "date_changed": row.date_changed,
                    "changed_by": row.changed_by,
                    "retired": row.retired,
                    "retired_by": row.retired_by,
                    "date_retired": row.date_retired,
                    "retire_reason": row.retire_reason,
                    "uuid": row.uuid
                })
            target_conn.commit()
        info(
            f"Transform _concept_reference_term completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")
    else:
        warning("No data found in source _concept_map table.")


def transform_concept_group():
    transform_concept_reference_term()


##### Loading functions #####
def load_concept_class():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_class (concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) 
    SELECT concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM _concept_class 
    WHERE name NOT IN (SELECT name FROM concept_class);
    """
    with target_engine.connect() as conn:
        info("Loading data for field table...")
        conn.execute(text(select_insert_sql))
        extra = """
        INSERT IGNORE INTO concept_class (name, description, creator, date_created, retired, uuid)
        VALUES ('Frequency', '', 1, CURRENT_TIMESTAMP(), 0, UUID());
        """
        conn.execute(text(extra))
        conn.commit()
    info(f"Load concept_class completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_datatype():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_datatype (concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid from _concept_datatype
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_datatype table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_datatype completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_name_tag():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_name_tag (concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid)
    SELECT concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid FROM _concept_name_tag
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_name_tag table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_name_tag completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept (concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid)
    SELECT concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid FROM _concept
    """
    with target_engine.connect() as conn:
        info("Loading data for concept table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_reference_source():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_reference_source(concept_source_id, name, description, hl7_code, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT concept_source_id, name, description, hl7_code, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM _concept_reference_source
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_reference_source table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_reference_source completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_answer():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_answer (concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, sort_weight, uuid)
    SELECT concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, sort_weight, uuid FROM _concept_answer
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_answer table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_answer completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_complex():
    start_time = time.time()
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading concept_complex for field table...")
        conn.execute(text("""
        INSERT IGNORE INTO concept_complex (concept_id, handler) VALUES (198, 'BinaryDataHandler') # Scanned Lab Report
        """))
        conn.commit()
    info(f"Load concept_complex completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_name():
    start_time = time.time()
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading data for concept_name table...")
        # Run UPSERT
        upsert_sql = """
        INSERT INTO concept_name (concept_name_id, concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, voided, voided_by, date_voided, void_reason, uuid)
        SELECT concept_name_id, concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, voided, voided_by, date_voided, void_reason, uuid FROM _concept_name
        ON DUPLICATE KEY UPDATE concept_id = VALUES(concept_id), name = VALUES(name), locale = VALUES(locale), locale_preferred = VALUES(locale_preferred), creator = VALUES(creator), date_created = VALUES(date_created), concept_name_type = VALUES(concept_name_type), voided = VALUES(voided), voided_by = VALUES(voided_by), date_voided = VALUES(date_voided), void_reason = VALUES(void_reason)
        """
        conn.execute(text(upsert_sql))
        conn.commit()
    info(f"Load concept_name completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_description():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_description (concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid)
    SELECT concept_description_id, concept_id, replace(description, '\n', '') AS description, locale, creator, date_created, changed_by, date_changed, uuid FROM _concept_description
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_name table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_name completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_numeric():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_numeric (concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, allow_decimal) 
    SELECT concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise FROM _concept_numeric
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_numeric table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_numeric completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_reference_term():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_reference_term (concept_reference_term_id, concept_source_id, name, code, version, description, creator, date_created, date_changed, changed_by, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT concept_reference_term_id, concept_source_id, name, code, version, description, creator, date_created, date_changed, changed_by, retired, retired_by, date_retired, retire_reason, uuid FROM _concept_reference_term
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_reference_term table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_reference_term completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_map_tag():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_name_tag (concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid) 
    SELECT concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid FROM _concept_name_tag
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_map_tag table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_map_tag completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_set():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_set (concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) 
    SELECT concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid FROM _concept_set
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_set table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_set completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_concept_group():
    # Sequenced to avoid foreign key constraint error
    load_concept_datatype()
    load_concept_class()
    load_concept()
    load_concept_name_tag()
    load_concept_reference_source()
    load_concept_reference_term()
    load_concept_answer()
    load_concept_complex()
    load_concept_name()
    load_concept_description()
    load_concept_numeric()
    load_concept_set()
