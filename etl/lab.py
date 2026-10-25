import time
import numpy as np
import pandas as pd

from config.database import get_target_engine
from models.schema_models import *
from utils.helpers import get_labtest_attribute_type_data, get_labtest_type_data
from utils.logger import info, warning


# DST: the drug sits in value_coded and the result in the obs concept - 118 SUSCEPTIBLE TO TUBERCULOSIS DRUG
# or 132 RESISTANT TO TUBERCULOSIS DRUG - stored as the answers 543 SUSCEPTIBLE / 183 RESISTANT, which are the
# answers the result fields accept. (137 INDETERMINATE has no matching answer and is left out.)
DST_RESULT_ANSWER_SQL = "CASE o.concept_id WHEN 118 THEN 543 WHEN 132 THEN 183 END"
DST_DRUGS = [
    ("AMIKACIN RESISTANCE", 115), ("BEDAQUILINE RESISTANCE", 447), ("CAPREOMYCIN RESISTANCE", 107),
    ("CIPROFLOXACIN RESISTANCE", 79), ("CLARITHROMYCIN RESISTANCE", 93), ("CLOFAZAMINE RESISTANCE", 72),
    ("CYCLOSERINE RESISTANCE", 104), ("DELAMANID RESISTANCE", 448), ("ETHAMBUTOL RESISTANCE", 113),
    ("ETHIONAMIDE RESISTANCE", 76), ("GATIFLOXACIN RESISTANCE", 117), ("ISONIAZID RESISTANCE", 92),
    ("KANAMYCIN RESISTANCE", 112), ("LEVOFLOXACIN RESISTANCE", 101), ("LINEZOLID RESISTANCE", 446),
    ("MOXIFLOXACIN RESISTANCE", 109), ("OFLOXACIN RESISTANCE", 108), ("P-AMINOSALICY RESISTANCE", 98),
    ("PROTHIONAMIDE RESISTANCE", 42), ("PYRAZINAMIDE RESISTANCE", 41), ("PYRIDOXINE RESISTANCE", 73),
    ("RIFABUTIN RESISTANCE", 114), ("RIFAMPICIN RESISTANCE", 74), ("STREPTOMYCIN RESISTANCE", 75),
    ("TERIZIDONE RESISTANCE", 40), ("THIOACETAZONE RESISTANCE", 99), ("VIOMYCIN RESISTANCE", 116),
    ("OTHER RESISTANCE", 19),
]


##### Extraction functions #####
def extract_labtest_type(drop_create=False):
    target_engine = get_target_engine()
    if drop_create or not table_exists(target_engine, '_labtest_type'):
        create_labtest_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from commonlab.xlsx test_types sheet...")

    df = get_labtest_type_data()
    if not df.empty:
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _labtest_type"))
            target_conn.commit()

        df = df.replace({np.nan: None, pd.NaT: None})
        source_data = df[[
            "type_id", "name", "description", "test_group", "short_name", "requires_specimen", "reference_concept_id", "uuid"
        ]].to_dict(orient='records')

        info(f"Inserting {len(source_data)} records from commonlab.xlsx test_types sheet into target _labtest_type table...")
        insert_query = text(
            """
            INSERT INTO _labtest_type (test_type_id, name, description, test_group, short_name, requires_specimen, reference_concept_id, creator, date_created, retired, uuid) 
            VALUES (:type_id, :name, :description, :test_group, :short_name, :requires_specimen, :reference_concept_id, 1, '2023-01-01', 0, :uuid)
            """)
        with target_engine.connect() as target_conn:
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import from commonlab.xlsx test_types sheet completed successfully.")
    else:
        warning("No data found in commonlab.xlsx test_types sheet.")


def extract_labtest_attribute_type(drop_create=False):
    target_engine = get_target_engine()
    if drop_create or not table_exists(target_engine, '_labtest_attribute_type'):
        create_labtest_attribute_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from commonlab.xlsx attribute_types sheet...")

    df = get_labtest_attribute_type_data()
    if not df.empty:
        with target_engine.connect() as target_conn:
            target_conn.execute(text("TRUNCATE TABLE _labtest_attribute_type"))
            target_conn.commit()

        df = df.replace({np.nan: None, pd.NaT: None})
        source_data = df[[
            "test_attribute_type_id", "test_type_id", "name", "datatype", "min_occurs", "max_occurs", "datatype_config", "sort_weight", "description", "preferred_handler", "hint", "group_name", "multiset_name", "creator", "date_created", "retired", "uuid"
        ]].to_dict(orient='records')

        info(f"Inserting {len(source_data)} records from commonlab.xlsx attribute_types sheet into target _labtest_attribute_type table...")
        insert_query = text(
            """
            INSERT INTO _labtest_attribute_type (test_attribute_type_id, test_type_id, name, datatype, min_occurs, max_occurs, datatype_config, sort_weight, description, preferred_handler, hint, group_name, multiset_name, creator, date_created, retired, uuid) 
            VALUES (:test_attribute_type_id, :test_type_id, :name, :datatype, :min_occurs, :max_occurs, :datatype_config, :sort_weight, :description, :preferred_handler, :hint, :group_name, :multiset_name, :creator, :date_created, :retired, :uuid)
            """)
        with target_engine.connect() as target_conn:
            target_conn.execute(insert_query, source_data)
            target_conn.commit()
        info("Import from commonlab.xlsx attribute_types sheet completed successfully.")
    else:
        warning("No data found in commonlab.xlsx attribute_types sheet.")


def extract_encounter_results(drop_create=False):
    target_engine = get_target_engine()
    if drop_create or not table_exists(target_engine, '_encounter_results'):
        create_encounter_results_table(target_engine, drop_create=drop_create)
    with target_engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE _encounter_results"))
        conn.commit()

        # Process each encounter type separately to avoid timeout
        encounter_types = [5, 7, 11]

        for enc_type in encounter_types:
            info(f"Inserting into _encounter_results table for encounter_type {enc_type}...")
            insert_query = text("""
                INSERT IGNORE INTO _encounter_results (encounter_id, person_id, obs_id, obs_group_id, parent_concept, obs_datetime, concept_id, question, value_numeric, value_text, value_datetime, value_coded, answer, location_id, creator, date_created, voided, voided_by, date_voided, void_reason)
                SELECT o.encounter_id, o.person_id, o.obs_id, o.obs_group_id, cnp.name as parent_concept, o.obs_datetime, o.concept_id, cn.name as question, o.value_numeric, o.value_text, o.value_datetime, o.value_coded, cn2.name as answer, o.location_id, o.creator, o.date_created, o.voided, o.voided_by, o.date_voided, o.void_reason from encounter e 
                INNER JOIN obs o ON o.encounter_id = e.encounter_id 
                INNER JOIN concept_name cn ON cn.concept_id = o.concept_id AND cn.locale = 'en' AND cn.concept_name_type = 'FULLY_SPECIFIED' 
                LEFT join concept_name cn2 ON cn2.concept_id = o.value_coded AND cn2.locale = 'en' AND cn2.concept_name_type = 'FULLY_SPECIFIED' 
                LEFT join obs op ON op.obs_id = o.obs_group_id 
                LEFT join concept_name cnp ON cnp.concept_id = op.concept_id AND cnp.locale = 'en' AND cnp.concept_name_type = 'FULLY_SPECIFIED' 
                WHERE e.encounter_type = :enc_type AND e.voided = 0 AND o.voided = 0
            """)
            conn.execute(insert_query, {"enc_type": enc_type})
            conn.commit()
            info(f"Completed importing encounter_type {enc_type}.")

        info("Import completed successfully.")


def extract_lab_group(drop_create=False):
    start_time = time.time()
    extract_labtest_type(drop_create=drop_create)
    extract_labtest_attribute_type(drop_create=drop_create)
    extract_encounter_results(drop_create=drop_create)
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")


##### Loading functions #####
def load_labtest_type():
    start_time = time.time()
    target_engine = get_target_engine()
    update_specimen_site_uuid_sql = """
        UPDATE global_property
        SET property_value = '31bf065e-0370-102d-b0e3-001ec94a0cc1'
        WHERE property = 'commonlabtest.specimenSiteConceptUuid'
    """
    update_specimen_type_uuid_sql = """
        UPDATE global_property
        SET property_value = '2da61322-bcc5-4c32-b412-1b1ef37f4a25'
        WHERE property = 'commonlabtest.specimenTypeConceptUuid'
    """
    insert_labtest_type_sql = """
        INSERT IGNORE INTO labtest_type (
            test_type_id, name, short_name, test_group, requires_specimen, reference_concept_id,
            description, creator, date_created, changed_by, date_changed, retired, retired_by,
            date_retired, retire_reason, uuid
        )
        SELECT
            test_type_id, name, short_name, test_group, requires_specimen, reference_concept_id,
            description, creator, date_created, changed_by, date_changed, retired, retired_by,
            date_retired, retire_reason, uuid
        FROM _labtest_type
    """
    with target_engine.connect() as conn:
        info("Loading data for labtest_type table...")
        conn.execute(text(update_specimen_site_uuid_sql))
        conn.execute(text(update_specimen_type_uuid_sql))
        conn.execute(text(insert_labtest_type_sql))
        conn.commit()
    info(f"Load labtest_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_labtest_sample():
    start_time = time.time()
    target_engine = get_target_engine()
    insert_labtest_sample_sql = """
        INSERT INTO labtest_sample (
            test_order_id, specimen_type, specimen_site, is_expirable, lab_sample_identifier, collector, status, creator, date_created, collection_date, processed_date, uuid
        )
        SELECT lt.test_order_id, 61 AS specimen_type, 491 AS specimen_site, 0, UUID() AS lab_sample_identifier, lt.creator, 'PROCESSED', lt.creator, lt.date_created, lt.date_created, lt.date_created, UUID()
        FROM labtest_test AS lt
        WHERE lt.voided = 0 AND NOT EXISTS (
          SELECT 1 FROM labtest_sample AS ls WHERE ls.test_order_id = lt.test_order_id AND ls.date_created = lt.date_created
        )
    """
    with target_engine.connect() as conn:
        info("Loading data for labtest_sample table...")
        conn.execute(text(insert_labtest_sample_sql))
        conn.commit()
    info(f"Load labtest completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_labtest():
    start_time = time.time()
    target_engine = get_target_engine()
    insert_labtest_test_sql = """
        INSERT IGNORE INTO labtest_test (
            test_order_id, test_type_id, lab_reference_number, creator, date_created,
            voided, voided_by, date_voided, void_reason, uuid
        )
        SELECT
            o.order_id, (CASE o.concept_id WHEN 410 THEN 5 ELSE 7 END) AS test_type_id,
            CONCAT(DATE_FORMAT(o.date_activated, '%Y%m%d'), '-', o.encounter_id) AS lab_reference_number,
            o.creator, o.date_created, o.voided, o.voided_by, o.date_voided, o.void_reason, UUID() AS uuid
        FROM orders AS o
        WHERE o.order_type_id = 3 AND o.voided = 0
    """
    with target_engine.connect() as conn:
        info("Loading data for labtest_test and labtest_sample tables...")
        conn.execute(text(insert_labtest_test_sql))
        conn.execute(text("""
            UPDATE labtest_test AS cat
            INNER JOIN orders AS o ON o.order_id = cat.test_order_id
            INNER JOIN encounter AS e ON e.encounter_id = o.encounter_id
            SET cat.date_changed = e.date_changed,
                cat.changed_by = e.changed_by
            WHERE e.date_changed IS NOT NULL
        """))
        conn.commit()
    info(f"Load labtest completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_labtest_attribute_type():
    start_time = time.time()
    target_engine = get_target_engine()
    insert_labtest_attribute_type_sql = """
        INSERT IGNORE INTO labtest_attribute_type (
            test_attribute_type_id, test_type_id, name, datatype, min_occurs, max_occurs, datatype_config, handler_config, sort_weight, description, creator, date_created,
            changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid, preferred_handler, hint, group_name, multiset_name
        )
        SELECT test_attribute_type_id, test_type_id, name, datatype, 0 AS min_occurs, NULL AS max_occurs, datatype_config, NULL AS handler_config, sort_weight, description, creator, date_created, NULL AS changed_by, NULL AS date_changed, retired, NULL AS retired_by, NULL AS date_retired, NULL AS retire_reason, uuid, preferred_handler, hint, group_name, multiset_name
        FROM _labtest_attribute_type
    """
    with target_engine.connect() as conn:
        info("Loading data for labtest_attribute_type table...")
        conn.execute(text(insert_labtest_attribute_type_sql))
        conn.execute(text("""
            UPDATE labtest_attribute_type
            SET description = CONCAT(description, ' - ', group_name)
            WHERE group_name IS NOT NULL
              AND description IS NULL
        """))
        conn.commit()
    info(f"Load labtest_attribute_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_labtest_attribute():
    start_time = time.time()
    target_engine = get_target_engine()
    TEXT_VALUE = (
        "CASE WHEN CHAR_LENGTH(TRIM(o.value_text)) > 255 "
        "THEN CONCAT(LEFT(TRIM(o.value_text), 250), '...') "
        "ELSE TRIM(o.value_text) END"
    )
    insert_sql_prefix = """
        INSERT IGNORE INTO labtest_attribute (
            test_attribute_id, test_order_id, attribute_type_id, value_reference, creator, date_created, voided, voided_by, date_voided, void_reason, uuid
        )
        select 0 as test_attribute_id, ct.test_order_id, tat.test_attribute_type_id, {value_reference}, o.creator, o.date_created, o.voided, o.voided_by, o.date_voided, o.void_reason, uuid() as uuid from obs as o
        inner join orders o2 ON o2.encounter_id = o.encounter_id
        inner join labtest_test ct on ct.test_order_id = o2.order_id
        {additional_joins}
        inner join labtest_attribute_type as tat on tat.test_type_id = 5{group_name_clause} and tat.name = '{attribute_name}'
        where o.concept_id = {concept_id} and o.voided = 0{additional_clause};
    """
    insert_queries = [
        # --- Ungrouped attributes ---
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE COLLECTED", concept_id=188,
            additional_joins="", group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="INVESTIGATION DATE", concept_id=427,
            additional_joins="", group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="LABORATORY INVESTIGATION NUMBER", concept_id=428,
            additional_joins="", group_name_clause="", additional_clause=" and o.obs_group_id is null"
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="PURPOSE OF INVESTIGATION", concept_id=425,
            additional_joins="inner join concept c on c.concept_id = o.value_coded",
            group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="REFERRING FACILITY", concept_id=498,
            additional_joins="", group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="REQUESTING MEDICAL FACILITY", concept_id=426,
            additional_joins="inner join concept c on c.concept_id = o.value_coded",
            group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="TUBERCULOSIS SAMPLE SOURCE", concept_id=67,
            additional_joins="inner join concept c on c.concept_id = o.value_coded",
            group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE OF REQUEST FOR LABORATORY INVESTIGATION", concept_id=589,
            additional_joins="", group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="LAB SPECIALIST NAME", concept_id=611,
            additional_joins="", group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="REFERRED BY", concept_id=497,
            additional_joins="", group_name_clause="", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="TUBERCULOSIS SPECIMEN COMMENTS", concept_id=149,
            additional_joins="", group_name_clause="", additional_clause=""
        ),

        # --- XPERT group ---
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="APPEARANCE OF SPECIMEN", concept_id=174,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311
            """,
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="MTB RESULT", concept_id=312,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311
            """,
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="ERROR CODE", concept_id=316,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311",
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=" and o.value_text regexp '^[0-9]*$'"
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="XPERT MTB BURDEN", concept_id=318,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311
            """,
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="ISONIAZID RESULT", concept_id=322,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311
            """,
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="RIFAMPICIN RESULT", concept_id=317,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311
            """,
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="TUBERCULOSIS TEST RESULT DATE", concept_id=68,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311",
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE OF SENDING TO CULTURE", concept_id=607,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311",
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE OF SENDING TO DST", concept_id=608,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311",
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="SENT TO CULTURE", concept_id=610,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311
            """,
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="SENT TO DST", concept_id=609,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 311
            """,
            group_name_clause=" and tat.group_name = 'XPERT'", additional_clause=""
        ),

        # --- HAIN group ---
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="APPEARANCE OF SPECIMEN", concept_id=174,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="MTB RESULT", concept_id=312,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="ERROR CODE", concept_id=316,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323",
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=" and o.value_text regexp '^[0-9]*$'"
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="XPERT MTB BURDEN", concept_id=318,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="ISONIAZID RESULT", concept_id=322,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="RIFAMPICIN RESULT", concept_id=317,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="TUBERCULOSIS TEST RESULT DATE", concept_id=68,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323",
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="ISONIAZID RESISTANCE", concept_id=545,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="RIFAMPICIN RESISTANCE", concept_id=546,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE OF SENDING TO CULTURE", concept_id=607,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323",
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE OF SENDING TO DST", concept_id=608,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323",
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="SENT TO CULTURE", concept_id=610,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="SENT TO DST", concept_id=609,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 323
            """,
            group_name_clause=" and tat.group_name = 'HAIN'", additional_clause=""
        ),

        # --- HAIN2 group ---
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="APPEARANCE OF SPECIMEN", concept_id=174,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="MTB RESULT", concept_id=312,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="ERROR CODE", concept_id=316,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414",
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=" and o.value_text regexp '^[0-9]*$'"
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="XPERT MTB BURDEN", concept_id=318,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="ISONIAZID RESULT", concept_id=322,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="RIFAMPICIN RESULT", concept_id=317,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="ETHAMBUTOL RESULT", concept_id=411,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="КМ/АМ/СМ RESULT", concept_id=412,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="MOX/OFX RESULT", concept_id=413,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="TUBERCULOSIS TEST RESULT DATE", concept_id=68,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414",
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE OF SENDING TO CULTURE", concept_id=607,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414",
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="SENT TO CULTURE", concept_id=610,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 414
            """,
            group_name_clause=" and tat.group_name = 'HAIN2'", additional_clause=""
        ),

        # --- CULTURE group ---
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="TUBERCULOSIS CULTURE METHOD", concept_id=151,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153
            """,
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="TUBERCULOSIS TEST RESULT DATE", concept_id=68,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153",
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="TYPE OF ORGANISM", concept_id=87,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153
            """,
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference=TEXT_VALUE, attribute_name="TYPE OF ORGANISM NON-CODED", concept_id=123,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153",
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="MGIT CULTURE RESULT", concept_id=519,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153
            """,
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="MT ID TEST", concept_id=517,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153
            """,
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="PLACE OF CULTURE", concept_id=532,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153
            """,
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="TUBERCULOSIS CULTURE RESULT", concept_id=152,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153
            """,
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="TYPE OF CULTURE REPORTED", concept_id=521,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153
            """,
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE OF INOCULATION OF CULTURE", concept_id=508,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153",
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="DATE OF REPORTING CULTURE RESULT", concept_id=522,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153",
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_numeric", attribute_name="LABORATORY NO", concept_id=509,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153",
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_numeric", attribute_name="COLONIES", concept_id=136,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153",
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_numeric", attribute_name="DAYS TO POSITIVITY", concept_id=106,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 153",
            group_name_clause=" and tat.group_name = 'CULTURE'", additional_clause=""
        ),

        # --- SMEAR group ---
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="APPEARANCE OF SPECIMEN", concept_id=174,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 69
            """,
            group_name_clause=" and tat.group_name = 'SMEAR'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="TUBERCULOSIS SMEAR RESULT", concept_id=54,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 69
            """,
            group_name_clause=" and tat.group_name = 'SMEAR'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_datetime", attribute_name="TUBERCULOSIS TEST RESULT DATE", concept_id=68,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 69",
            group_name_clause=" and tat.group_name = 'SMEAR'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="o.value_numeric", attribute_name="BACILLI", concept_id=43,
            additional_joins="inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 69",
            group_name_clause=" and tat.group_name = 'SMEAR'", additional_clause=""
        ),
        insert_sql_prefix.format(
            value_reference="c.uuid", attribute_name="MICROSCOPY RESULT", concept_id=400,
            additional_joins="""
                inner join concept c on c.concept_id = o.value_coded
                inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 69
            """,
            group_name_clause=" and tat.group_name = 'SMEAR'", additional_clause=""
        ),
    ]

    with target_engine.connect() as conn:
        info("Truncating table labtest_attribute")
        conn.execute(text("TRUNCATE TABLE labtest_attribute"))
        conn.commit()
        info("Loading data for labtest_attribute table...")
        for query in insert_queries:
            info(f"Executing query: {query}")
            conn.execute(text(query))
        conn.commit()
    info(f"Load labtest_attribute completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_labtest_attribute_for_drugs():
    start_time = time.time()
    target_engine = get_target_engine()

    # Queries from obs, joining labtest_test (test_type_id=7). The value_reference is the SUSCEPTIBLE (543) or
    # RESISTANT (183) answer, taken from the obs concept (see DST_RESULT_ANSWER_SQL).
    # Filtered by obs group concept 138 (TB DRUG SENSITIVITY TEST). Only DST MGIT tests get these fields: without the
    # test type filter every DST result was copied onto the COMMON TEST, which already gets it from the query below.
    insert_from_obs = """
        INSERT IGNORE INTO labtest_attribute (
            test_attribute_id, test_order_id, attribute_type_id, value_reference, creator, date_created, voided, voided_by, date_voided, void_reason, uuid
        )
        select 0, ct.test_order_id, cat.test_attribute_type_id, c2.uuid, o.creator, o.date_created, o.voided, o.voided_by, o.date_voided, o.void_reason, uuid() as uuid from obs as o
        inner join orders o2 ON o2.encounter_id = o.encounter_id
        inner join labtest_test ct on ct.test_order_id = o2.order_id and ct.test_type_id = 7
        inner join concept c on c.concept_id = o.value_coded
        inner join labtest_attribute_type cat on cat.test_type_id = 7 and cat.name = '{attribute_name}'
        inner join concept c2 on c2.concept_id = """ + DST_RESULT_ANSWER_SQL + """
        inner join obs o3 on o3.obs_id = o.obs_group_id and o3.concept_id = 138
        where o.concept_id in (118, 132) and o.value_coded = {drug_concept_id} and o.voided = 0;
    """
    insert_queries = [
        # --- From obs, test_type_id=7 ---
        insert_from_obs.format(attribute_name=name, drug_concept_id=drug) for name, drug in DST_DRUGS
    ]
    with target_engine.connect() as conn:
        info("Loading data for labtest_attribute table...")
        for query in insert_queries:
            info(f"Executing query: {query}")
            conn.execute(text(query))
        conn.commit()
    info(f"Load labtest_attribute_for_drugs completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

    # Queries from _encounter_results, joining labtest_test (test_type_id=5) - the DST results the app shows.
    # The value_reference is the SUSCEPTIBLE / RESISTANT answer (the obs concept itself is not an accepted answer, so
    # the app could not display it), and only the DST group's fields are used: the same field names also exist
    # ungrouped and in the HAIN group, which used to get a copy of every DST result.
    # Filtered by parent obs concept in (118, 138)
    insert_from_encounter_results = """
        INSERT IGNORE INTO labtest_attribute (
            test_attribute_id, test_order_id, attribute_type_id, value_reference, creator, date_created, voided, voided_by, date_voided, void_reason, uuid
        )
        select 0, ct.test_order_id, cat.test_attribute_type_id, c.uuid, o.creator, o.date_created, o.voided, o.voided_by, o.date_voided, o.void_reason, uuid() as uuid from _encounter_results as o
        inner join encounter e on e.encounter_id = o.encounter_id
        inner join encounter_type et on et.encounter_type_id = e.encounter_type
        inner join obs p on p.obs_id = o.obs_group_id
        inner join orders o2 on o2.encounter_id = e.encounter_id
        inner join concept c on c.concept_id = """ + DST_RESULT_ANSWER_SQL + """
        inner join labtest_test ct on ct.test_order_id = o2.order_id
        inner join labtest_attribute_type cat on cat.test_type_id = 5 and cat.group_name = 'DST' and cat.name = '{attribute_name}'
        where p.concept_id in (118, 138) and o.value_coded = {drug_concept_id};
    """
    insert_queries = [
        # --- From _encounter_results, test_type_id=5 ---
        insert_from_encounter_results.format(attribute_name=name, drug_concept_id=drug) for name, drug in DST_DRUGS
    ]
    with target_engine.connect() as conn:
        info("Loading data for labtest_attribute table...")
        for query in insert_queries:
            info(f"Executing query: {query}")
            conn.execute(text(query))
        conn.commit()
    info(f"Load labtest_attribute_for_drugs completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_lab_group():
    load_labtest_type()
    load_labtest_attribute_type()
    load_labtest()
    # After load_labtest(), so a new lab test gets its sample in the same run
    load_labtest_sample()
    load_labtest_attribute()
    load_labtest_attribute_for_drugs()


