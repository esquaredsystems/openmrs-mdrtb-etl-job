import time
import numpy as np
import pandas as pd

from config.database import get_target_engine
from models.schema_models import *
from utils.helpers import get_labtest_attribute_type_data, get_labtest_type_data
from utils.logger import info, warning


##### Extraction functions #####
def extract_labtest_type(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
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
    if drop_create:
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
    truncate_labtest_sample_sql = "TRUNCATE labtest_sample"
    insert_labtest_sample_sql = """
        INSERT INTO labtest_sample (
            test_order_id, specimen_type, specimen_site, is_expirable, lab_sample_identifier,
            collector, status, creator, date_created, collection_date, processed_date, uuid
        )
        SELECT test_order_id, 61 AS specimen_type, 491 AS specimen_site, 0, UUID() AS lab_sample_identifier, creator, 'PROCESSED', creator, date_created, date_created, date_created, UUID() FROM labtest_test
        WHERE voided = 0
    """
    with target_engine.connect() as conn:
        info("Loading data for labtest_test and labtest_sample tables...")
        conn.execute(text(insert_labtest_test_sql))
        conn.execute(text(truncate_labtest_sample_sql))
        conn.execute(text(insert_labtest_sample_sql))
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
        SELECT test_attribute_type_id, test_type_id, name, 'org.openmrs.customdatatype.datatype.FreeTextDatatype' AS datatype, 0 AS min_occurs, NULL AS max_occurs, datatype_config, NULL AS handler_config, sort_weight, description, creator, date_created, NULL AS changed_by, NULL AS date_changed, retired, NULL AS retired_by, NULL AS date_retired, NULL AS retire_reason, uuid, preferred_handler, hint, group_name, multiset_name
        FROM _labtest_attribute_type
    """
    with target_engine.connect() as conn:
        info("Loading data for labtest_attribute_type table...")
        conn.execute(text(insert_labtest_attribute_type_sql))
        conn.commit()
    info(f"Load labtest_attribute_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_lab_group():
    load_labtest_type()
    load_labtest()
    load_labtest_attribute_type()


##### Transform functions #####
def transform_lab_group():
    start_time = time.time()
    target_engine = get_target_engine()
    update_attribute_type_description_sql = """
        UPDATE labtest_attribute_type
        SET description = CONCAT(description, ' - ', group_name)
        WHERE group_name IS NOT NULL
          AND description IS NULL
    """
    update_labtest_test_changed_metadata_sql = """
        UPDATE labtest_test AS cat
        INNER JOIN orders AS o ON o.order_id = cat.test_order_id
        INNER JOIN encounter AS e ON e.encounter_id = o.encounter_id
        SET cat.date_changed = e.date_changed,
            cat.changed_by = e.changed_by
        WHERE e.date_changed IS NOT NULL
    """
    with target_engine.connect() as conn:
        info("Transforming labtest tables...")
        conn.execute(text(update_attribute_type_description_sql))
        conn.execute(text(update_labtest_test_changed_metadata_sql))
        conn.commit()
    info(f"Transform lab group completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")
