import time

from sqlalchemy import text

from config.config import BATCH_SIZE
from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning

##### Extraction functions #####
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

def extract_obs_group(drop_create):
    start_time = time.time()
    extract_obs(drop_create=drop_create, resume=True)
    info(f"Obs table created successfully (Time: {time.time() - start_time:.2f} seconds)")

##### Loading functions #####
def load_obs_group():
    pass

