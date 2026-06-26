import time
from datetime import date

from config.config import BATCH_SIZE
from config.database import get_source_engine, get_target_engine, set_foreign_key_checks
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_obs(drop_create=False, resume=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create or not table_exists(target_engine, '_obs'):
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
    info("Obs table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")

##### Loading functions #####
def load_obs(resume=False):
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO obs (
        obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id,
        obs_group_id, accession_number, value_group_id, value_coded,
        value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier,
        value_text, value_complex, comments, creator, date_created, voided, voided_by,
        date_voided, void_reason, uuid
    )
    SELECT
        obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id,
        obs_group_id, accession_number, value_group_id,
        CASE
            WHEN value_boolean = 1 THEN 1
            WHEN value_boolean = 0 THEN 0
            ELSE value_coded
        END,
        value_coded_name_id, value_drug, value_datetime, value_numeric, 
        value_modifier, value_text, value_complex, comments, creator, date_created, 
        voided, voided_by, date_voided, void_reason, uuid
    FROM _obs
    WHERE date_created <= CURRENT_DATE()
      AND date_created >= :date_start
      AND date_created < :date_end
    """

    with target_engine.connect() as conn:
        info("Loading data for obs table...")
        current_year = date.today().year

        if resume:
            start_year = current_year
            info(f"Resume enabled. Loading obs from start of current year ({current_year}).")
        else:
            row = conn.execute(text("SELECT DATE(MIN(date_created)) FROM _obs WHERE date_created <= CURRENT_DATE()")).fetchone()
            if row is None or row[0] is None:
                warning("No data found in staging _obs table to load.")
                return
            start_year = row[0].year
            info(f"Loading obs year-by-year from {start_year} to {current_year}.")

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        try:
            for year in range(start_year, current_year + 1):
                date_start = date(year, 1, 1)
                date_end = date(year + 1, 1, 1)
                params = {
                    "date_start": date_start,
                    "date_end": date_end,
                }

                info(f"Loading obs records for year {year} (from {date_start} to {date_end})...")
                conn.execute(text(select_insert_sql), params)
                conn.commit()
        finally:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

    info(f"Load obs completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_obs_group(resume=False):
    start_time = time.time()
    load_obs(resume=resume)
    info(f"Load obs group completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")
