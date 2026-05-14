import time
from datetime import date

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
      AND date_created >= :year_start
      AND date_created < :year_end
    """

    with target_engine.connect() as conn:
        info("Loading data for obs table...")
        date_bounds_sql = text("SELECT DATE(MIN(date_created)) AS first_date, CURRENT_TIMESTAMP() AS current FROM _obs WHERE date_created <= CURRENT_DATE()")
        date_bounds = conn.execute(date_bounds_sql).fetchone()
        first_date = date_bounds[0] if date_bounds and date_bounds[0] is not None else None
        current_date = date_bounds[1] if date_bounds and date_bounds[1] is not None else date.today()

        if first_date is None:
            warning("No data found in staging _obs table to load.")
            return

        start_date = first_date
        if resume:
            fetch_latest_sql = text("SELECT DATE(COALESCE(MAX(date_created), '1900-01-01')) AS latest FROM obs WHERE date_created <= CURRENT_DATE()")
            last_entry = conn.execute(fetch_latest_sql).fetchone()
            last_date = last_entry[0] if last_entry and last_entry[0] is not None else "1900-01-01"
            info(f"Resume enabled. Using latest target obs date_created: {last_date}")

            if isinstance(last_date, str):
                last_date = date.fromisoformat(last_date)
            start_date = max(first_date, last_date)

        start_year = start_date.year
        current_year = current_date.year
        info(f"Loading obs year-by-year from {start_year} to {current_year}")

        for year in range(start_year, current_year + 1):
            year_start = date(year, 1, 1)
            if year == start_year and start_date > year_start:
                year_start = start_date

            year_end = date(year + 1, 1, 1) if year < current_year else date(current_year + 1, 1, 1)
            params = {
                "year_start": year_start,
                "year_end": year_end,
            }

            info(f"Loading obs records for year {year} (from {year_start} to {year_end})...")
            conn.execute(text(select_insert_sql), params)
            conn.commit()

    info(f"Load obs completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def load_obs_group():
    start_time = time.time()
    load_obs(resume=True)
    info(f"Load obs group completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")
