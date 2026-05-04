import argparse
import time

from sqlalchemy import text

from config.database import get_source_engine, get_target_engine
from etl.address_hierarchy import extract_address_hierarchy_group, load_address_hierarchy_group
from etl.cohort import extract_cohort_group, load_cohort_group
from etl.concept import extract_concept_group, load_concept_group, transform_concept_group
from etl.drug import extract_drug_group, load_drug_group
from etl.encounter import extract_encounter_group, load_encounter_group, transform_encounter_group
from etl.form import extract_form_group, load_form_group
from etl.hl7 import extract_hl7_group, load_hl7_group
from etl.location import extract_location_group, load_location_group
from etl.misc import extract_misc_group, load_misc_group
from etl.obs import extract_obs_group, load_obs_group
from etl.orders import extract_orders_group, load_orders_group
from etl.patient import extract_patient_group, load_patient_group
from etl.program import extract_program_group, load_program_group
from etl.report import extract_report_group, load_report_group
from etl.user import extract_user_group, load_user_group
from utils.logger import info


def run_extract_job(hard_reset=False):
    start_time = time.time()
    extract_address_hierarchy_group(hard_reset)
    extract_cohort_group(hard_reset)
    extract_concept_group(hard_reset)
    extract_drug_group(hard_reset)
    extract_form_group(hard_reset)
    extract_hl7_group(hard_reset)
    extract_location_group(hard_reset)
    extract_orders_group(hard_reset)
    extract_program_group(hard_reset)
    extract_user_group(hard_reset)
    extract_report_group(hard_reset)
    extract_misc_group(hard_reset)
    extract_patient_group(hard_reset)
    extract_encounter_group(hard_reset)
    extract_obs_group(hard_reset)
    info(f"Extraction job completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def run_transform_job():
    start_time = time.time()
    transform_encounter_group()
    transform_concept_group()
    info(f"Transformation job completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def run_load_job():
    start_time = time.time()
    load_user_group()
    load_address_hierarchy_group()
    load_cohort_group()
    load_concept_group()
    load_drug_group()
    # load_form_group()
    # load_hl7_group()
    # load_location_group()
    # load_orders_group()
    # load_program_group()
    # load_report_group()
    # load_misc_group()
    # load_patient_group()
    # load_encounter_group()
    # load_obs_group()
    info(f"Load job completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenMRS MDR-TB ETL Job")
    parser.add_argument("--extract", action="store_true", help="Run the extraction job")
    parser.add_argument("--transform", action="store_true", help="Run the transformation job")
    parser.add_argument("--load", action="store_true", help="Run the load job")
    parser.add_argument("--hard-reset", action="store_true", help="Hard reset (Drop-Create) the tables before extraction")
    args = parser.parse_args()

    info("Connecting to source database...")
    source = get_source_engine()
    info("Connecting to target database...")
    target = get_target_engine()
    with source.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1, "Connection to source database failed"
        info("Source connection successful")
    with target.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1, "Connection to target database failed"
        info("Target connection successful")
    
    if args.extract:
        run_extract_job(hard_reset=args.hard_reset)

    if args.transform:
        run_transform_job()

    # if args.load:
    run_load_job()
