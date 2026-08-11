import argparse
import sys
from etl.address_hierarchy import *
from etl.cohort import *
from etl.concept import *
from etl.drug import *
from etl.encounter import *
from etl.fk_check import *
from etl.form import *
from etl.hl7 import *
from etl.lab import *
from etl.location import *
from etl.misc import *
from etl.obs import *
from etl.orders import *
from etl.patient import *
from etl.program import *
from etl.report import *
from etl.user import *
from utils.logger import info
from config.database import _get_required_env


def pre_etl_job():
    """
    Perform pre-ETL tasks including disabling foreign key checks and ensuring all 
    text columns in the target database use the utf8mb4 character set.
    """
    target_engine = get_target_engine()
    database_name = _get_required_env("TARGET_DB_NAME")
    with target_engine.connect() as conn:
        conn.execute(text(f"SET FOREIGN_KEY_CHECKS = 0"))
        conn.commit()
        # Make sure all text columns are utf8mb4
        select_query = f"""
        select concat('ALTER TABLE ', c.TABLE_NAME, ' MODIFY COLUMN ', c.COLUMN_NAME, ' ', c.COLUMN_TYPE, ' CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;') as q from information_schema.`COLUMNS` c
        where c.TABLE_SCHEMA = '{database_name}' and c.DATA_TYPE in ('char', 'text', 'varchar') and c.COLUMN_NAME <> 'uuid' and c.CHARACTER_SET_NAME <> 'utf8mb4'
        """
        queries = conn.execute(text(select_query))
        for query in queries:
            try:
                alter_query = query[0]
                info(f"Executing query: {alter_query}")
                conn.execute(text(alter_query))
                conn.commit()
            except Exception as e:
                warning(f"Error executing query: {e}")
        conn.execute(text(f"SET FOREIGN_KEY_CHECKS = 1"))
        conn.commit()
    info("Pre-ETL job completed successfully")


def run_extract_job(hard_reset=False):
    """
    Execute the data extraction phase of the ETL job.
    Calls individual extraction functions for various data groups.
    """
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
    extract_lab_group(hard_reset)
    info(f"Extraction job completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def run_load_job():
    """
    Execute the data loading phase of the ETL job.
    Loads the transformed data into the target database in the correct order.
    """
    start_time = time.time()
    load_user_group()
    load_address_hierarchy_group()
    load_cohort_group()
    load_concept_group()
    load_location_group()
    load_drug_group()
    load_form_group()
    load_hl7_group()
    load_program_group()
    load_report_group()
    load_misc_group()
    load_patient_group()
    load_encounter_group()
    load_obs_group()
    load_orders_group()
    # Must run after load_orders_group(): drug_order.order_id -> orders.order_id
    # (drug orders are inserted into `orders` inside load_orders_group()).
    load_drug_order()
    load_lab_group()
    info(f"Load job completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")


def post_etl_job():
    """
    Perform post-ETL tasks such as data cleanup, fixing obsolete location IDs,
    and ensuring necessary privileges for anonymous users are set.
    """
    # Placeholder for any post-ETL tasks like cleanup, indexing, etc.
    # Update location IDs for obsolete location
    with get_target_engine().connect() as conn:
        # Replace the location 205 with the correct one
        conn.execute(text("UPDATE encounter SET location_id = 214 WHERE location_id = 205;"))
        conn.execute(text("UPDATE obs SET location_id = 214 WHERE location_id = 205;"))
        conn.execute(text("UPDATE patient_identifier SET location_id = 214 WHERE location_id = 205;"))
        conn.execute(text("UPDATE patient_program SET location_id = 214 WHERE location_id = 205;"))

        # Set concepts for the program outcomes in program tables
        conn.execute(text("UPDATE program SET outcomes_concept_id = 30 where program_id = 1;"))
        conn.execute(text("UPDATE program SET outcomes_concept_id = 275 where program_id = 2;"))

        # CRITICAL! These are the privileges that are required for users to operate
        conn.execute(text("""
            INSERT IGNORE INTO role_privilege (role, privilege) VALUES
                ('Anonymous', 'Get Concepts'),
                ('Anonymous', 'Get Locations'),
                ('Anonymous', 'View Concepts'),
                ('Anonymous', 'View Locations'),
                ('Anonymous', 'Get Global Properties'),

                ('Authenticated', 'Edit Users'),
                ('Authenticated', 'Get Provider Attribute Types'),
                ('Authenticated', 'Get Location Attribute Types'),
                ('Authenticated', 'Get Concepts'),
                
                ('Lab View', 'View LabTest Metadata'),
                ('Lab View', 'View LabTest Orders'),
                ('Lab View', 'View LabTest Results'),
                ('Lab View', 'View LabTest Samples'),
                
                ('Lab Technician', 'Add LabTest Orders'),
                ('Lab Technician', 'Add LabTest Results'),
                ('Lab Technician', 'Add LabTest Samples'),
                ('Lab Technician', 'Edit LabTest Orders'),
                ('Lab Technician', 'Edit LabTest Results'),
                ('Lab Technician', 'Edit LabTest Samples'),
                ('Lab Technician', 'Delete LabTest Results'),
                ('Lab Technician', 'Delete LabTest Samples'),

                ('волонтер', 'Add Visits'),
                ('волонтер', 'Delete Visits'),
                ('волонтер', 'Edit DOTS-MDR Data'),
                ('волонтер', 'Edit Orders'),
                ('волонтер', 'Edit Patient Programs'),
                ('волонтер', 'Edit Visits'),
                ('волонтер', 'Get Care Settings'),
                ('волонтер', 'Get Encounter Roles'),
                ('волонтер', 'Get Encounters'),
                ('волонтер', 'Get Forms'),
                ('волонтер', 'Get Observations'),
                ('волонтер', 'Get Order Sets'),
                ('волонтер', 'Get Order Types'),
                ('волонтер', 'Get Orders'),
                ('волонтер', 'Get Patient Cohorts'),
                ('волонтер', 'Get Patient Identifiers'),
                ('волонтер', 'Get Patient Programs'),
                ('волонтер', 'Get Patients'),
                ('волонтер', 'Get People'),
                ('волонтер', 'Get Person Attribute Types'),
                ('волонтер', 'Get Privileges'),
                ('волонтер', 'Get Privileges'),
                ('волонтер', 'Get Programs'),
                ('волонтер', 'Get Relationships'),
                ('волонтер', 'Get Visits'),
                ('волонтер', 'Manage MDR'),
                ('волонтер', 'Manage Relationships'),
                ('волонтер', 'Patient Dashboard - View Demographics Section'),
                ('волонтер', 'Patient Dashboard - View Encounters Section'),
                ('волонтер', 'Patient Dashboard - View Forms Section'),
                ('волонтер', 'Patient Dashboard - View Graphs Section'),
                ('волонтер', 'Patient Dashboard - View Overview Section'),
                ('волонтер', 'Patient Dashboard - View Regimen Section'),
                ('волонтер', 'Patient Dashboard - View Visits Section'),
                ('волонтер', 'Patient Dashboard - View Programs Section'),
                ('волонтер', 'Patient Dashboard - View Relationships Section'),
                ('волонтер', 'View Relationships'), 
                ('волонтер', 'View Patient Programs');
        """))

        conn.commit()

        #TODO: Delete these privileges from privilege table as well as role_privilege:
        # Add Test Result, Delete Test Result, Edit Test Result, Manage Lab, View Lab Entry, View Lab Functions, View Lab Search


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenMRS MDR-TB ETL Job")
    parser.add_argument("--extract", action="store_true", help="Run the extraction job")
    parser.add_argument("--load", action="store_true", help="Run the load job")
    parser.add_argument("--hard-reset", action="store_true",
                        help="Hard reset (Drop-Create) the tables before extraction")
    parser.add_argument("--check-integrity", action="store_true",
                        help="Scan the target database for foreign key violations (read-only). "
                             "Run alone to check only; combine with --extract/--load to check afterwards")
    args = parser.parse_args()

    info("Connecting to source database...")
    source = get_source_engine()
    with source.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1, "Connection to source database failed"
        info("Source connection successful")

    info("Connecting to target database...")
    target = get_target_engine()
    with target.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1, "Connection to target database failed"
        info("Target connection successful")

    if args.extract or args.load:
        pre_etl_job()
        if args.extract:
            run_extract_job(hard_reset=args.hard_reset)
        if args.load:
            run_load_job()
        post_etl_job()

    if args.check_integrity:
        run_fk_check()
