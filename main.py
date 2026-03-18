# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import argparse
from sqlalchemy import text

from config.database import get_source_engine, get_target_engine
from jobs.migrate_openmrs import run_extract_job, run_transform_job, run_load_job
from utils.logger import info

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