from config.database import get_source_engine, get_target_engine
from etl.openmrs16_extractor import *
from utils.logger import info
from sqlalchemy import text


def run_extract_job(include_tables=[]):
    if 'address_hierarchy' in include_tables:
        extract_address_hierarchy_level(drop_create=True)
        info("Extracting data from source database and inserting into target database...")
        extract_address_hierarchy_entry(drop_create=True)
        info("Address hierarchy level and entry tables created successfully")

    if 'cohort' in include_tables:
        extract_cohort(drop_create=True)
        info("Cohort table created successfully")
        extract_cohort_member(drop_create=True)
        info("Cohort member table created successfully")

    if 'concept' in include_tables:
        extract_concept(drop_create=True)
        info("Concept table created successfully")
        extract_concept_answer(drop_create=True)
        info("Concept answer table created successfully")
        extract_concept_class(drop_create=True)
        info("Concept class table created successfully")
        extract_concept_complex(drop_create=True)
        info("Concept complex table created successfully")
        extract_concept_datatype(drop_create=True)
        info("Concept datatype table created successfully")
        extract_concept_derived(drop_create=True)
        info("Concept derived table created successfully")
        extract_concept_description(drop_create=True)
        info("Concept description table created successfully")
        extract_concept_map(drop_create=True)
        info("Concept map table created successfully")
        extract_concept_name(drop_create=True)
        info("Concept name table created successfully")
        extract_concept_name_tag(drop_create=True)
        info("Concept name tag table created successfully")
        extract_concept_numeric(drop_create=True)
        info("Concept numeric table created successfully")
        extract_concept_reference_source(drop_create=True)
        info("Concept reference source table created successfully")
        extract_concept_set(drop_create=True)
        info("Concept set table created successfully")
        extract_concept_word(drop_create=True)
        info("Concept word table created successfully")

    if 'drug' in include_tables:
        extract_drug(drop_create=True)
        info("Drug table created successfully")
        extract_drug_ingredient(drop_create=True)
        info("Drug ingredient table created successfully")
        extract_drug_order(drop_create=True)
        info("Drug order table created successfully")

    if 'encounter' in include_tables:
        extract_encounter(drop_create=True)
        info("Encounter table created successfully")


def run_job():

    info("Connecting to source database...")
    source = get_source_engine()

    info("Connecting to target database...")
    target = get_target_engine()

    with source.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM privilege"))
        info(f"Source patient count: {list(result)[0][0]}")

    run_extract_job(include_tables=[
        # "address_hierarchy",
        # "cohort",
        # "concept",
        # "drug",
        "encounter",
    ])
