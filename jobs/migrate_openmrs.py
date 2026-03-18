from etl.openmrs16_extractor import *
from utils.logger import info
from sqlalchemy import text


def extract_address_hierarchy_group(drop_create):
    extract_address_hierarchy_level(drop_create=drop_create)
    info("Extracting data from source database and inserting into target database...")
    extract_address_hierarchy_entry(drop_create=drop_create)
    info("Address hierarchy level and entry tables created successfully")


def extract_cohort_group(drop_create):
    extract_cohort(drop_create=drop_create)
    info("Cohort table created successfully")
    extract_cohort_member(drop_create=drop_create)
    info("Cohort member table created successfully")


def extract_concept_group(drop_create):
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


def extract_drug_group(drop_create):
    extract_drug(drop_create=drop_create)
    info("Drug table created successfully")
    extract_drug_ingredient(drop_create=drop_create)
    info("Drug ingredient table created successfully")
    extract_drug_order(drop_create=drop_create)
    info("Drug order table created successfully")


def extract_form_group(drop_create):
    extract_form(drop_create=drop_create)
    info("Form table created successfully")
    extract_form_field(drop_create=drop_create)
    info("Form field table created successfully")
    extract_field(drop_create=drop_create)
    info("Field table created successfully")
    extract_field_answer(drop_create=drop_create)
    info("Field answer table created successfully")
    extract_field_type(drop_create=drop_create)
    info("Field type table created successfully")
    extract_htmlformentry_html_form(drop_create=drop_create)
    info("HTML Form Entry table created successfully")


def extract_hl7_group(drop_create):
    extract_hl7_in_error(drop_create=drop_create)
    info("HL7 in error table created successfully")
    extract_hl7_in_queue(drop_create=drop_create)
    info("HL7 in queue table created successfully")
    extract_hl7_source(drop_create=drop_create)
    info("HL7 source table created successfully")


def extract_location_group(drop_create):
    extract_location(drop_create=drop_create)
    info("Location table created successfully")


def extract_orders_group(drop_create):
    extract_orders(drop_create=drop_create)
    info("Orders table created successfully")
    extract_order_type(drop_create=drop_create)
    info("Order type table created successfully")


def extract_program_group(drop_create):
    extract_program(drop_create=drop_create)
    info("Program table created successfully")
    extract_program_workflow(drop_create=drop_create)
    info("Program workflow table created successfully")
    extract_program_workflow_state(drop_create=drop_create)
    info("Program workflow state table created successfully")
    extract_relationship_type(drop_create=drop_create)
    info("Relationship type table created successfully")


def extract_user_group(drop_create):
    extract_privilege(drop_create=drop_create)
    info("Privilege table created successfully")
    extract_role(drop_create=drop_create)
    info("Role table created successfully")
    extract_role_privilege(drop_create=drop_create)
    info("Role privilege table created successfully")
    extract_role_role(drop_create=drop_create)
    info("Role role table created successfully")
    extract_users(drop_create=drop_create)
    info("Users table created successfully")
    extract_user_property(drop_create=drop_create)
    info("User property table created successfully")


def extract_report_group(drop_create):
    extract_report_object(drop_create=drop_create)
    info("Report object table created successfully")
    extract_report_schema_xml(drop_create=drop_create)
    info("Report schema XML table created successfully")
    extract_serialized_object(drop_create=drop_create)
    info("Serialized object table created successfully")


def extract_misc_group(drop_create):
    extract_global_property(drop_create=drop_create)
    info("Global property table created successfully")
    extract_note(drop_create=drop_create)
    info("Note table created successfully")
    extract_notification_alert(drop_create=drop_create)
    info("Notification alert table created successfully")
    extract_notification_alert_recipient(drop_create=drop_create)
    info("Notification alert recipient table created successfully")


def extract_patient_group(drop_create):
    extract_patient(drop_create=drop_create)
    info("Patient table created successfully")
    extract_patient_identifier(drop_create=drop_create)
    info("Patient identifier table created successfully")
    extract_patient_identifier_type(drop_create=drop_create)
    info("Patient identifier type table created successfully")
    extract_patient_program(drop_create=drop_create)
    info("Patient program table created successfully")
    extract_person(drop_create=drop_create)
    info("Person table created successfully")
    extract_person_address(drop_create=drop_create)
    info("Person address table created successfully")
    extract_person_attribute(drop_create=drop_create)
    info("Person attribute table created successfully")
    extract_person_attribute_type(drop_create=drop_create)
    info("Person attribute type table created successfully")
    extract_person_name(drop_create=drop_create)
    info("Person name table created successfully")


def extract_encounter_group(drop_create):
    extract_encounter_type(drop_create=drop_create)
    info("Encounter type table created successfully")
    extract_encounter(drop_create=drop_create)
    info("Encounter table created successfully")
    extract_encounter_provider(drop_create=drop_create)
    info("Encounter provider table created successfully")


def extract_obs_group(drop_create):
    extract_obs(drop_create=drop_create, resume=True)
    info("Obs table created successfully")


def run_job(hard_reset=False):

    info("Connecting to source database...")
    source = get_source_engine()

    info("Connecting to target database...")
    target = get_target_engine()

    with source.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM privilege"))
        info(f"Privilege count: {list(result)[0][0]}")

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
