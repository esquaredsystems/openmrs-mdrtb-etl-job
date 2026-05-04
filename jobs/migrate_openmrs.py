from etl.openmrs16_extractor import *
from etl.openmrs25_loader import *
from etl.openmrs_transformer import transform_provider, transform_encounter_provider, transform_concept_reference_term
from utils.logger import info
import time

##### Extraction functions #####
def extract_address_hierarchy_group(drop_create):
    start_time = time.time()
    extract_address_hierarchy_level(drop_create=drop_create)
    info("Extracting data from source database and inserting into target database...")
    extract_address_hierarchy_entry(drop_create=drop_create)
    info(f"Address hierarchy level and entry tables created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_cohort_group(drop_create):
    start_time = time.time()
    extract_cohort(drop_create=drop_create)
    info("Cohort table created successfully")
    extract_cohort_member(drop_create=drop_create)
    info(f"Cohort member table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_concept_group(drop_create):
    start_time = time.time()
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
    info(f"Concept word table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_drug_group(drop_create):
    start_time = time.time()
    extract_drug(drop_create=drop_create)
    info("Drug table created successfully")
    extract_drug_ingredient(drop_create=drop_create)
    info("Drug ingredient table created successfully")
    extract_drug_order(drop_create=drop_create)
    info(f"Drug order table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_form_group(drop_create):
    start_time = time.time()
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
    info(f"HTML Form Entry table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_hl7_group(drop_create):
    start_time = time.time()
    extract_hl7_in_error(drop_create=drop_create)
    info("HL7 in error table created successfully")
    extract_hl7_in_queue(drop_create=drop_create)
    info("HL7 in queue table created successfully")
    extract_hl7_source(drop_create=drop_create)
    info(f"HL7 source table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_location_group(drop_create):
    start_time = time.time()
    extract_location(drop_create=drop_create)
    info(f"Location table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_orders_group(drop_create):
    start_time = time.time()
    extract_orders(drop_create=drop_create)
    info("Orders table created successfully")
    extract_order_type(drop_create=drop_create)
    info(f"Order type table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_program_group(drop_create):
    start_time = time.time()
    extract_program(drop_create=drop_create)
    info("Program table created successfully")
    extract_program_workflow(drop_create=drop_create)
    info("Program workflow table created successfully")
    extract_program_workflow_state(drop_create=drop_create)
    info("Program workflow state table created successfully")
    extract_relationship_type(drop_create=drop_create)
    info(f"Relationship type table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_user_group(drop_create):
    start_time = time.time()
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
    extract_user_role(drop_create=drop_create)
    info(f"User role table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_report_group(drop_create):
    start_time = time.time()
    extract_report_object(drop_create=drop_create)
    info("Report object table created successfully")
    extract_report_schema_xml(drop_create=drop_create)
    info("Report schema XML table created successfully")
    extract_serialized_object(drop_create=drop_create)
    info(f"Serialized object table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_misc_group(drop_create):
    start_time = time.time()
    extract_global_property(drop_create=drop_create)
    info("Global property table created successfully")
    extract_note(drop_create=drop_create)
    info("Note table created successfully")
    extract_notification_alert(drop_create=drop_create)
    info("Notification alert table created successfully")
    extract_notification_alert_recipient(drop_create=drop_create)
    info(f"Notification alert recipient table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_patient_group(drop_create):
    start_time = time.time()
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
    info(f"Person name table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_encounter_group(drop_create):
    start_time = time.time()
    extract_encounter_type(drop_create=drop_create)
    info("Encounter type table created successfully")
    extract_encounter(drop_create=drop_create)
    info("Encounter table created successfully")
    extract_provider(drop_create=drop_create)
    info("Provider table created successfully")
    extract_encounter_provider(drop_create=drop_create)
    info(f"Encounter provider table created successfully (Time: {time.time() - start_time:.2f} seconds)")

def extract_obs_group(drop_create):
    start_time = time.time()
    extract_obs(drop_create=drop_create, resume=True)
    info(f"Obs table created successfully (Time: {time.time() - start_time:.2f} seconds)")


##### Loading functions #####
def load_user_group():
    load_privilege()
    load_role()
    load_role_role()
    load_role_privilege()
    load_users()
    load_user_property()
    load_user_role()

def load_address_hierarchy_group():
    load_address_hierarchy_level()
    load_address_hierarchy_entry()

def load_cohort_group():
    load_cohort()

def load_concept_group():
    # Sequenced to avoid foreign key constraint error
    load_concept_datatype()
    load_concept_class()
    load_concept()
    load_concept_name_tag()
    load_concept_reference_source()
    load_concept_reference_term()
    load_concept_answer()
    load_concept_complex()
    load_concept_name()
    load_concept_description()
    load_concept_numeric()

def load_drug_group():
    load_drug()
    load_drug_ingredient()
    load_drug_order()

def load_form_group():
    load_field()
    load_field_type()
    load_form_field()
    load_form()
    load_htmlformentry_html_form()

def load_hl7_group():
    load_hl7_source()
    load_hl7_error()
    load_hl7_queue()

def load_location_group():
    pass

def load_orders_group():
    pass

def load_program_group():
    load_program()
    load_program_workflow()
    load_program_workflow_state()

def load_report_group():
    pass

def load_misc_group():
    load_global_property()
    load_relationship_type()

def load_patient_group():
    pass

def load_encounter_group():
    pass

def load_obs_group():
    pass


##### Caller functions #####
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
    transform_provider()
    # transform_encounter_provider()
    transform_concept_reference_term()
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
