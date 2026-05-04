import time
from sqlalchemy import text
from config.database import get_target_engine
from utils.logger import info, warning

### User Group ###
def load_privilege():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO privilege (privilege, description, uuid)
    SELECT privilege, description, uuid FROM _privilege
    """
    with target_engine.connect() as conn:
        info("Loading data for privilege table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load privilege completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_role():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO role (role, description, uuid)
    SELECT role, description, uuid FROM _role
    """
    with target_engine.connect() as conn:
        info("Loading data for role table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load role completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_role_role():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO role_role (parent_role, child_role)
    SELECT parent_role, child_role FROM _role_role
    """
    with target_engine.connect() as conn:
        info("Loading data for role_role table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load role_role completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_role_privilege():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO role_privilege (role, privilege)
    SELECT role, privilege FROM _role_privilege
    """
    with target_engine.connect() as conn:
        info("Loading data for role_privilege table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load role_privilege completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_users():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO users (user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid FROM _users
    """
    with target_engine.connect() as conn:
        info("Loading data for users table...")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        try:
            conn.execute(text(select_insert_sql))
        finally:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        conn.commit()
    info(f"Load users completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_user_property():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO user_property (user_id, property, property_value) 
    SELECT user_id, property, property_value FROM _user_property
    """
    with target_engine.connect() as conn:
        info("Loading data for user_property table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load user_property completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_user_role():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO user_role (user_id, role)
    SELECT user_id, role FROM _user_role
    """
    with target_engine.connect() as conn:
        info("Loading data for user_role table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load user_role completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

### Address Hierarchy Group ###
def load_address_hierarchy_level():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO address_hierarchy_level (address_hierarchy_level_id, name, parent_level_id, address_field, uuid, required)
    SELECT address_hierarchy_level_id, name, parent_level_id, address_field, uuid, required FROM _address_hierarchy_level
    """
    with target_engine.connect() as conn:
        info("Loading data for address_hierarchy_level table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load address_hierarchy_level completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_address_hierarchy_entry():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO address_hierarchy_entry (address_hierarchy_entry_id, name, level_id, parent_id, user_generated_id, latitude, longitude, elevation, uuid)
    SELECT address_hierarchy_entry_id, name, level_id, parent_id, user_generated_id, latitude, longitude, elevation, uuid FROM _address_hierarchy_entry
    """
    with target_engine.connect() as conn:
        info("Loading data for address_hierarchy_entry table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load address_hierarchy_entry completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

### Concept Group ###
def load_concept_class():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_class (concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) 
    SELECT concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM _concept_class 
    WHERE name NOT IN (SELECT name FROM concept_class);
    """
    with target_engine.connect() as conn:
        info("Loading data for field table...")
        conn.execute(text(select_insert_sql))
        extra = """
        INSERT IGNORE INTO concept_class (name, description, creator, date_created, retired, uuid)
        VALUES ('Frequency', '', 1, CURRENT_TIMESTAMP(), 0, UUID());
        """
        conn.execute(text(extra))
        conn.commit()
    info(f"Load concept_class completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_datatype():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_datatype (concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid from _concept_datatype
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_datatype table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_datatype completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_name_tag():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_name_tag (concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid)
    SELECT concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid FROM _concept_name_tag
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_name_tag table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_name_tag completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept (concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid)
    SELECT concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid FROM _concept
    """
    with target_engine.connect() as conn:
        info("Loading data for concept table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_reference_source():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_reference_source(concept_source_id, name, description, hl7_code, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT concept_source_id, name, description, hl7_code, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM _concept_reference_source
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_reference_source table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_reference_source completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_answer():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_answer (concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, sort_weight, uuid)
    SELECT concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, sort_weight, uuid FROM _concept_answer
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_answer table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_answer completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_complex():
    start_time = time.time()
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        info("Loading concept_complex for field table...")
        conn.execute(text("""
        INSERT IGNORE INTO concept_complex (concept_id, handler) VALUES (198, 'BinaryDataHandler') # Scanned Lab Report
        """))
        conn.commit()
    info(f"Load concept_complex completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_name():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_name (concept_name_id, concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, voided, voided_by, date_voided, void_reason, uuid)
    SELECT concept_name_id, concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, voided, voided_by, date_voided, void_reason, uuid FROM _concept_name
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_name table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_name completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_description():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_description (concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid)
    SELECT concept_description_id, concept_id, replace(description, '\n', '') AS description, locale, creator, date_created, changed_by, date_changed, uuid FROM _concept_description
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_name table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_name completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_numeric():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_numeric (concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, allow_decimal) 
    SELECT concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise FROM _concept_numeric
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_numeric table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_numeric completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_reference_term():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_reference_term (concept_reference_term_id, concept_source_id, name, code, version, description, creator, date_created, date_changed, changed_by, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT concept_reference_term_id, concept_source_id, name, code, version, description, creator, date_created, date_changed, changed_by, retired, retired_by, date_retired, retire_reason, uuid FROM _concept_reference_term
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_reference_term table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_reference_term completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_map_tag():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_name_tag (concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid) 
    SELECT concept_name_tag_id, tag, description, creator, date_created, voided, voided_by, date_voided, void_reason, uuid FROM _concept_name_tag
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_map_tag table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_map_tag completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_concept_set():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO concept_set (concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) 
    SELECT concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid FROM _concept_set
    """
    with target_engine.connect() as conn:
        info("Loading data for concept_set table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load concept_set completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

### Drug Group ###
def load_drug():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO drug (drug_id, concept_id, name, combination, dosage_form, maximum_daily_dose, minimum_daily_dose, route, creator, date_created, retired, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid, strength)
    SELECT drug_id, concept_id, name, combination, dosage_form, maximum_daily_dose, minimum_daily_dose, route, creator, date_created, retired, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid,
        CASE
            WHEN dose_strength IS NULL THEN NULL
            WHEN units IS NULL OR units = '' THEN CAST(dose_strength AS CHAR)
            ELSE CONCAT(dose_strength, ' ', units)
        END AS strength
    FROM _drug
    """
    with target_engine.connect() as conn:
        info("Loading data for drug table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load drug completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_drug_ingredient():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO drug_ingredient (drug_id, ingredient_id, uuid)
    SELECT d.drug_id, di.ingredient_id, di.uuid
    FROM _drug_ingredient di
    INNER JOIN _drug d ON d.concept_id = di.concept_id
    """
    with target_engine.connect() as conn:
        info("Loading data for drug_ingredient table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load drug_ingredient completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_drug_order():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO drug_order (order_id, drug_inventory_id, dose, as_needed, quantity, dosing_instructions)
    SELECT order_id, drug_inventory_id, dose, prn AS as_needed, quantity, frequency
    FROM _drug_order
    """
    # equivalent_daily_dose units
    with target_engine.connect() as conn:
        info("Loading data for drug_order table...")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        try:
            conn.execute(text(select_insert_sql))
        finally:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        conn.commit()
    info(f"Load drug_order completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

### Form Group ###
def load_field():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO field (field_id, name, description, field_type, concept_id, table_name, attribute_name, default_value, select_multiple, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) 
    SELECT field_id, name, description, field_type, concept_id, table_name, attribute_name, default_value, select_multiple, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM _field
    """
    with target_engine.connect() as conn:
        info("Loading data for field table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load field completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_field_type():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO field_type (field_type_id, name, description, is_set, creator, date_created, uuid) 
    SELECT field_type_id, name, description, is_set, creator, date_created, uuid FROM _field_type
    """
    with target_engine.connect() as conn:
        info("Loading data for field_type table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load field_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_form_field():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO form_field (form_field_id, form_id, field_id, field_number, field_part, page_number, parent_form_field, min_occurs, max_occurs, required, changed_by, date_changed, creator, date_created, sort_weight, uuid) 
    SELECT form_field_id, form_id, field_id, field_number, field_part, page_number, parent_form_field, min_occurs, max_occurs, required, changed_by, date_changed, creator, date_created, sort_weight, uuid FROM _form_field
    """
    with target_engine.connect() as conn:
        info("Loading data for form_field table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load form_field completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_form():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO form (form_id, name, version, build, published, xslt, template, description, encounter_type, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retired_reason, uuid) 
    SELECT form_id, name, version, build, published, xslt, template, description, encounter_type, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retired_reason, uuid FROM _form
    """
    with target_engine.connect() as conn:
        info("Loading data for form table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load form completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_htmlformentry_html_form():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO htmlformentry_html_form (id, form_id, name, xml_data, creator, date_created, changed_by, date_changed, retired, uuid, description, retired_by, date_retired, retire_reason) 
    SELECT id, form_id, name, xml_data, creator, date_created, changed_by, date_changed, retired, uuid, description, retired_by, date_retired, retire_reason FROM _htmlformentry_html_form
    """
    with target_engine.connect() as conn:
        info("Loading data for htmlformentry_html_form table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load htmlformentry_html_form completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_program():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO program (program_id, concept_id, outcomes_concept_id, creator, date_created, changed_by, date_changed, retired, name, description, uuid) 
    SELECT program_id, concept_id, outcomes_concept_id, creator, date_created, changed_by, date_changed, retired, name, description, uuid FROM _program
    """
    with target_engine.connect() as conn:
        info("Loading data for program table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load program completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

### Program Group ###
def load_program_workflow():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO program_workflow (program_workflow_id, program_id, concept_id, creator, date_created, retired, changed_by, date_changed, uuid) 
    SELECT program_workflow_id, program_id, concept_id, creator, date_created, retired, changed_by, date_changed, uuid FROM _program_workflow
    """
    with target_engine.connect() as conn:
        info("Loading data for program_workflow table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load program_workflow completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_program_workflow_state():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO program_workflow_state (program_workflow_state_id, program_workflow_id, concept_id, `initial`, terminal, creator, date_created, retired, changed_by, date_changed, uuid) 
    SELECT program_workflow_state_id, program_workflow_id, concept_id, `initial`, terminal, creator, date_created, retired, changed_by, date_changed, uuid FROM _program_workflow_state
    """
    with target_engine.connect() as conn:
        info("Loading data for program_workflow_state table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load program_workflow_state completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

### HL7 Group ###
def load_hl7_source():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO hl7_source (hl7_source_id, name, description, creator, date_created, uuid) 
    SELECT hl7_source_id, name, description, creator, date_created, uuid FROM _hl7_source
    """
    with target_engine.connect() as conn:
        info("Loading data for hl7_source table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load hl7_source completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_hl7_error():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO hl7_in_error (hl7_in_error_id, hl7_source, hl7_source_key, hl7_data, error, error_details, date_created, uuid) 
    SELECT hl7_in_error_id, hl7_source, hl7_source_key, hl7_data, error, error_details, date_created, uuid FROM _hl7_in_error
    """
    with target_engine.connect() as conn:
        info("Loading data for hl7_in_error table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load hl7_error completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_hl7_queue():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO hl7_in_queue (hl7_in_queue_id, hl7_source, hl7_source_key, hl7_data, message_state, date_processed, error_msg, date_created, uuid) 
    SELECT hl7_in_queue_id, hl7_source, hl7_source_key, hl7_data, message_state, date_processed, error_msg, date_created, uuid FROM _hl7_in_queue
    """
    with target_engine.connect() as conn:
        info("Loading data for hl7_in_queue table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load hl7_queue completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_relationship_type():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO relationship_type (relationship_type_id, a_is_to_b, b_is_to_a, preferred, weight, description, creator, date_created, uuid, retired, retired_by, date_retired, retire_reason) 
    SELECT relationship_type_id, a_is_to_b, b_is_to_a, preferred, weight, description, creator, date_created, uuid, retired, retired_by, date_retired, retire_reason FROM _relationship_type
    """
    with target_engine.connect() as conn:
        info("Loading data for relationship_type table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load relationship_type completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_cohort():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO cohort (cohort_id, name, description, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) 
    SELECT cohort_id, name, description, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid FROM _cohort
    """
    with target_engine.connect() as conn:
        info("Loading data for cohort table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load cohort completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_global_property():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO global_property (property, property_value, description, uuid, datatype, datatype_config, preferred_handler, handler_config) 
    SELECT property, property_value, description, uuid, datatype, datatype_config, preferred_handler, handler_config FROM _global_property
    """
    with target_engine.connect() as conn:
        info("Loading data for global_property table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load global_property completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")
