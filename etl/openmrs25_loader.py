import time
from sqlalchemy import text
from config.database import get_target_engine
from utils.logger import info, warning


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

def load_scheduler_task_config():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO scheduler_task_config (task_config_id, name, description, schedulable_class, start_time, start_time_pattern, repeat_interval, start_on_startup, started, created_by, date_created, changed_by, date_changed, uuid, last_execution_time) 
    SELECT task_config_id, name, description, schedulable_class, start_time, start_time_pattern, repeat_interval, start_on_startup, started, created_by, date_created, changed_by, date_changed, uuid, last_execution_time FROM _scheduler_task_config
    """
    with target_engine.connect() as conn:
        info("Loading data for scheduler_task_config table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load scheduler_task_config completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")
