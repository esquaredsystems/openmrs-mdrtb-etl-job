import time

from sqlalchemy import text

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning

##### Extraction functions #####
def extract_form(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_form_table(target_engine, drop_create=drop_create)
    info("Fetching data from source form table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _form"))
        target_conn.commit()

    insert_query = text("INSERT INTO _form (form_id, name, version, build, published, xslt, template, description, encounter_type, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retired_reason, uuid) VALUES (:form_id, :name, :version, :build, :published, :xslt, :template, :description, :encounter_type, :creator, :date_created, :changed_by, :date_changed, :retired, :retired_by, :date_retired, :retired_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM form")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _form table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "form_id": row.form_id, "name": row.name, "version": row.version, "build": row.build, "published": row.published, "xslt": row.xslt, "template": row.template, "description": row.description, "encounter_type": row.encounter_type, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retired_reason": row.retired_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source form table.")

def extract_form_field(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_form_field_table(target_engine, drop_create=drop_create)
    info("Fetching data from source form_field table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _form_field"))
        target_conn.commit()

    insert_query = text("INSERT INTO _form_field (form_field_id, form_id, field_id, field_number, field_part, page_number, parent_form_field, min_occurs, max_occurs, required, changed_by, date_changed, creator, date_created, sort_weight, uuid) VALUES (:form_field_id, :form_id, :field_id, :field_number, :field_part, :page_number, :parent_form_field, :min_occurs, :max_occurs, :required, :changed_by, :date_changed, :creator, :date_created, :sort_weight, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM form_field")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _form_field table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "form_field_id": row.form_field_id, "form_id": row.form_id, "field_id": row.field_id, "field_number": row.field_number, "field_part": row.field_part, "page_number": row.page_number, "parent_form_field": row.parent_form_field, "min_occurs": row.min_occurs, "max_occurs": row.max_occurs, "required": row.required, "changed_by": row.changed_by, "date_changed": row.date_changed, "creator": row.creator, "date_created": row.date_created, "sort_weight": row.sort_weight, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source form_field table.")

def extract_field(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_field_table(target_engine, drop_create=drop_create)
    info("Fetching data from source field table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _field"))
        target_conn.commit()

    insert_query = text("INSERT INTO _field (field_id, name, description, field_type, concept_id, table_name, attribute_name, default_value, select_multiple, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:field_id, :name, :description, :field_type, :concept_id, :table_name, :attribute_name, :default_value, :select_multiple, :creator, :date_created, :changed_by, :date_changed, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM field")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _field table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "field_id": row.field_id, "name": row.name, "description": row.description, "field_type": row.field_type, "concept_id": row.concept_id, "table_name": row.table_name, "attribute_name": row.attribute_name, "default_value": row.default_value, "select_multiple": row.select_multiple, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source field table.")

def extract_field_answer(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_field_answer_table(target_engine, drop_create=drop_create)
    info("Fetching data from source field_answer table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _field_answer"))
        target_conn.commit()

    insert_query = text("INSERT INTO _field_answer (field_id, answer_id, creator, date_created, uuid) VALUES (:field_id, :answer_id, :creator, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM field_answer")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _field_answer table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "field_id": row.field_id, "answer_id": row.answer_id, "creator": row.creator, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source field_answer table.")

def extract_field_type(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_field_type_table(target_engine, drop_create=drop_create)
    info("Fetching data from source field_type table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _field_type"))
        target_conn.commit()

    insert_query = text("INSERT INTO _field_type (field_type_id, name, description, is_set, creator, date_created, uuid) VALUES (:field_type_id, :name, :description, :is_set, :creator, :date_created, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM field_type")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _field_type table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "field_type_id": row.field_type_id, "name": row.name, "description": row.description, "is_set": row.is_set, "creator": row.creator, "date_created": row.date_created, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source field_type table.")

def extract_htmlformentry_html_form(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_htmlformentry_html_form(target_engine, drop_create=drop_create)
    info("Fetching data from source htmlformentry_html_form table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _htmlformentry_html_form"))
        target_conn.commit()

    insert_query = text("INSERT INTO _htmlformentry_html_form (id, form_id, name, xml_data, creator, date_created, changed_by, date_changed, retired, uuid, description, retired_by, date_retired, retire_reason) VALUES (:id, :form_id, :name, :xml_data, :creator, :date_created, :changed_by, :date_changed, :retired, :uuid, :description, :retired_by, :date_retired, :retire_reason)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM htmlformentry_html_form")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _htmlformentry_html_form table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "id": row.id, "form_id": row.form_id, "name": row.name, "xml_data": row.xml_data, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "retired": row.retired, "uuid": row.uuid, "description": row.description, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source htmlformentry_html_form table.")

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

##### Loading functions #####
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

def load_form_group():
    load_field()
    load_field_type()
    load_form_field()
    load_form()
    load_htmlformentry_html_form()
