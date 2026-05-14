import time

from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_privilege(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_privilege_table(target_engine, drop_create=drop_create)
    info("Fetching data from source privilege table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _privilege"))
        target_conn.commit()

    insert_query = text("INSERT INTO _privilege (privilege, description, uuid) VALUES (:privilege, :description, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM privilege")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _privilege table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "privilege": row.privilege, "description": row.description, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source privilege table.")

def extract_role(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_role_table(target_engine, drop_create=drop_create)
    info("Fetching data from source role table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _role"))
        target_conn.commit()

    insert_query = text("INSERT INTO _role (role, description, uuid) VALUES (:role, :description, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM role")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _role table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "role": row.role, "description": row.description, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source role table.")

def extract_role_privilege(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_role_privilege_table(target_engine, drop_create=drop_create)
    info("Fetching data from source role_privilege table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _role_privilege"))
        target_conn.commit()

    insert_query = text("INSERT INTO _role_privilege (role, privilege) VALUES (:role, :privilege)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM role_privilege")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _role_privilege table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "role": row.role, "privilege": row.privilege
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source role_privilege table.")

def extract_role_role(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_role_role_table(target_engine, drop_create=drop_create)
    info("Fetching data from source role_role table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _role_role"))
        target_conn.commit()

    insert_query = text("INSERT INTO _role_role (parent_role, child_role) VALUES (:parent_role, :child_role)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM role_role")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _role_role table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "parent_role": row.parent_role, "child_role": row.child_role
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source role_role table.")

def extract_users(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_users_table(target_engine, drop_create=drop_create)
    info("Fetching data from source users table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _users"))
        target_conn.commit()

    insert_query = text("INSERT INTO _users (user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:user_id, :system_id, :username, :password, :salt, :secret_question, :secret_answer, :creator, :date_created, :changed_by, :date_changed, :person_id, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM users")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _users table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "user_id": row.user_id, "system_id": row.system_id, "username": row.username, "password": row.password, "salt": row.salt, "secret_question": row.secret_question, "secret_answer": row.secret_answer, "creator": row.creator, "date_created": row.date_created, "changed_by": row.changed_by, "date_changed": row.date_changed, "person_id": row.person_id, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source users table.")

def extract_user_property(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_user_property_table(target_engine, drop_create=drop_create)
    info("Fetching data from source user_property table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _user_property"))
        target_conn.commit()

    insert_query = text("INSERT INTO _user_property (user_id, property, property_value) VALUES (:user_id, :property, :property_value)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM user_property")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _user_property table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "user_id": row.user_id, "property": row.property, "property_value": row.property_value
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source user_property table.")

def extract_user_role(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_user_role_table(target_engine, drop_create=drop_create)
    info("Fetching data from source user_role table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _user_role"))
        target_conn.commit()

    insert_query = text("INSERT INTO _user_role (user_id, role) VALUES (:user_id, :role)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM user_role")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _user_role table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "user_id": row.user_id, "role": row.role
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source user_role table.")

def extract_provider(drop_create=False):
    target_engine = get_target_engine()
    if drop_create:
        create_provider_table(target_engine, drop_create=drop_create)

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
    info("User role table created successfully")
    extract_provider(drop_create=drop_create)
    info("Provider table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")

##### Loading functions #####
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
        conn.execute(text(select_insert_sql))
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

def load_provider():
    start_time = time.time()
    target_engine = get_target_engine()
    select_insert_sql = """
    INSERT IGNORE INTO provider (provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid)
    SELECT provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM _provider
    """
    with target_engine.connect() as conn:
        info("Loading data for provider table...")
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load provider completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_user_group():
    load_privilege()
    load_role()
    load_role_role()
    load_role_privilege()
    load_users()
    load_user_property()
    load_user_role()
    load_provider()