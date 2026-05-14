import time

from config.config import BATCH_SIZE
from config.database import get_source_engine, get_target_engine
from models.schema_models import *
from utils.logger import info, warning


##### Extraction functions #####
def extract_drug(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_drug_table(target_engine, drop_create=drop_create)
    info("Fetching data from source drug table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _drug"))
        target_conn.commit()

    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM drug")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _drug table...")
        insert_query = text("INSERT INTO _drug (drug_id, concept_id, name, combination, dosage_form, dose_strength, maximum_daily_dose, minimum_daily_dose, route, units, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) VALUES (:drug_id, :concept_id, :name, :combination, :dosage_form, :dose_strength, :maximum_daily_dose, :minimum_daily_dose, :route, :units, :creator, :date_created, :retired, :retired_by, :date_retired, :retire_reason, :uuid)")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "drug_id": row.drug_id, "concept_id": row.concept_id, "name": row.name, "combination": row.combination, "dosage_form": row.dosage_form, "dose_strength": row.dose_strength, "maximum_daily_dose": row.maximum_daily_dose, "minimum_daily_dose": row.minimum_daily_dose, "route": row.route, "units": row.units, "creator": row.creator, "date_created": row.date_created, "retired": row.retired, "retired_by": row.retired_by, "date_retired": row.date_retired, "retire_reason": row.retire_reason, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source drug table.")

def extract_drug_ingredient(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_drug_ingredient_table(target_engine, drop_create=drop_create)
    info("Fetching data from source drug_ingredient table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _drug_ingredient"))
        target_conn.commit()

    insert_query = text("INSERT INTO _drug_ingredient (concept_id, ingredient_id, uuid) VALUES (:concept_id, :ingredient_id, :uuid)")
    with source_engine.connect() as source_conn:
        source_data = source_conn.execute(text("SELECT * FROM drug_ingredient")).fetchall()
    if source_data:
        info(f"Inserting {len(source_data)} records into target _drug_ingredient table...")
        with target_engine.connect() as target_conn:
            for row in source_data:
                target_conn.execute(insert_query, {
                    "concept_id": row.concept_id, "ingredient_id": row.ingredient_id, "uuid": row.uuid
                })
            target_conn.commit()
        info("Import completed successfully.")
    else:
        warning("No data found in source drug_ingredient table.")

def extract_drug_order(drop_create=False):
    source_engine = get_source_engine()
    target_engine = get_target_engine()
    if drop_create:
        create_drug_order_table(target_engine, drop_create=drop_create)
    info("Fetching data from source drug_order table...")
    with target_engine.connect() as target_conn:
        target_conn.execute(text("TRUNCATE TABLE _drug_order"))
        target_conn.commit()

    insert_query = text("INSERT INTO _drug_order (order_id, drug_inventory_id, dose, equivalent_daily_dose, units, frequency, prn, complex, quantity) VALUES (:order_id, :drug_inventory_id, :dose, :equivalent_daily_dose, :units, :frequency, :prn, :complex, :quantity)")
    
    with source_engine.connect() as source_conn:
        # Using execution_options(yield_per=BATCH_SIZE) for batching
        result = source_conn.execution_options(yield_per=BATCH_SIZE).execute(text("SELECT * FROM drug_order"))
        
        batch = []
        count = 0
        with target_engine.connect() as target_conn:
            for row in result:
                batch.append({
                    "order_id": row.order_id, "drug_inventory_id": row.drug_inventory_id, "dose": row.dose, "equivalent_daily_dose": row.equivalent_daily_dose, "units": row.units, "frequency": row.frequency, "prn": row.prn, "complex": row.complex, "quantity": row.quantity
                })
                count += 1
                if len(batch) >= BATCH_SIZE:
                    info(f"Inserting batch of {len(batch)} records into target _drug_order table...")
                    target_conn.execute(insert_query, batch)
                    target_conn.commit()
                    batch = []
            if batch:
                info(f"Inserting final batch of {len(batch)} records into target _drug_order table...")
                target_conn.execute(insert_query, batch)
                target_conn.commit()
    
    if count > 0:
        info(f"Import completed successfully. Total {count} records imported.")
    else:
        warning("No data found in source drug_order table.")

def extract_drug_group(drop_create):
    start_time = time.time()
    extract_drug(drop_create=drop_create)
    info("Drug table created successfully")
    extract_drug_ingredient(drop_create=drop_create)
    info("Drug ingredient table created successfully")
    extract_drug_order(drop_create=drop_create)
    info("Drug order table created successfully")
    info(f"Extraction completed in {time.time() - start_time:.2f} seconds")

##### Loading functions #####
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
        conn.execute(text(select_insert_sql))
        conn.commit()
    info(f"Load drug_order completed successfully (Total Time: {time.time() - start_time:.2f} seconds)")

def load_drug_group():
    load_drug()
    load_drug_ingredient()
    load_drug_order()


##### Transform functions #####
def transform_drug_group():
    """
    Updates the dose_limit_units column in the drug table based on the strength suffix.
    """
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        conn.execute(text("""
            UPDATE drug SET dose_limit_units = CASE
                WHEN strength LIKE '%mg' THEN (
                    SELECT concept_id FROM concept_name WHERE name = 'MILLIGRAM(S)' AND locale = 'en' LIMIT 1
                )
                WHEN strength LIKE '%ml' THEN (
                    SELECT concept_id FROM concept_name WHERE name = 'MILLILITRE(S)' AND locale = 'en' LIMIT 1
                )
                WHEN strength LIKE '%tab' OR strength LIKE '%tab(s)' THEN (
                    SELECT concept_id FROM concept_name WHERE name = 'FILM COATED TABLET' AND locale = 'en' LIMIT 1
                )
                ELSE dose_limit_units
            END
            WHERE (dose_limit_units IS NULL OR dose_limit_units = 0)
            AND (strength LIKE '%mg' OR strength LIKE '%ml' OR strength LIKE '%tab' OR strength LIKE '%tab(s)')
        """))
        conn.commit()
    info("Drug dose_limit_units updated based on strength.")
