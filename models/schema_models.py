from sqlalchemy import text

from utils.logger import info


def create_address_hierarchy_entry_table(engine, drop_create=False):
    """
    Create the _address_hierarchy_entry table in the database.
    :param drop_create: If true, drop the _address_hierarchy_entry table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _address_hierarchy_entry"))
        create_query = "CREATE TABLE _address_hierarchy_entry (address_hierarchy_entry_id int(10) NOT NULL, name varchar(160), level_id int(10) NOT NULL, parent_id int(10), user_generated_id varchar(11), latitude double(22,0), longitude double(22,0), elevation double(22,0), uuid char(38), PRIMARY KEY (address_hierarchy_entry_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()

def create_address_hierarchy_level_table(engine, drop_create=False):
    """
    Create the _address_hierarchy_level table in the database.
    :param drop_create: If true, drop the _address_hierarchy_level table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _address_hierarchy_level"))
        create_query = "CREATE TABLE _address_hierarchy_level (address_hierarchy_level_id int(10) NOT NULL, name varchar(160), parent_level_id int(10), address_field varchar(50), uuid char(38), required bit(1), PRIMARY KEY (address_hierarchy_level_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()

def create_cohort_table(engine, drop_create=False):
    """
    Create the _cohort table in the database.
    :param drop_create: If true, drop the _cohort table before recreating it.
    """
    info("Dropping and recreating _cohort table...")
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _cohort"))
        create_query = "CREATE TABLE _cohort (cohort_id int(10) NOT NULL, name varchar(255) NOT NULL, description varchar(1000), creator int(10) NOT NULL, date_created datetime NOT NULL, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), changed_by int(10), date_changed datetime, uuid char(38), PRIMARY KEY (cohort_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_cohort_member_table(engine, drop_create=False):
    """
    Create the _cohort_member table in the database.
    :param drop_create: If true, drop the _cohort_member table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _cohort_member"))
        create_query = "CREATE TABLE _cohort_member (cohort_id int(10) NOT NULL, patient_id int(10) NOT NULL, PRIMARY KEY (cohort_id,patient_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_table(engine, drop_create=False):
    """
    Create the _concept table in the database.
    :param drop_create: If true, drop the _concept table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept"))
        create_query = "CREATE TABLE _concept (concept_id int(10) NOT NULL, retired tinyint(1) NOT NULL, short_name varchar(255), description text, form_text text, datatype_id int(10) NOT NULL, class_id int(10) NOT NULL, is_set tinyint(1) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, version varchar(50), changed_by int(10), date_changed datetime, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (concept_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_answer_table(engine, drop_create=False):
    """
    Create the _concept_answer table in the database.
    :param drop_create: If true, drop the _concept_answer table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_answer"))
        create_query = "CREATE TABLE _concept_answer (concept_answer_id int(10) NOT NULL, concept_id int(10) NOT NULL, answer_concept int(10), answer_drug int(10), creator int(10) NOT NULL, date_created datetime NOT NULL, sort_weight double(22,0), uuid char(38), PRIMARY KEY (concept_answer_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_class_table(engine, drop_create=False):
    """
    Create the _concept_class table in the database.
    :param drop_create: If true, drop the _concept_class table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_class"))
        create_query = "CREATE TABLE _concept_class (concept_class_id int(10) NOT NULL, name varchar(255) NOT NULL, description varchar(255) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (concept_class_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_complex_table(engine, drop_create=False):
    """
    Create the _concept_complex table in the database.
    :param drop_create: If true, drop the _concept_complex table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_complex"))
        create_query = "CREATE TABLE _concept_complex (concept_id int(10) NOT NULL, handler varchar(255), PRIMARY KEY (concept_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_datatype_table(engine, drop_create=False):
    """
    Create the _concept_datatype table in the database.
    :param drop_create: If true, drop the _concept_datatype table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_datatype"))
        create_query = "CREATE TABLE _concept_datatype (concept_datatype_id int(10) NOT NULL, name varchar(255) NOT NULL, hl7_abbreviation varchar(3), description varchar(255) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, retired tinyint(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38) NOT NULL, PRIMARY KEY (concept_datatype_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_derived_table(engine, drop_create=False):
    """
    Create the _concept_derived table in the database.
    :param drop_create: If true, drop the _concept_derived table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_derived"))
        create_query = "CREATE TABLE _concept_derived (concept_id int(10) NOT NULL, rule mediumtext, compile_date datetime DEFAULT NULL, compile_status varchar(255), class_name varchar(1024), PRIMARY KEY (concept_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_description_table(engine, drop_create=False):
    """
    Create the _concept_description table in the database.
    :param drop_create: If true, drop the _concept_description table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_description"))
        create_query = "CREATE TABLE _concept_description (concept_description_id int(10) NOT NULL, concept_id int(10) NOT NULL, description text NOT NULL, locale varchar(50) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, uuid char(38), PRIMARY KEY (concept_description_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_map_table(engine, drop_create=False):
    """
    Create the _concept_map table in the database.
    :param drop_create: If true, drop the _concept_map table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_map"))
        create_query = "CREATE TABLE _concept_map (concept_map_id int(10) NOT NULL, source int(10) DEFAULT NULL, source_code varchar(255), comment varchar(255), creator int(10) NOT NULL, date_created datetime NOT NULL, concept_id int(10) NOT NULL, uuid char(38), PRIMARY KEY (concept_map_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_name_table(engine, drop_create=False):
    """
    Create the _concept_name table in the database.
    :param drop_create: If true, drop the _concept_name table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_name"))
        create_query = "CREATE TABLE _concept_name (concept_name_id int(10) NOT NULL, concept_id int(10), name varchar(255) NOT NULL, locale varchar(50) NOT NULL, locale_preferred tinyint(1), creator int(10) NOT NULL, date_created datetime NOT NULL, concept_name_type varchar(50), voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38), PRIMARY KEY (concept_name_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_name_tag_table(engine, drop_create=False):
    """
    Create the _concept_name_tag table in the database.
    :param drop_create: If true, drop the _concept_name_tag table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_name_tag"))
        create_query = "CREATE TABLE _concept_name_tag (concept_name_tag_id int(10) NOT NULL, tag varchar(50) NOT NULL, description text NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38), PRIMARY KEY (concept_name_tag_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_name_tag_map_table(engine, drop_create=False):
    """
    Create the _concept_name_tag_map table in the database.
    :param drop_create: If true, drop the _concept_name_tag_map table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_name_tag_map"))
        create_query = "CREATE TABLE _concept_name_tag_map (concept_name_id int(10) NOT NULL, concept_name_tag_id int(10) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_numeric_table(engine, drop_create=False):
    """
    Create the _concept_numeric table in the database.
    :param drop_create: If true, drop the _concept_numeric table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_numeric"))
        create_query = "CREATE TABLE _concept_numeric (concept_id int(10) NOT NULL, hi_absolute double(22,0), hi_critical double(22,0), hi_normal double(22,0), low_absolute double(22,0), low_critical double(22,0), low_normal double(22,0), units varchar(50), precise tinyint(1) NOT NULL, PRIMARY KEY (concept_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_reference_source_table(engine, drop_create=False):
    """
    Create the _concept_reference_source table in the database.
    :param drop_create: If true, drop the _concept_reference_source table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_reference_source"))
        create_query = "CREATE TABLE _concept_reference_source (concept_source_id int(10) NOT NULL, name varchar(50) NOT NULL, description text NOT NULL, hl7_code varchar(50), creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (concept_source_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_set_table(engine, drop_create=False):
    """
    Create the _concept_set table in the database.
    :param drop_create: If true, drop the _concept_set table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_set"))
        create_query = "CREATE TABLE _concept_set (concept_set_id int(10) NOT NULL, concept_id int(10) NOT NULL, concept_set int(10) NOT NULL, sort_weight double(22,0), creator int(10) NOT NULL, date_created datetime NOT NULL, uuid char(38), PRIMARY KEY (concept_set_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_word_table(engine, drop_create=False):
    """
    Create the _concept_word table in the database.
    :param drop_create: If true, drop the _concept_word table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_word"))
        create_query = "CREATE TABLE _concept_word (concept_word_id int(10) NOT NULL, concept_id int(10) NOT NULL, word varchar(50) NOT NULL, locale varchar(20) NOT NULL, concept_name_id int(10) NOT NULL, weight double(22,0), PRIMARY KEY (concept_word_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_drug_table(engine, drop_create=False):
    """
    Create the _drug table in the database.
    :param drop_create: If true, drop the _drug table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _drug"))
        create_query = "CREATE TABLE _drug (drug_id int(10) NOT NULL, concept_id int(10) NOT NULL, name varchar(255), combination bit(1) NOT NULL, dosage_form int(10), dose_strength double(22,0), maximum_daily_dose double(22,0), minimum_daily_dose double(22,0), route int(10), units varchar(50), creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, changed_by int(10), date_changed datetime, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (drug_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_drug_ingredient_table(engine, drop_create=False):
    """
    Create the _drug_ingredient table in the database.
    :param drop_create: If true, drop the _drug_ingredient table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _drug_ingredient"))
        create_query = "CREATE TABLE _drug_ingredient (concept_id int(10) NOT NULL, ingredient_id int(10) NOT NULL, uuid char(38), PRIMARY KEY (concept_id,ingredient_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_drug_order_table(engine, drop_create=False):
    """
    Create the _drug_order table in the database.
    :param drop_create: If true, drop the _drug_order table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _drug_order"))
        create_query = "CREATE TABLE _drug_order (order_id int(10) NOT NULL, drug_inventory_id int(10), dose double(22,0), equivalent_daily_dose double(22,0), units varchar(255), frequency varchar(255), prn bit(1) NOT NULL, complex bit(1) NOT NULL, quantity int(10), PRIMARY KEY (order_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_encounter_table(engine, drop_create=False):
    """
    Create the _encounter table in the database.
    :param drop_create: If true, drop the _encounter table before recreating it.
    """
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _encounter"))
        create_query = "CREATE TABLE _encounter (encounter_id int(10) NOT NULL, encounter_type int(10) NOT NULL, patient_id int(10) NOT NULL, location_id int(10), form_id int(10), encounter_datetime datetime NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), changed_by int(10), date_changed datetime, visit_id int(10), uuid char(38), PRIMARY KEY (encounter_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()
