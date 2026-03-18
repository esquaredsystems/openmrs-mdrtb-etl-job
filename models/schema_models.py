from sqlalchemy import text

from utils.logger import info


def create_address_hierarchy_entry_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _address_hierarchy_entry"))
        create_query = "CREATE TABLE _address_hierarchy_entry (address_hierarchy_entry_id int(10) NOT NULL, name varchar(160), level_id int(10) NOT NULL, parent_id int(10), user_generated_id varchar(11), latitude double(22,0), longitude double(22,0), elevation double(22,0), uuid char(38), PRIMARY KEY (address_hierarchy_entry_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_address_hierarchy_level_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _address_hierarchy_level"))
        create_query = "CREATE TABLE _address_hierarchy_level (address_hierarchy_level_id int(10) NOT NULL, name varchar(160), parent_level_id int(10), address_field varchar(50), uuid char(38), required bit(1), PRIMARY KEY (address_hierarchy_level_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_cohort_member_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _cohort_member"))
        create_query = "CREATE TABLE _cohort_member (cohort_id int(10) NOT NULL, patient_id int(10) NOT NULL, PRIMARY KEY (cohort_id,patient_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_cohort_table(engine, drop_create=False):
    info("Dropping and recreating _cohort table...")
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _cohort"))
        create_query = "CREATE TABLE _cohort (cohort_id int(10) NOT NULL, name varchar(255) NOT NULL, description varchar(1000), creator int(10) NOT NULL, date_created datetime NOT NULL, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), changed_by int(10), date_changed datetime, uuid char(38), PRIMARY KEY (cohort_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_answer_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_answer"))
        create_query = "CREATE TABLE _concept_answer (concept_answer_id int(10) NOT NULL, concept_id int(10) NOT NULL, answer_concept int(10), answer_drug int(10), creator int(10) NOT NULL, date_created datetime NOT NULL, sort_weight double(22,0), uuid char(38), PRIMARY KEY (concept_answer_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_class_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_class"))
        create_query = "CREATE TABLE _concept_class (concept_class_id int(10) NOT NULL, name varchar(255) NOT NULL, description varchar(255) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (concept_class_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_complex_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_complex"))
        create_query = "CREATE TABLE _concept_complex (concept_id int(10) NOT NULL, handler varchar(255), PRIMARY KEY (concept_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_datatype_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_datatype"))
        create_query = "CREATE TABLE _concept_datatype (concept_datatype_id int(10) NOT NULL, name varchar(255) NOT NULL, hl7_abbreviation varchar(3), description varchar(255) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, retired tinyint(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38) NOT NULL, PRIMARY KEY (concept_datatype_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_derived_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_derived"))
        create_query = "CREATE TABLE _concept_derived (concept_id int(10) NOT NULL, rule mediumtext, compile_date datetime DEFAULT NULL, compile_status varchar(255), class_name varchar(1024), PRIMARY KEY (concept_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_description_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_description"))
        create_query = "CREATE TABLE _concept_description (concept_description_id int(10) NOT NULL, concept_id int(10) NOT NULL, description text NOT NULL, locale varchar(50) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, uuid char(38), PRIMARY KEY (concept_description_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_map_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_map"))
        create_query = "CREATE TABLE _concept_map (concept_map_id int(10) NOT NULL, source int(10) DEFAULT NULL, source_code varchar(255), comment varchar(255), creator int(10) NOT NULL, date_created datetime NOT NULL, concept_id int(10) NOT NULL, uuid char(38), PRIMARY KEY (concept_map_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_name_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_name"))
        create_query = "CREATE TABLE _concept_name (concept_name_id int(10) NOT NULL, concept_id int(10), name varchar(255) NOT NULL, locale varchar(50) NOT NULL, locale_preferred tinyint(1), creator int(10) NOT NULL, date_created datetime NOT NULL, concept_name_type varchar(50), voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38), PRIMARY KEY (concept_name_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_name_tag_map_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_name_tag_map"))
        create_query = "CREATE TABLE _concept_name_tag_map (concept_name_id int(10) NOT NULL, concept_name_tag_id int(10) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_name_tag_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_name_tag"))
        create_query = "CREATE TABLE _concept_name_tag (concept_name_tag_id int(10) NOT NULL, tag varchar(50) NOT NULL, description text NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38), PRIMARY KEY (concept_name_tag_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_numeric_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_numeric"))
        create_query = "CREATE TABLE _concept_numeric (concept_id int(10) NOT NULL, hi_absolute double(22,0), hi_critical double(22,0), hi_normal double(22,0), low_absolute double(22,0), low_critical double(22,0), low_normal double(22,0), units varchar(50), precise tinyint(1) NOT NULL, PRIMARY KEY (concept_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_reference_source_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_reference_source"))
        create_query = "CREATE TABLE _concept_reference_source (concept_source_id int(10) NOT NULL, name varchar(50) NOT NULL, description text NOT NULL, hl7_code varchar(50), creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (concept_source_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_set_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_set"))
        create_query = "CREATE TABLE _concept_set (concept_set_id int(10) NOT NULL, concept_id int(10) NOT NULL, concept_set int(10) NOT NULL, sort_weight double(22,0), creator int(10) NOT NULL, date_created datetime NOT NULL, uuid char(38), PRIMARY KEY (concept_set_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept"))
        create_query = "CREATE TABLE _concept (concept_id int(10) NOT NULL, retired tinyint(1) NOT NULL, short_name varchar(255), description text, form_text text, datatype_id int(10) NOT NULL, class_id int(10) NOT NULL, is_set tinyint(1) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, version varchar(50), changed_by int(10), date_changed datetime, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (concept_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_concept_word_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _concept_word"))
        create_query = "CREATE TABLE _concept_word (concept_word_id int(10) NOT NULL, concept_id int(10) NOT NULL, word varchar(50) NOT NULL, locale varchar(20) NOT NULL, concept_name_id int(10) NOT NULL, weight double(22,0), PRIMARY KEY (concept_word_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_drug_ingredient_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _drug_ingredient"))
        create_query = "CREATE TABLE _drug_ingredient (concept_id int(10) NOT NULL, ingredient_id int(10) NOT NULL, uuid char(38), PRIMARY KEY (concept_id,ingredient_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_drug_order_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _drug_order"))
        create_query = "CREATE TABLE _drug_order (order_id int(10) NOT NULL, drug_inventory_id int(10), dose double(22,0), equivalent_daily_dose double(22,0), units varchar(255), frequency varchar(255), prn bit(1) NOT NULL, complex bit(1) NOT NULL, quantity int(10), PRIMARY KEY (order_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_drug_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _drug"))
        create_query = "CREATE TABLE _drug (drug_id int(10) NOT NULL, concept_id int(10) NOT NULL, name varchar(255), combination bit(1) NOT NULL, dosage_form int(10), dose_strength double(22,0), maximum_daily_dose double(22,0), minimum_daily_dose double(22,0), route int(10), units varchar(50), creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, changed_by int(10), date_changed datetime, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (drug_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_encounter_provider_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _encounter_provider"))
        create_query = "CREATE TABLE _encounter_provider (encounter_provider_id int(10) AUTO_INCREMENT NOT NULL, encounter_id int(10) NOT NULL, provider_id int(10) NOT NULL, encounter_role_id int(10) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, voided bit(1) NOT NULL, date_voided datetime, voided_by int(10), void_reason varchar(255), uuid char(38) NOT NULL, PRIMARY KEY (encounter_provider_id), UNIQUE KEY _encounter_provider_unique (encounter_id,provider_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_encounter_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _encounter"))
        create_query = "CREATE TABLE _encounter (encounter_id int(10) NOT NULL, encounter_type int(10) NOT NULL, patient_id int(10) NOT NULL, provider_id int(10) NOT NULL, location_id int(10), form_id int(10), encounter_datetime datetime NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), changed_by int(10), date_changed datetime, visit_id int(10), uuid char(38), PRIMARY KEY (encounter_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_encounter_type_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _encounter_type"))
        create_query = "CREATE TABLE _encounter_type (encounter_type_id int(10) NOT NULL, name varchar(50) NOT NULL, description text, creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (encounter_type_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_field_answer_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _field_answer"))
        create_query = "CREATE TABLE _field_answer (field_id int(10) NOT NULL, answer_id int(10) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, uuid char(38), PRIMARY KEY (field_id,answer_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_field_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _field"))
        create_query = "CREATE TABLE _field (field_id int(10) NOT NULL, name varchar(255) NOT NULL, description text, field_type int(10), concept_id int(10), table_name varchar(50), attribute_name varchar(50), default_value text, select_multiple bit(1) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (field_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()

def create_field_type_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _field_type"))
        create_query = "CREATE TABLE _field_type ( field_type_id int(10) NOT NULL, name varchar(50), description text, is_set bit(1) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, uuid char(38), PRIMARY KEY (field_type_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_form_field_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _form_field"))
        create_query = "CREATE TABLE _form_field (form_field_id int(10) NOT NULL, form_id int(10) NOT NULL, field_id int(10) NOT NULL, field_number int(10), field_part varchar(5), page_number int(10), parent_form_field int(10), min_occurs int(10), max_occurs int(10), required bit(1) NOT NULL, changed_by int(10), date_changed datetime, creator int(10) NOT NULL, date_created datetime NOT NULL, sort_weight double(22,0), uuid char(38), PRIMARY KEY (form_field_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_form_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _form"))
        create_query = "CREATE TABLE _form (form_id int(10) NOT NULL, name varchar(255) NOT NULL, version varchar(50) NOT NULL, build int(10), published bit(1) NOT NULL, xslt text, template text, description text, encounter_type int(10), creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retired_reason varchar(255), uuid char(38), PRIMARY KEY (form_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_global_property_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _global_property"))
        create_query = "CREATE TABLE _global_property (property varchar(1024) NOT NULL, property_value text, description text, uuid char(38), datatype varchar(255), datatype_config text, preferred_handler varchar(255), handler_config text) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_hl7_in_error_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _hl7_in_error"))
        create_query = "CREATE TABLE _hl7_in_error (hl7_in_error_id int(10) NOT NULL, hl7_source int(10) NOT NULL, hl7_source_key text, hl7_data mediumtext NOT NULL, error varchar(255) NOT NULL, error_details text, date_created datetime NOT NULL, uuid char(38) NOT NULL, PRIMARY KEY (hl7_in_error_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_hl7_in_queue_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _hl7_in_queue"))
        create_query = "CREATE TABLE _hl7_in_queue (hl7_in_queue_id int(10) NOT NULL, hl7_source int(10) NOT NULL, hl7_source_key text, hl7_data text NOT NULL, message_state int(10) NOT NULL, date_processed datetime, error_msg text, date_created datetime, uuid char(38) NOT NULL, PRIMARY KEY (hl7_in_queue_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_hl7_source_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _hl7_source"))
        create_query = "CREATE TABLE _hl7_source (hl7_source_id int(10) NOT NULL, name varchar(255) NOT NULL, description text, creator int(10) NOT NULL, date_created datetime NOT NULL, uuid char(38), PRIMARY KEY (hl7_source_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_htmlformentry_html_form(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _htmlformentry_html_form"))
        create_query = "CREATE TABLE _htmlformentry_html_form (id int(10) NOT NULL, form_id int(10), name varchar(255), xml_data mediumtext NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, retired bit(1) NOT NULL, uuid char(38) NOT NULL, description varchar(1000), retired_by int(10), date_retired datetime, retire_reason varchar(255), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_location_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _location"))
        create_query = "CREATE TABLE _location (location_id int(10) NOT NULL, name varchar(255) NOT NULL, description varchar(255), address1 varchar(255), address2 varchar(255), city_village varchar(255), state_province varchar(255), postal_code varchar(50), country varchar(50), latitude varchar(50), longitude varchar(50), creator int(10) NOT NULL, date_created datetime NOT NULL, county_district varchar(255), address3 varchar(255), address4 varchar(255), address5 varchar(255), address6 varchar(255), retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), parent_location int(10), uuid char(38), PRIMARY KEY (location_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_note_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _note"))
        create_query = "CREATE TABLE _note (note_id int(10) NOT NULL, note_type varchar(50), patient_id int(10), obs_id int(10), encounter_id int(10), text text NOT NULL, priority int(10), parent int(10), creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, uuid char(38), PRIMARY KEY (note_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_notification_alert_recipient_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _notification_alert_recipient"))
        create_query = "CREATE TABLE _notification_alert_recipient (alert_id int(10) NOT NULL, user_id int(10) NOT NULL, alert_read int(10) NOT NULL, date_changed datetime, uuid char(38) NOT NULL, PRIMARY KEY (alert_id,user_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_notification_alert_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _notification_alert"))
        create_query = "CREATE TABLE _notification_alert (alert_id int(10) NOT NULL, text varchar(512) NOT NULL, satisfied_by_any bit(1) NOT NULL, alert_read bit(1) NOT NULL, date_to_expire datetime, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, uuid char(38), PRIMARY KEY (alert_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_obs_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _obs"))
        create_query = "CREATE TABLE _obs (obs_id int(10) NOT NULL, person_id int(10) NOT NULL, concept_id int(10) NOT NULL, encounter_id int(10), order_id int(10), obs_datetime datetime NOT NULL, location_id int(10), obs_group_id int(10), accession_number varchar(255), value_group_id int(10), value_boolean bit(1), value_coded int(10), value_coded_name_id int(10), value_drug int(10), value_datetime datetime, value_numeric double(22,0), value_modifier varchar(2), value_text text, value_complex varchar(255), comments varchar(255), creator int(10) NOT NULL, date_created datetime NOT NULL, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38), previous_version int(10), PRIMARY KEY (obs_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_order_type_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _order_type"))
        create_query = "CREATE TABLE _order_type (order_type_id int(10) NOT NULL, name varchar(255) NOT NULL, description varchar(255) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (order_type_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_orders_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _orders"))
        create_query = "CREATE TABLE _orders (order_id int(10) NOT NULL, order_type_id int(10) NOT NULL, concept_id int(10) NOT NULL, orderer int(10), encounter_id int(10), instructions text, start_date datetime, auto_expire_date datetime, discontinued bit(1) NOT NULL, discontinued_date datetime, discontinued_by int(10), discontinued_reason int(10), discontinued_reason_non_coded varchar(255), creator int(10) NOT NULL, date_created datetime NOT NULL, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), patient_id int(10) NOT NULL, accession_number varchar(255), uuid char(38), PRIMARY KEY (order_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_patient_identifier_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _patient_identifier"))
        create_query = "CREATE TABLE _patient_identifier (patient_identifier_id int(10) NOT NULL, patient_id int(10) NOT NULL, identifier varchar(50) NOT NULL, identifier_type int(10) NOT NULL, preferred tinyint(1) NOT NULL, location_id int(10) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, date_changed datetime, changed_by int(10), voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38) NOT NULL, PRIMARY KEY (patient_identifier_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_patient_identifier_type_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _patient_identifier_type"))
        create_query = "CREATE TABLE _patient_identifier_type (patient_identifier_type_id int(10) NOT NULL, name varchar(50) NOT NULL, description text NOT NULL, format varchar(50), check_digit tinyint(1) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, required tinyint(1) NOT NULL, format_description varchar(255), validator varchar(200), location_behavior varchar(50), retired tinyint(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38) NOT NULL, PRIMARY KEY (patient_identifier_type_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_patient_program_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _patient_program"))
        create_query = "CREATE TABLE _patient_program (patient_program_id int(10) NOT NULL, patient_id int(10) NOT NULL, program_id int(10) NOT NULL, date_enrolled datetime, date_completed datetime, location_id int(10), outcome_concept_id int(10), creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38) NOT NULL, PRIMARY KEY (patient_program_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_patient_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _patient"))
        create_query = "CREATE TABLE _patient (patient_id int(10) NOT NULL, tribe int(10), creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), PRIMARY KEY (patient_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_person_address_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _person_address"))
        create_query = "CREATE TABLE _person_address (person_address_id int(10) NOT NULL, person_id int(10), preferred tinyint(1) NOT NULL, address1 varchar(50), address2 varchar(50), city_village varchar(50), state_province varchar(50), postal_code varchar(50), country varchar(50), latitude varchar(50), longitude varchar(50), start_date datetime, end_date datetime, creator int(10) NOT NULL, date_created datetime NOT NULL, voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), county_district varchar(50), address3 varchar(50), address4 varchar(50), address5 varchar(50), address6 varchar(50), date_changed datetime, changed_by int(10), uuid char(38) NOT NULL, PRIMARY KEY (person_address_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_person_attribute_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _person_attribute"))
        create_query = "CREATE TABLE _person_attribute (person_attribute_id int(10) NOT NULL, person_id int(10) NOT NULL, value varchar(50) NOT NULL, person_attribute_type_id int(10) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38) NOT NULL, PRIMARY KEY (person_attribute_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_person_attribute_type_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _person_attribute_type"))
        create_query = "CREATE TABLE _person_attribute_type (person_attribute_type_id int(10) NOT NULL, name varchar(50) NOT NULL, description text NOT NULL, format varchar(50), foreign_key int(10), searchable tinyint(1) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, retired tinyint(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), edit_privilege varchar(255), sort_weight double(22,0), uuid char(38) NOT NULL, PRIMARY KEY (person_attribute_type_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_person_name_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _person_name"))
        create_query = "CREATE TABLE _person_name (person_name_id int(10) NOT NULL, preferred tinyint(1) NOT NULL, person_id int(10) NOT NULL, prefix varchar(50), given_name varchar(50), middle_name varchar(50), family_name_prefix varchar(50), family_name varchar(50), family_name2 varchar(50), family_name_suffix varchar(50), degree varchar(50), creator int(10) NOT NULL, date_created datetime NOT NULL, voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), changed_by int(10), date_changed datetime, uuid char(38) NOT NULL, PRIMARY KEY (person_name_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_person_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _person"))
        create_query = "CREATE TABLE _person (person_id int(10) NOT NULL, gender varchar(50), birthdate date, birthdate_estimated tinyint(1) NOT NULL, dead tinyint(1) NOT NULL, death_date datetime, cause_of_death int(10), creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, voided tinyint(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38) NOT NULL, PRIMARY KEY (person_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_privilege_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _privilege"))
        create_query = "CREATE TABLE _privilege (privilege varchar(50) NOT NULL, description varchar(250) NOT NULL, uuid char(38), PRIMARY KEY (privilege)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_program_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _program"))
        create_query = "CREATE TABLE _program (program_id int(10) NOT NULL, concept_id int(10) NOT NULL, outcomes_concept_id int(10), creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, retired tinyint(1) NOT NULL, name varchar(50) NOT NULL, description varchar(500), uuid char(38) NOT NULL, PRIMARY KEY (program_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_program_workflow_state_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _program_workflow_state"))
        create_query = "CREATE TABLE _program_workflow_state (program_workflow_state_id int(10) NOT NULL, program_workflow_id int(10) NOT NULL, concept_id int(10) NOT NULL, initial bit(1) NOT NULL, terminal bit(1) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, changed_by int(10), date_changed datetime, uuid char(38), PRIMARY KEY (program_workflow_state_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_program_workflow_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _program_workflow"))
        create_query = "CREATE TABLE _program_workflow (program_workflow_id int(10) NOT NULL, program_id int(10) NOT NULL, concept_id int(10) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, retired bit(1) NOT NULL, changed_by int(10), date_changed datetime, uuid char(38), PRIMARY KEY (program_workflow_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_provider_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _provider"))
        create_query = "CREATE TABLE _provider (provider_id int(11) NOT NULL, person_id int(11), name varchar(255), identifier varchar(255), creator int(11) NOT NULL, date_created datetime NOT NULL, changed_by int(11), date_changed datetime, retired tinyint(1) NOT NULL, retired_by int(11), date_retired datetime, retire_reason varchar(255), uuid char(38), role_id int(11), speciality_id int(11), PRIMARY KEY (`provider_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()

def create_relationship_type_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _relationship_type"))
        create_query = "CREATE TABLE _relationship_type (relationship_type_id int(10) NOT NULL, a_is_to_b varchar(50) NOT NULL, b_is_to_a varchar(50) NOT NULL, preferred int(10) NOT NULL, weight int(10) NOT NULL, description varchar(255) NOT NULL, creator int(10) NOT NULL, date_created datetime NOT NULL, uuid char(38) NOT NULL, retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), PRIMARY KEY (relationship_type_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_report_object_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _report_object"))
        create_query = "CREATE TABLE _report_object (report_object_id int(10) NOT NULL, name varchar(255) NOT NULL, description varchar(1000), report_object_type varchar(255) NOT NULL, report_object_sub_type varchar(255) NOT NULL, xml_data text, creator int(10) NOT NULL, date_created datetime NOT NULL, changed_by int(10), date_changed datetime, voided bit(1) NOT NULL, voided_by int(10), date_voided datetime, void_reason varchar(255), uuid char(38), PRIMARY KEY (report_object_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_report_schema_xml_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _report_schema_xml"))
        create_query = "CREATE TABLE _report_schema_xml (report_schema_id int(10) NOT NULL, name varchar(255) NOT NULL, description text NOT NULL, xml_data text NOT NULL, uuid char(38), PRIMARY KEY (report_schema_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_role_privilege_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _role_privilege"))
        create_query = "CREATE TABLE _role_privilege (role varchar(50) NOT NULL, privilege varchar(50) NOT NULL, PRIMARY KEY (role,privilege)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_role_role_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _role_role"))
        create_query = "CREATE TABLE _role_role (parent_role varchar(50) NOT NULL, child_role varchar(50) NOT NULL, PRIMARY KEY (parent_role,child_role)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_role_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _role"))
        create_query = "CREATE TABLE _role (role varchar(50) NOT NULL, description varchar(255) NOT NULL, uuid char(38), PRIMARY KEY (role)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_serialized_object_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _serialized_object"))
        create_query = "CREATE TABLE _serialized_object (serialized_object_id int(10) NOT NULL, name varchar(255) NOT NULL, description varchar(5000), type varchar(255) NOT NULL, subtype varchar(255) NOT NULL, serialization_class varchar(255) NOT NULL, serialized_data text NOT NULL, date_created datetime NOT NULL, creator int(10) NOT NULL, date_changed datetime, changed_by int(10), retired bit(1) NOT NULL, date_retired datetime, retired_by int(10), retire_reason varchar(1000), uuid char(38) NOT NULL, PRIMARY KEY (serialized_object_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_user_property_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _user_property"))
        create_query = "CREATE TABLE _user_property (user_id int(10) NOT NULL, property varchar(100) NOT NULL, property_value varchar(255) NOT NULL, PRIMARY KEY (user_id,property)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()


def create_users_table(engine, drop_create=False):
    with engine.connect() as conn:
        if drop_create:
            conn.execute(text("DROP TABLE IF EXISTS _users"))
        create_query = "CREATE TABLE _users (user_id int(10) NOT NULL, system_id varchar(50) NOT NULL, username varchar(50), password varchar(128), salt varchar(128), secret_question varchar(255), secret_answer varchar(255), creator int(10), date_created datetime NOT NULL, changed_by int(10), date_changed datetime, person_id int(10), retired bit(1) NOT NULL, retired_by int(10), date_retired datetime, retire_reason varchar(255), uuid char(38), PRIMARY KEY (user_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
        conn.execute(text(create_query))
        conn.commit()
