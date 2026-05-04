import pandas as pd
import os
from utils.logger import info, warning

def read_excel_sheet(filename, sheet_name):
    """
    Reads a specific sheet from an Excel file in the resources directory and returns a pandas DataFrame.
    """
    excel_path = os.path.join('resources', filename)
    info(f"Reading data from {excel_path}, sheet: {sheet_name}...")
    
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        if not df.empty:
            # Convert NaN to None for database compatibility
            df = df.where(pd.notnull(df), None)
            return df
        else:
            warning(f"No data found in {sheet_name} sheet of {filename}.")
            return pd.DataFrame()
    except Exception as e:
        warning(f"Error reading {filename}: {e}")
        return pd.DataFrame()

def get_commonlab_test_type_data():
    # Expected columns: type_id	name	description	test_group	short_name	requires_specimen	reference_concept_id	uuid
    df = read_excel_sheet('commonlab.xlsx', 'test_types')
    return df

def get_commonlab_attribute_type_data():
    # Expected columns: test_attribute_type_id	test_type_id	name	datatype_config	sort_weight	description	preferred_handler	hint	group_name	multiset_name	creator	date_created	retired	uuid
    df = read_excel_sheet('commonlab.xlsx', 'attribute_types')
    return df

def get_concept_data():
    # Expected columns: concept_id	retired	retired_by	datatype_id	class_id	is_set	creator	date_created	changed_by	date_changed	uuid
    df = read_excel_sheet('concept_mapping.xlsx', 'concept')
    return df

def get_concept_map_data():
    # Expected columns: concept_map_id	source_id	source	source_code	comment	creator	date_created	concept_id	uuid
    df = read_excel_sheet('concept_mapping.xlsx', 'concept_map')
    return df

def get_concept_name_data():
    # Expected columns: concept_id	name	locale	type
    df = read_excel_sheet('concept_mapping.xlsx', 'concept_name')
    return df

def get_concept_source_data():
    # Expected columns: concept_source_id	name	description	hl7_code	creator	date_created	retired	retired_by	date_retired	retire_reason	uuid
    df = read_excel_sheet('concept_mapping.xlsx', 'concept_source')
    return df

def get_global_property_data():
    # Expected columns: source	property	property_value	description	uuid
    df = read_excel_sheet('global_properties.xlsx', 'live')
    return df

def get_location_data():
    # Expected columns: location_id	name	level	parent_location	description	state_province	county_district	date_created	retired	retired_by	date_retired	retire_reason	uuid
    df = read_excel_sheet('locations.xlsx', 'Locations')
    return df

def get_message_properties(language):
    # Expected columns: Key	Value
    assert language in ['en', 'ru', 'tj']
    # 'ru' and 'tj' sheets in message.properties.xlsx do not have headers, but 'en' does.
    if language in ['ru', 'tj']:
        df = read_excel_sheet('message.properties.xlsx', language)
        if not df.empty:
            # Re-read with header=None if the first row contains data (which is the case for ru and tj)
            excel_path = os.path.join('resources', 'message.properties.xlsx')
            df = pd.read_excel(excel_path, sheet_name=language, header=None)
            df.columns = ['Key', 'Value']
            # Convert NaN to None for database compatibility as in read_excel_sheet
            df = df.where(pd.notnull(df), None)
            return df
    df = read_excel_sheet('message.properties.xlsx', language)
    return df
