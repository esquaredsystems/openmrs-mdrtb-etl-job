# tests/test_helpers.py

import os
import pandas as pd
import pytest
from utils.helpers import (
    read_excel_sheet,
    get_commonlab_test_type_data,
    get_commonlab_attribute_type_data,
    get_concept_data,
    get_concept_map_data,
    get_concept_name_data,
    get_concept_source_data,
    get_global_property_data,
    get_location_data,
    get_message_properties
)


@pytest.fixture
def setup_excel_file(tmp_path, monkeypatch):
    """Create a temporary Excel file for testing utility function."""
    resources_dir = tmp_path / "resources"
    resources_dir.mkdir()
    file_path = resources_dir / "test.xlsx"
    data = {
        "Sheet1": pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}),
        "EmptySheet": pd.DataFrame(),
    }
    with pd.ExcelWriter(file_path) as writer:
        for sheet_name, df in data.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    
    # Mock os.path.join to use the tmp_path/resources instead of the actual resources dir
    original_join = os.path.join
    def mocked_join(*args):
        if args[0] == 'resources':
            return original_join(str(resources_dir), *args[1:])
        return original_join(*args)
    
    monkeypatch.setattr(os.path, "join", mocked_join)
    
    yield file_path
    if file_path.exists():
        os.remove(file_path)

def test_read_excel_sheet_valid_sheet(setup_excel_file):
    """Test reading a valid sheet from an Excel file."""
    file_path = setup_excel_file
    df = read_excel_sheet(file_path.name, "Sheet1")
    expected_df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    pd.testing.assert_frame_equal(expected_df, df)

def test_read_excel_sheet_empty_sheet(setup_excel_file):
    """Test reading an empty sheet from an Excel file."""
    file_path = setup_excel_file
    df = read_excel_sheet(file_path.name, "EmptySheet")
    expected_df = pd.DataFrame()
    pd.testing.assert_frame_equal(expected_df, df)

def test_read_excel_sheet_invalid_sheet(setup_excel_file):
    """Test reading a non-existent sheet from an Excel file."""
    file_path = setup_excel_file
    df = read_excel_sheet(file_path.name, "NonExistentSheet")
    expected_df = pd.DataFrame()
    pd.testing.assert_frame_equal(expected_df, df)

def test_read_excel_sheet_nonexistent_file():
    """Test reading from a non-existent file."""
    df = read_excel_sheet("nonexistent.xlsx", "Sheet1")
    expected_df = pd.DataFrame()
    pd.testing.assert_frame_equal(expected_df, df)

def test_get_commonlab_test_type_data():
    """Test get_commonlab_test_type_data returns expected columns."""
    df = get_commonlab_test_type_data()
    expected_columns = ["type_id", "name", "description", "test_group", "short_name", "requires_specimen", "reference_concept_id", "uuid"]
    for col in expected_columns:
        assert col in df.columns

def test_get_commonlab_attribute_type_data():
    """Test get_commonlab_attribute_type_data returns expected columns."""
    df = get_commonlab_attribute_type_data()
    expected_columns = ["test_attribute_type_id", "test_type_id", "name", "datatype_config", "sort_weight", "description", "preferred_handler", "hint", "group_name", "multiset_name", "creator", "date_created", "retired", "uuid"]
    for col in expected_columns:
        assert col in df.columns

def test_get_concept_data():
    """Test get_concept_data returns expected columns."""
    df = get_concept_data()
    expected_columns = ["concept_id", "retired", "retired_by", "datatype_id", "class_id", "is_set", "creator", "date_created", "changed_by", "date_changed", "uuid"]
    for col in expected_columns:
        assert col in df.columns

def test_get_concept_map_data():
    """Test get_concept_map_data returns expected columns."""
    df = get_concept_map_data()
    expected_columns = ["concept_map_id", "source_id", "source", "source_code", "comment", "creator", "date_created", "concept_id", "uuid"]
    for col in expected_columns:
        assert col in df.columns

def test_get_concept_name_data():
    """Test get_concept_name_data returns expected columns."""
    df = get_concept_name_data()
    expected_columns = ["concept_id", "name", "locale", "type"]
    for col in expected_columns:
        assert col in df.columns

def test_get_concept_source_data():
    """Test get_concept_source_data returns expected columns."""
    df = get_concept_source_data()
    expected_columns = ["concept_source_id", "name", "description", "hl7_code", "creator", "date_created", "retired", "retired_by", "date_retired", "retire_reason", "uuid"]
    for col in expected_columns:
        assert col in df.columns

def test_get_global_property_data():
    """Test get_global_property_data returns expected columns."""
    df = get_global_property_data()
    expected_columns = ["source", "property", "property_value", "description", "uuid"]
    for col in expected_columns:
        assert col in df.columns

def test_get_location_data():
    """Test get_location_data returns expected columns."""
    df = get_location_data()
    expected_columns = ["location_id", "name", "level", "parent_location", "description", "state_province", "county_district", "date_created", "retired", "retired_by", "date_retired", "retire_reason", "uuid"]
    for col in expected_columns:
        assert col in df.columns

def test_get_message_properties_en():
    """Test get_message_properties for 'en' returns expected columns."""
    df = get_message_properties("en")
    expected_columns = ["Key", "Value"]
    for col in expected_columns:
        assert col in df.columns

def test_get_message_properties_ru():
    """Test get_message_properties for 'ru' returns expected columns."""
    df = get_message_properties("ru")
    expected_columns = ["Key", "Value"]
    for col in expected_columns:
        assert col in df.columns

def test_get_message_properties_tj():
    """Test get_message_properties for 'tj' returns expected columns."""
    df = get_message_properties("tj")
    expected_columns = ["Key", "Value"]
    for col in expected_columns:
        assert col in df.columns
