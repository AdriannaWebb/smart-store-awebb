"""
tests/test_data_scrubber.py

To run, open a terminal in the root project folder. 
Activate your virtual environment if needed, and run one of the following commands:

    py tests\test_data_scrubber.py
    python3 tests\test_data_scrubber.py

This test suite verifies that each function in the DataScrubber class works as expected.
"""

import unittest
import pathlib
import sys
from io import StringIO
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import DataScrubber from the scripts module
from scripts.data_scrubber import DataScrubber  # noqa: E402

# Create a fake CSV file using StringIO
csv_data = StringIO("""
ID,Name,Score,Date
1,Alice,10,2023-01-01
2,Bob,15,2023-01-02
3,Charlie,20,2023-01-03
4,Alice,,2023-01-04
5,Eve,25,2023-01-05
5,Eve,30,2023-01-05
""")

# Load the fake CSV data into a DataFrame
df = pd.read_csv(csv_data)


class TestDataScrubber(unittest.TestCase):

    def setUp(self):
        """Set up a fresh instance of DataScrubber before each test."""
        self.scrubber = DataScrubber(df.copy())

    def test_check_data_consistency_before_cleaning(self):
        """Test data consistency check before cleaning."""
        consistency = self.scrubber.check_data_consistency_before_cleaning()
        self.assertGreaterEqual(consistency['null_counts']['Score'], 0, "Null count for column Score should be 0 or more")
        self.assertGreaterEqual(consistency['duplicate_count'], 0, "Duplicate count should be 0 or more")


    def test_check_data_consistency_after_cleaning(self):
        """Test data consistency check after cleaning."""
        # Perform cleaning steps
        self.scrubber.handle_missing_data(fill_value=0)
        self.scrubber.remove_duplicate_records()
        
        # Verify post-cleaning consistency
        consistency = self.scrubber.check_data_consistency_after_cleaning()
        self.assertEqual(consistency['null_counts'].sum(), 0, "Null values not cleared in CLEAN stage")
        self.assertEqual(consistency['duplicate_count'], 0, "Duplicates not removed in CLEAN stage")

    def test_convert_column_to_new_data_type(self):
        df_converted = self.scrubber.convert_column_to_new_data_type('Score', 'float')
        self.assertEqual(df_converted['Score'].dtype, 'float64', "Data type not converted correctly")

    def test_drop_columns(self):
        df_dropped = self.scrubber.drop_columns(['Date'])
        self.assertNotIn('Date', df_dropped.columns, "Column Date not dropped correctly")

    def test_filter_column_outliers(self):
        df_filtered = self.scrubber.filter_column_outliers('Score', 10, 25)
        self.assertLessEqual(df_filtered['Score'].max(), 25, "Outliers not filtered correctly")

    def test_format_column_strings_to_lower_and_trim(self):
        df_formatted = self.scrubber.format_column_strings_to_lower_and_trim('Name')
        self.assertEqual(df_formatted['Name'].str.contains(' ').sum(), 0, "Strings not formatted to lowercase correctly")
        self.assertTrue(df_formatted['Name'].str.islower().all(), "Strings not formatted to lowercase correctly")

    def test_format_column_strings_to_upper_and_trim(self):
        df_formatted = self.scrubber.format_column_strings_to_upper_and_trim('Name')
        self.assertEqual(df_formatted['Name'].str.contains(' ').sum(), 0, "Strings not formatted to uppercase correctly")
        self.assertTrue(df_formatted['Name'].str.isupper().all(), "Strings not formatted to uppercase correctly")
    
    def test_handle_missing_data(self):
        df_filled = self.scrubber.handle_missing_data(fill_value=0)
        self.assertEqual(df_filled.isnull().sum().sum(), 0, "Missing values not handled correctly")

    def test_inspect_data(self):
        info, describe = self.scrubber.inspect_data()
        self.assertIsNotNone(info, "DataFrame info should not be None")
        self.assertIsNotNone(describe, "DataFrame description should not be None")

    def test_parse_dates_to_add_standard_datetime(self):
        df_parsed = self.scrubber.parse_dates_to_add_standard_datetime('Date')
        self.assertIn('StandardDateTime', df_parsed.columns, "StandardDateTime column not added correctly")
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df_parsed['StandardDateTime']), "StandardDateTime column not parsed correctly")

    def test_remove_duplicate_records(self):
        df_no_duplicates = self.scrubber.remove_duplicate_records()
        self.assertEqual(df_no_duplicates.duplicated().sum(), 0, "Duplicates not removed correctly")

    def test_rename_columns(self):
        df_renamed = self.scrubber.rename_columns({'ID': 'Identifier', 'Name': 'FullName'})
        self.assertIn('Identifier', df_renamed.columns, "Column ID not renamed correctly")
        self.assertIn('FullName', df_renamed.columns, "Column Name not renamed correctly")

    def test_reorder_columns(self):
        df_reordered = self.scrubber.reorder_columns(['Name', 'ID', 'Date'])
        self.assertEqual(df_reordered.columns.tolist(), ['Name', 'ID', 'Date'], "Columns not reordered correctly")


# Run the tests with verbosity=2 for detailed output
if __name__ == "__main__":
    unittest.main(verbosity=2)