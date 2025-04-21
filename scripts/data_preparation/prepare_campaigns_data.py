"""
scripts/data_preparation/prepare_campaigns_data.py

This script reads campaign data from the data/raw folder, cleans the data, 
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Standardize date formats
- Ensure consistent formatting

-----------------------------------
How to Run:
1. Open a terminal in the main root project folder.
2. Activate the local project virtual environment.
3. Choose the correct commands for your OS to run this script
"""

import pathlib
import sys
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger  # noqa: E402

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

# -------------------
# Reusable Functions
# -------------------

def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Read raw data from CSV.

    Args:
        file_name (str): Name of the CSV file to read.
    
    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    logger.info(f"FUNCTION START: read_raw_data with file_name={file_name}")
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")
    return df

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save cleaned data to CSV.

    Args:
        df (pd.DataFrame): Cleaned DataFrame.
        file_name (str): Name of the output file.
    """
    logger.info(f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}")
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial_count = len(df)
    
    # For campaigns data, we want to ensure CampaignID is unique
    df = df.drop_duplicates(subset=['campaignid'])
    
    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} duplicate rows")
    logger.info(f"{len(df)} records remaining after removing duplicates.")
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values by filling or dropping.
    This logic is specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")
    
    # Log missing values by column before handling
    missing_by_col = df.isna().sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")
    
    # For campaign data, we don't expect any missing values in our key fields
    if 'description' in df.columns:
        df['description'].fillna('', inplace=True)
    
    # Log missing values by column after handling
    missing_after = df.isna().sum()
    logger.info(f"Missing values by column after handling:\n{missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df

def parse_dates_to_add_standard_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse dates and add a standard datetime column.

    Args:
        df (pd.DataFrame): Input DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with standardized date formats.
    """
    logger.info(f"FUNCTION START: parse_dates_to_add_standard_datetime with dataframe shape={df.shape}")
    
    # Convert both date columns to datetime format
    df['startdate'] = pd.to_datetime(df['startdate'])
    df['enddate'] = pd.to_datetime(df['enddate'])
    
    # Convert to string with consistent format (like we see in the customers_data_prepared.csv)
    df['startdate'] = df['startdate'].dt.strftime('%Y-%m-%d')
    df['enddate'] = df['enddate'].dt.strftime('%Y-%m-%d')
    
    logger.info("Completed standardizing date formats")
    return df

def main() -> None:
    """
    Main function for processing campaign data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_campaigns_data.py")
    logger.info("==================================")

    logger.info(f"Root project folder: {PROJECT_ROOT}")
    logger.info(f"data / raw folder: {RAW_DATA_DIR}")
    logger.info(f"data / prepared folder: {PREPARED_DATA_DIR}")

    input_file = "campaigns_data.csv"
    output_file = "campaigns_data_prepared.csv"
    
    # Read raw data
    df = read_raw_data(input_file)

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")
    
    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()
    
    # Log if any column names changed
    changed_columns = [f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Process data
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = parse_dates_to_add_standard_datetime(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info("FINISHED prepare_campaigns_data.py")
    logger.info("==================================")

# -------------------
# Conditional Execution Block
# -------------------

if __name__ == "__main__":
    main()