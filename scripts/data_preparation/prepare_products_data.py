"""
Smart Store Data Preparation
File: scripts/data_preparation/prepare_products_data.py
Purpose: Process product data from raw data files
"""

import pathlib
import sys
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"Reading raw data from {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if any other error occurs

def process_data(file_name: str) -> None:
    """Process raw data by cleaning and saving to prepared directory."""
    # Make sure the prepared directory exists
    PREPARED_DATA_DIR.mkdir(exist_ok=True, parents=True)
    
    # Read the raw data
    df = read_raw_data(file_name)
    
    if not df.empty:
        # Log the number of raw records
        raw_count = len(df)
        logger.info(f"Raw data contains {raw_count} records.")
        
        # Basic cleaning: Remove duplicates
        df_prepared = df.drop_duplicates()
        prepared_count = len(df_prepared)
        
        if prepared_count < raw_count:
            logger.info(f"Removed {raw_count - prepared_count} duplicate records.")
        
        # Save the prepared data
        output_file_name = file_name.replace(".csv", "_prepared.csv")
        output_path = PREPARED_DATA_DIR.joinpath(output_file_name)
        df_prepared.to_csv(output_path, index=False)
        logger.info(f"Saved prepared data to {output_path}. Total records: {prepared_count}")
    else:
        logger.warning(f"No data processed for {file_name}")

def main() -> None:
    """Main function for processing product data."""
    logger.info("Starting product data preparation...")
    process_data("products_data.csv")
    logger.info("Product data preparation complete.")

if __name__ == "__main__":
    main()