"""
Module 3: Data Preparation Script
File: scripts/data_prep.py

This script is just one example of a possible data preparation process.
It loads raw CSV files from the 'data/raw/' directory, cleans and prepares each file, 
and saves the prepared data to 'data/prepared/'.
The data preparation steps include removing duplicates, handling missing values, 
trimming whitespace, and more.

This script uses the general DataScrubber class and its methods to perform common, reusable tasks.

To run it, open a terminal in the root project folder.
Activate the local project virtual environment.
Choose the correct command for your OS to run this script.

py scripts\data_prep.py
python3 scripts\data_prep.py

NOTE: I use the ruff linter. 
It warns if all import statements are not at the top of the file.  
I was having trouble with the relative paths, so I  
temporarily add the project root before I can import. 
By adding this comment at the end of an import line noqa: E402
ruff will ignore the warning on just that line. 
"""

import pathlib
import sys
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger  # noqa: E402
from scripts.data_scrubber import DataScrubber  # noqa: E402

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    return pd.read_csv(file_path)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV."""
    file_path: pathlib.Path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def main() -> None:
    """Main function for pre-processing customer, product, and sales data."""
    logger.info("======================")
    logger.info("STARTING data_prep.py")
    logger.info("======================")

    logger.info("========================")
    logger.info("Starting CUSTOMERS prep")
    logger.info("========================")

    df_customers = read_raw_data("customers_data.csv")

    df_customers.columns = df_customers.columns.str.strip()  # Clean column names
    df_customers = df_customers.drop_duplicates()            # Remove duplicates

    df_customers['Name'] = df_customers['Name'].str.strip()  # Trim whitespace from column values
    df_customers = df_customers.dropna(subset=['CustomerID', 'Name'])  # Drop rows missing critical info
    
    scrubber_customers = DataScrubber(df_customers)
    scrubber_customers.check_data_consistency_before_cleaning()
    scrubber_customers.inspect_data()
    
    df_customers = scrubber_customers.handle_missing_data(fill_value="N/A")
    df_customers = scrubber_customers.parse_dates_to_add_standard_datetime('JoinDate')
    scrubber_customers.check_data_consistency_after_cleaning()

    save_prepared_data(df_customers, "customers_data_prepared.csv")

    logger.info("========================")
    logger.info("Starting PRODUCTS prep")
    logger.info("========================")

    df_products = read_raw_data("products_data.csv")

    df_products.columns = df_products.columns.str.strip()  # Clean column names
    df_products = df_products.drop_duplicates()            # Remove duplicates

    df_products['ProductName'] = df_products['ProductName'].str.strip()  # Trim whitespace from column values
    
    scrubber_products = DataScrubber(df_products)
    scrubber_products.check_data_consistency_before_cleaning()
    scrubber_products.inspect_data()

    scrubber_products.check_data_consistency_after_cleaning()
    save_prepared_data(df_products, "products_data_prepared.csv")

    logger.info("========================")
    logger.info("Starting SALES prep")
    logger.info("========================")

    df_sales = read_raw_data("sales_data.csv")

    df_sales.columns = df_sales.columns.str.strip()  # Clean column names
    df_sales = df_sales.drop_duplicates()            # Remove duplicates

    df_sales['SaleDate'] = pd.to_datetime(df_sales['SaleDate'], errors='coerce')  # Ensure sale_date is datetime
    df_sales = df_sales.dropna(subset=['TransactionID', 'SaleDate'])  # Drop rows missing key information
    
    scrubber_sales = DataScrubber(df_sales)
    scrubber_sales.check_data_consistency_before_cleaning()
    scrubber_sales.inspect_data()
    
    df_sales = scrubber_sales.handle_missing_data(fill_value="Unknown")
    scrubber_sales.check_data_consistency_after_cleaning()

    save_prepared_data(df_sales, "sales_data_prepared.csv")

    logger.info("======================")
    logger.info("FINISHED data_prep.py")
    logger.info("======================")

if __name__ == "__main__":
    main()