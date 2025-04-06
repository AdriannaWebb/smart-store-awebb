import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import logger
from utils.logger import logger  # noqa: E402

# Constants
DW_DIR = PROJECT_ROOT.joinpath("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_store.db")
PREPARED_DATA_DIR = PROJECT_ROOT.joinpath("data").joinpath("prepared")

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    logger.info("Creating schema tables...")
    
    # Create the dimension tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            region TEXT,
            join_date TEXT,
            loyalty_points INTEGER,
            customer_segment TEXT,
            standard_date_time TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            unit_price REAL,
            stock_quantity INTEGER,
            subcategory TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_store (
            store_id INTEGER PRIMARY KEY,
            store_name TEXT DEFAULT 'Unknown'
        )
    """)
    
    # Create the fact table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            transaction_id INTEGER PRIMARY KEY,
            sale_date TEXT,
            customer_id INTEGER,
            product_id INTEGER,
            store_id INTEGER,
            campaign_id INTEGER,
            sale_amount REAL,
            discount_percent REAL,
            payment_type TEXT,
            FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
            FOREIGN KEY (product_id) REFERENCES dim_product (product_id),
            FOREIGN KEY (store_id) REFERENCES dim_store (store_id)
        )
    """)
    
    logger.info("Schema created successfully.")

def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the tables."""
    logger.info("Deleting existing records from tables...")
    
    cursor.execute("DELETE FROM fact_sales")
    cursor.execute("DELETE FROM dim_customer")
    cursor.execute("DELETE FROM dim_product")
    cursor.execute("DELETE FROM dim_store")
    
    logger.info("Existing records deleted.")

def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the dim_customer table."""
    logger.info("Inserting customer data...")
    
    # Prepare customer data
    customer_data = customers_df[['CustomerID', 'Name', 'Region', 'JoinDate', 'LoyaltyPoints', 'CustomerSegment', 'StandardDateTime']]
    customer_data.columns = ['customer_id', 'name', 'region', 'join_date', 'loyalty_points', 'customer_segment', 'standard_date_time']
    
    # Insert into dim_customer
    customer_data.to_sql("dim_customer", cursor.connection, if_exists="append", index=False)
    
    logger.info(f"Inserted {len(customer_data)} customers.")

def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the dim_product table."""
    logger.info("Inserting product data...")
    
    # Prepare product data
    product_data = products_df[['ProductID', 'ProductName', 'Category', 'UnitPrice', 'StockQuantity', 'Subcategory']]
    product_data.columns = ['product_id', 'product_name', 'category', 'unit_price', 'stock_quantity', 'subcategory']
    
    # Insert into dim_product
    product_data.to_sql("dim_product", cursor.connection, if_exists="append", index=False)
    
    logger.info(f"Inserted {len(product_data)} products.")

def insert_stores(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert store data into the dim_store table."""
    logger.info("Inserting store data...")
    
    # Extract unique store IDs from sales data
    store_ids = sales_df['StoreID'].unique()
    
    # Create a DataFrame for stores
    store_data = pd.DataFrame({
        'store_id': store_ids,
        'store_name': [f"Store {store_id}" for store_id in store_ids]
    })
    
    # Insert into dim_store
    store_data.to_sql("dim_store", cursor.connection, if_exists="append", index=False)
    
    logger.info(f"Inserted {len(store_data)} stores.")

def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the fact_sales table."""
    logger.info("Inserting sales data...")
    
    # Prepare sales data
    sales_data = sales_df[['TransactionID', 'SaleDate', 'CustomerID', 'ProductID', 'StoreID', 
                          'CampaignID', 'SaleAmount', 'DiscountPercent', 'PaymentType']]
    
    sales_data.columns = ['transaction_id', 'sale_date', 'customer_id', 'product_id', 'store_id',
                         'campaign_id', 'sale_amount', 'discount_percent', 'payment_type']
    
    # Insert into fact_sales
    sales_data.to_sql("fact_sales", cursor.connection, if_exists="append", index=False)
    
    logger.info(f"Inserted {len(sales_data)} sales transactions.")

def load_data_to_db() -> None:
    """Load data into the data warehouse."""
    
    logger.info("Starting ETL process to load data into data warehouse...")
    
    # Create DW directory if it doesn't exist
    DW_DIR.mkdir(exist_ok=True, parents=True)
    
    try:
        # Connect to SQLite – will create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)
        
        logger.info("Loading prepared data...")
        
        # Load prepared data using pandas
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_data_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"))
        
        logger.info(f"Loaded {len(customers_df)} customer records, {len(products_df)} product records, and {len(sales_df)} sales records.")
        
        # Insert data into the database
        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_stores(sales_df, cursor)
        insert_sales(sales_df, cursor)
        
        # Commit changes
        conn.commit()
        logger.info("Data warehouse populated successfully.")
        
    except Exception as e:
        logger.error(f"Error during ETL process: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    logger.info("ETL to Data Warehouse process starting...")
    load_data_to_db()
    logger.info("ETL to Data Warehouse process completed.")