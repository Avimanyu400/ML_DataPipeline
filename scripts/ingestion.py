# ***************************************************************************************
# Script Name: Recomart data loader
# ***************************************************************************************
# Objective: Read json data from data lake and write to Datawarehouse (postgres database).
#            Move files from raw_data folder to processed_data folder after loaded to postgres. 
# ****************************************************************************************
# Import dependency library
 
import pandas as pd
from sqlalchemy import create_engine
import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy.exc import OperationalError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psycopg2
import shutil

# Database Connection
engine = create_engine('postgresql://postgres:postgres@localhost/dmml')
    
# Create schema if not exist
conn = psycopg2.connect(
       host="localhost",
       database="dmml",
       user="postgres",
       password="postgres"
       )

cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS dmmlschema")
cur.execute("CREATE SCHEMA IF NOT EXISTS dmmlapischema")
conn.commit()
cur.close()
conn.close()
# Database Connection
#engine = create_engine('postgresql://postgres:postgres@localhost/dmml')

#Get current directory
script_dir = os.getcwd() 
base_path = os.path.join(script_dir, "raw_data")

source1="batch"
source2="batch"
source3="API"
source4 = "streaming"

data_type1="product_catalog"
data_type2="purchase_history"
data_type3="external_sentiments"
data_type4="clickstream_logs"


# 1. Get current time components
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
timestamp = now.strftime("%H%M%S")


# 3. Construct the full directory path
# Using os.path.join handles Windows backslashes (\) automatically
source_dir1 = os.path.join(base_path, source1, data_type1, year, month, day)
source_dir2 = os.path.join(base_path, source2, data_type2, year, month, day)
source_dir3 = os.path.join(base_path, source3, data_type3, year, month, day)
source_dir4 = os.path.join(base_path, source4, data_type4, year, month, day)

# --- A. LOGGING & AUDIT TRAIL SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("recomart_ingestion.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("Ingestor")
# ***************************************************************************
def fetch_json_data(filepath,filename):
    base_dir = Path(filepath)
    # Use the / operator to join them safely
    full_path = base_dir / filename
 
    # Use lines=True if your file is "JSON Lines" (one JSON object per line)
    logger.info(f"Reading clickstream data from {filepath}")
    df = pd.read_json(full_path, lines=True)
    #print(df.head())
    return df

# ***********************************************************************
def load_batch_data():
    
    try:

        # Task 1: product_catalog (CSV)
        product_catalog_df = fetch_product_catalog_csv(source_dir1,"product_catalog.csv")
        load_to_postgres(product_catalog_df,"product_catalog","dmmlschema", engine)
       
        # Task 2: purchase_history (CSV)
        purchase_history_df = fetch_purchase_history_csv(source_dir2,"purchase_history.csv")
        load_to_postgres(purchase_history_df, "purchase_history","dmmlschema", engine)
        
        # Task 3: external_sentiment 
        external_sentiment_df = fetch_external_sentiment_csv(source_dir3,"external_sentiments.csv")
        load_to_postgres(external_sentiment_df, "external_sentiments", "dmmlapischema", engine)

        logger.info("Pipeline completed successfully.")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)  

# --- DATA FETCHING FUNCTIONS ---

def fetch_product_catalog_csv(filepath,filename):
    """Source : Local CSV File"""
    base_dir = Path(filepath)
    # Use the / operator to join them safely
    full_path = base_dir / filename
    
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Missing product_catalog file: {filepath}")
    logger.info(f"Reading product_catalog from {filepath}")
    return pd.read_csv(full_path)

def fetch_purchase_history_csv(filepath,filename):
    """Source : Local CSV File"""
    base_dir = Path(filepath)
    # Use the / operator to join them safely
    full_path = base_dir / filename
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Missing purchase_history file: {filepath}")
    logger.info(f"Reading purchase_history from {filepath}")
    return pd.read_csv(full_path)


def fetch_external_sentiment_csv(filepath,filename):
    """Source : Local CSV File"""
    base_dir = Path(filepath)
    # Use the / operator to join them safely
    full_path = base_dir / filename

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Missing sentiment API file: {filepath}")
    logger.info(f"Reading sentiment API from {filepath}")
    return pd.read_csv(full_path)

    
# --- DATABASE HANDLER ---

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=4, max=15),
    retry=retry_if_exception_type(OperationalError),
    before_sleep=lambda rs: logger.warning(f"DB Busy. Retry {rs.attempt_number}/4...")
)

def load_to_postgres(df, table_name, rmschema, engine):
    """Appends data to Postgres. Creates table if it doesn't exist."""
    df.to_sql(table_name, engine, schema=rmschema, if_exists='append', index=False)
    logger.info(f"Audit: Successfully loaded {len(df)} rows to postgres table '{table_name}'")


def load_streaming_data():
    df_json = fetch_json_data(source_dir4,"clickstreamdata_1.json")
    load_to_postgres(df_json,"clickstream_logs","dmmlschema", engine)

def move_raw_data():
    try:

        base_path2 = os.path.join(script_dir, "processed_data")
 
        target_dir1 = os.path.join(base_path2, source1, data_type1)
        target_dir2 = os.path.join(base_path2, source2, data_type2)
        target_dir3 = os.path.join(base_path2, source3, data_type3)
        target_dir4 = os.path.join(base_path2, source4, data_type4)  

        move_raw_to_processed(source_dir1,target_dir1,"product_catalog.csv")
        move_raw_to_processed(source_dir2,target_dir2,"purchase_history.csv")
        move_raw_to_processed(source_dir3,target_dir3,"external_sentiments.csv")
        move_raw_to_processed(source_dir4,target_dir4,"clickstreamdata_1.json")
    
        logger.info("Raw data movement completed successfully.")
        
    except Exception as e:
        logger.error(f"Raw data movement failed: {str(e)}")
        sys.exit(1)  


def move_raw_to_processed(source,destination,file_name):
        
    # 2. Get current date in YYYYMMDD format
    date_suffix = datetime.now().strftime("%Y%m%d")

    if not os.path.exists(destination):
        os.makedirs(destination)

    # 3. Process and Move
    for file_name in os.listdir(source):
        source_path = os.path.join(source, file_name)
    
        if os.path.isfile(source_path):
            # Split filename into name and extension (e.g., 'clickstreamdata_1' and '.json')
            base_name, extension = os.path.splitext(file_name)
        
            # Create new filename with suffix
            new_file_name = f"{base_name}_{date_suffix}{extension}"
            dest_path = os.path.join(destination, new_file_name)
        
            # Move and rename simultaneously
            shutil.move(source_path, dest_path)
            logger.info(f"Archived: {file_name} -> {new_file_name}")
    

# ***************************************
# Main Function
# ***************************************
    
if __name__ == "__main__":
    # write streaming data to postgres
    load_streaming_data();
    # write batch data to postgres
    load_batch_data()
    # Move raw data to processed folder
    move_raw_data()
