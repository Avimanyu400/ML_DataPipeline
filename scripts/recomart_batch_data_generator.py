# ***************************************************************************************
# Script Name: Recomart batch data generator
# ***************************************************************************************
# Objective: To generates synthetic data for product_catalog, Purchase_history and external sentiment.
# ****************************************************************************************
#  Import dependency libraries
import pandas as pd
import numpy as np
import requests
import logging
import os
import sys
import uuid
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import psycopg2
from pathlib import Path
from sqlalchemy.exc import OperationalError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- LOGGING & AUDIT TRAIL SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("recomart_ingestion.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("Ingestor")
#=========================================================

# 1. Get current time components
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
timestamp = now.strftime("%H%M%S")

# 2. Get current directory 
script_dir = os.getcwd() 
base_path = os.path.join(script_dir, "raw_data")
source1="batch"
source2="API"
data_type1="product_catalog"
data_type2="purchase_history"
data_type3="external_sentiments"

# 3. Construct the full directory path
# Using os.path.join handles Windows backslashes (\) automatically
target_dir1 = os.path.join(base_path, source1, data_type1, year, month, day)
target_dir2 = os.path.join(base_path, source1, data_type2, year, month, day)
target_dir3 = os.path.join(base_path, source2, data_type3, year, month, day)


 # *******************************************************************************************
# --- DATA GENERATION FUNCTIONS ---
def generate_periodic_data():

    # Configuration
    NUM_USERS = 5000
    NUM_PRODUCTS = 1000
    NUM_TRANSACTIONS = 20000
    NUM_LOGS = 20000

    # 1. Product Metadata (Catalog)
    products = {
        'product_id': [f'rm_{i:03d}' for i in range(NUM_PRODUCTS)],
        'product_name': [f"Product_{i}" for i in range(NUM_PRODUCTS)],
        'category': [random.choice(['Electronics', 'Apparel', 'Home&Kitchen', 'Books', 'Food','Sports']) for _ in range(NUM_PRODUCTS)],
        'price': [round(random.uniform(10.0, 1500.0), 2) for _ in range(NUM_PRODUCTS)],
        'rating': np.random.uniform(3.0, 5.0, NUM_PRODUCTS).round(1),
        'in_stock': np.random.choice([True, False], NUM_PRODUCTS, p=[0.9, 0.1]),
        'brand': [random.choice(['Samsung', 'Reliance', 'Dmart','Brand X', 'Unknown','Brand Y','Brand ABC']) for _ in range(NUM_PRODUCTS)]
        }
    df_products = pd.DataFrame(products)

    # 2. Transactional Purchase History
    transactions = []
    for _ in range(NUM_TRANSACTIONS):
        transactions.append({
            'transaction_id': str(uuid.uuid4()),
            'user_id': f'USER_{random.randint(1, NUM_USERS):03d}',
            'product_id': random.choice(df_products['product_id']),
            'price': [round(random.uniform(10.0, 1500.0), 2) for _ in range(NUM_PRODUCTS)],
            'timestamp': (datetime.now() - timedelta(minutes=random.randint(0, 43200))).isoformat(),
            'quantity': random.randint(1, 3)
            })
    df_transactions = pd.DataFrame(transactions)
        
    # 3. External API Data (Sentiment/Popularity)
    external_data = {
        'product_id': df_products['product_id'],
        'sentiment_score': [round(random.uniform(0.1, 1.0), 2) for _ in range(NUM_PRODUCTS)],
        'popularity_index': [random.randint(1, 100) for _ in range(NUM_PRODUCTS)]
        }
    df_external = pd.DataFrame(external_data)
    
    # -----------------------------------

    # 3. Create the folder if it doesn't exist
    if not os.path.exists(target_dir1):
        os.makedirs(target_dir1)
        
    if not os.path.exists(target_dir2):
        os.makedirs(target_dir2)    
        
    file_path1 = os.path.join(target_dir1, "product_catalog.csv")
    file_path2 = os.path.join(target_dir2, "purchase_history.csv")

    # 4. save product_catalog data
    df_products.to_csv(file_path1, index=False)
    print(f"✓ Generated {NUM_PRODUCTS} product_catalog records")
    print(f"  Saved to: {target_dir1},'product_catalog.csv' ")

    # 5. save purchase_history data
    df_transactions.to_csv(file_path2, index=False)
    print(f"✓ Generated {NUM_TRANSACTIONS} purchase history records")
    print(f"  Saved to: {target_dir2},'purchase_history.csv' ") 
    # ---------------------------------------------------------------------------------


    # 6. Create the folder if it doesn't exist
    if not os.path.exists(target_dir3):
        os.makedirs(target_dir3)
        
    file_path3 = os.path.join(target_dir3, "external_sentiments.csv")

    # 7. save external_sentiments data
    df_external.to_csv(file_path3, index=False)
    print(f"✓ Generated {NUM_PRODUCTS} external_sentiments records")
    print(f"  Saved to: {target_dir3},'external_sentiments.csv' ")   

    print("=" * 90 + "\n")

# *************************************************************************************
# --- Main Function
# *************************************************************************************

if __name__ == "__main__":
    # Generate synthetic data and save as csv files
    print("Generating synthetic RecoMart data …")
    generate_periodic_data()
