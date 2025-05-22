
import pandas as pd
import logging
import sys
from pymongo import MongoClient, errors
from datetime import datetime
import os

# === CONFIG ===
CSV_FILE = 'Application_Data.csv'  # Replace with your actual CSV file path
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'etl_db'
COLLECTION_NAME = 'etl_collection'
BATCH_SIZE = 1000
LOG_FILE = 'etl_pipeline.log'

# === LOGGER SETUP ===
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def log_and_print(message, level='info'):
    getattr(logging, level)(message)
    print(message)

# === MONGO CONNECTION ===
def get_mongo_collection(uri, db_name, collection_name):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # Check connection
        db = client[db_name]
        collection = db[collection_name]
        log_and_print('Connected to MongoDB successfully.')
        return collection
    except errors.ServerSelectionTimeoutError as e:
        log_and_print(f'MongoDB connection error: {e}', 'error')
        sys.exit(1)

# === EXTRACT DATA ===
def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        log_and_print(f'Extracted {len(df)} rows from {file_path}.')
        return df
    except Exception as e:
        log_and_print(f'Error reading CSV file: {e}', 'error')
        sys.exit(1)

# === TRANSFORM DATA ===
def transform_data(df):
    try:
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        df = df.where(pd.notnull(df), None)  # Correct way to replace NaN with None for MongoDB
        log_and_print('Transformation complete.')
        return df
    except Exception as e:
        log_and_print(f'Transformation error: {e}', 'error')
        sys.exit(1)

# === LOAD DATA TO MONGODB ===
def load_data_to_mongodb(df, collection):
    try:
        records = df.to_dict(orient='records')
        total_inserted = 0
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            result = collection.insert_many(batch)
            total_inserted += len(result.inserted_ids)
        log_and_print(f'Successfully inserted {total_inserted} records into MongoDB.')
    except Exception as e:
        log_and_print(f'Error loading data to MongoDB: {e}', 'error')
        sys.exit(1)

# === ETL PROCESS ===
def run_etl():
    start_time = datetime.now()
    log_and_print('ETL process started.')

    collection = get_mongo_collection(MONGO_URI, DB_NAME, COLLECTION_NAME)
    df = extract_data(CSV_FILE)
    df = transform_data(df)
    load_data_to_mongodb(df, collection)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    log_and_print(f'ETL process completed in {duration:.2f} seconds.')

if __name__ == '__main__':
    run_etl()
