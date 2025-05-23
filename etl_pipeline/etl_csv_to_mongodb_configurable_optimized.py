
import pandas as pd
import sys
from pymongo import MongoClient, errors
from datetime import datetime
import configparser
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# === LOAD CONFIG ===
config = configparser.ConfigParser()
config.read('config.ini')

CSV_FILES = [f.strip() for f in config['etl']['csv_files'].split(',')]
MONGO_URI = config['database']['mongo_uri']
DB_NAME = config['database']['db_name']
COLLECTION_NAME = config['database']['optimized_collection']
BATCH_SIZE = int(config['etl']['batch_size'])

def log_and_print(message):
    print(f"[ETL] {message}")

# === MONGO CONNECTION ===
def get_mongo_client(uri):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.server_info()
        log_and_print('Connected to MongoDB successfully.')
        return client
    except errors.ServerSelectionTimeoutError as e:
        log_and_print(f'MongoDB connection error: {e}')
        sys.exit(1)

# === EXTRACT DATA ===
def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        log_and_print(f'Extracted {len(df)} rows from {file_path}.')
        return df
    except Exception as e:
        log_and_print(f'Error reading CSV file: {e}')
        sys.exit(1)

# === TRANSFORM DATA ===
def transform_data(df):
    try:
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        df = df.where(pd.notnull(df), None)
        log_and_print('Transformation complete.')
        return df
    except Exception as e:
        log_and_print(f'Transformation error: {e}')
        sys.exit(1)

# === LOAD DATA TO MONGODB ===
def load_data_to_mongodb(df, collection, file_name):
    try:
        records = df.to_dict(orient='records')
        total_inserted = 0
        start_time = time.time()
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            result = collection.insert_many(batch)
            total_inserted += len(result.inserted_ids)
            log_and_print(f"{file_name}: Inserted batch {i//BATCH_SIZE + 1} ({len(batch)} records).")
        duration = time.time() - start_time
        log_and_print(f'{file_name}: Finished inserting {total_inserted} records into MongoDB.')
        log_and_print(f'{file_name}: MongoDB write time: {duration:.2f} seconds.')
    except Exception as e:
        log_and_print(f'Error loading data to MongoDB: {e}')
        sys.exit(1)

# === ETL PROCESS FOR A SINGLE FILE ===
def process_file(file_name, collection):
    df = extract_data(file_name)
    df = transform_data(df)
    load_data_to_mongodb(df, collection, file_name)

# === MAIN PARALLEL ETL PROCESS ===
def run_parallel_etl():
    start_time = time.time()
    log_and_print('Parallel ETL process started.')

    client = get_mongo_client(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    with ThreadPoolExecutor(max_workers=len(CSV_FILES)) as executor:
        for file in CSV_FILES:
            executor.submit(process_file, file, collection)

    total_duration = time.time() - start_time
    log_and_print(f'Parallel ETL process completed in {total_duration:.2f} seconds.')
    log_and_print(f'Main thread: {threading.current_thread().name}')
    log_and_print(f'Active threads used: {threading.active_count()}')

if __name__ == '__main__':
    run_parallel_etl()
