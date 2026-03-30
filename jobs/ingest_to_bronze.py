import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
import re


DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin@localhost:5433/lakehouse")
engine = create_engine(DB_URL)

def truncate_append_tables():
    tables = ["bronze.f_dates", "bronze.f_funds_transfer", "bronze.f_stmt_entry"]
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(tables)} RESTART IDENTITY CASCADE"))

def ingest_folder(data_dir):    
    TABLE_CONFIG = {
        "f_currency_bronze.csv": {"table": "f_currency", "strategy": "replace"},
        "f_customer_bronze.csv": {"table": "f_customer", "strategy": "replace"},
        "f_account_bronze.csv":  {"table": "f_account",  "strategy": "replace"},
        "f_dates_bronze.csv":    {"table": "f_dates",    "strategy": "append"},
        "f_funds_transfer_bronze.csv": {"table": "f_funds_transfer", "strategy": "append"},
        "f_stmt_entry_bronze.csv":     {"table": "f_stmt_entry",     "strategy": "append"}
    }
    
    for csv_file_name, config in TABLE_CONFIG.items():
        csv_path = os.path.join(data_dir, csv_file_name)
        if not os.path.exists(csv_path): continue
        
        table_name = config["table"]
        strategy = config["strategy"]
        
        try:
            df = pd.read_csv(csv_path, dtype=str)
            df.to_sql(table_name, engine, schema="bronze", if_exists=strategy, index=False)
        except Exception as e:
            print(f"Erreur sur {table_name} ({data_dir}): {e}")

def get_base_data_dir():
    docker_path = "/opt/airflow/data"
    if os.path.exists(docker_path):
        return docker_path
    
    local_path = "data"
    if os.path.exists(local_path):
        return local_path
    
    parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    if os.path.exists(parent_path):
        return parent_path
    
    raise FileNotFoundError("Could not find 'data' directory in /opt/airflow/data or current/parent directory.")

def run_global_ingestion():
    
    base_dir = get_base_data_dir()
    
    folders = [f for f in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, f)) and f.startswith("semaine_")]
    
    if not folders:
        ingest_folder(base_dir)
        return

    def natural_sort_key(s):
        return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]
    
    folders.sort(key=natural_sort_key)

    truncate_append_tables()

    for folder in folders:
        ingest_folder(os.path.join(base_dir, folder))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        ingest_folder(sys.argv[1])
    else:
        run_global_ingestion() 
