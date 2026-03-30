import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime, date, timedelta

import os


DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin@localhost:5433/lakehouse")
engine = create_engine(DB_URL)

def run_silver_to_gold_dims():
   
    df_curr = pd.read_sql("SELECT \"@ID\" as iso_code, \"FRACTIONAL.DIGITS\" as decimals FROM silver.f_currency", engine)
    names_map = {'XAF': 'Franc CFA', 'EUR': 'Euro', 'USD': 'Dollar US'}
    df_curr['currency_name'] = df_curr['iso_code'].map(names_map).fillna(df_curr['iso_code'])
    df_curr.to_sql('dim_currency', engine, schema='gold', if_exists='replace', index=False)

    
    df_cust = pd.read_sql("SELECT \"@ID\" as customer_id, \"NAME.1\" as full_name, \"SECTOR\" as sector_code, \"NATIONALITY\" as country FROM silver.f_customer", engine)
    
    sector_map = {'1001': 'Particuliers', '1002': 'Professionnels', '2001': 'Entreprises'}
    df_cust['customer_segment'] = df_cust['sector_code'].map(sector_map).fillna('Autre')
    df_cust.to_sql('dim_customer', engine, schema='gold', if_exists='replace', index=False)

  
    df_acc = pd.read_sql("SELECT \"@ID\" as account_id, \"CUSTOMER\" as customer_id, \"CATEGORY\" as category_code, \"CURRENCY\" as currency_code, \"OPENING.DATE\" as opening_date FROM silver.f_account", engine)
    
    cat_map = {'1001': 'Compte Courant', '6001': 'Compte Épargne'}
    df_acc['product_type'] = df_acc['category_code'].map(cat_map).fillna('Autre Produit')
    df_acc.to_sql('dim_account', engine, schema='gold', if_exists='replace', index=False)

    
    start_date = date(2024, 1, 1)
    end_date = date(2026, 12, 31)
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    
    df_date = pd.DataFrame({'date_day': date_list})
    df_date['date_day'] = pd.to_datetime(df_date['date_day']).dt.date
    df_date['year'] = pd.to_datetime(df_date['date_day']).dt.year
    df_date['month'] = pd.to_datetime(df_date['date_day']).dt.month
    df_date['month_name'] = pd.to_datetime(df_date['date_day']).dt.month_name()
    df_date['quarter'] = pd.to_datetime(df_date['date_day']).dt.quarter
    df_date['day_of_week'] = pd.to_datetime(df_date['date_day']).dt.day_name()
    
    df_date.to_sql('dim_date', engine, schema='gold', if_exists='replace', index=False)


if __name__ == "__main__":
    run_silver_to_gold_dims()
