import pandas as pd
from sqlalchemy import create_engine
import numpy as np

import os


DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin@localhost:5433/lakehouse")
engine = create_engine(DB_URL)

def run_silver_to_gold_facts():

    df_rates = pd.read_sql("SELECT \"@ID\" as iso_code, \"MID.REVAL.RATE\" as rate FROM silver.f_currency", engine)
    rates_map = df_rates.set_index('iso_code')['rate'].to_dict()

    
    df_acc = pd.read_sql("SELECT account_id, customer_id, currency_code, \"WORKING.BALANCE\" as working_balance FROM gold.dim_account a JOIN silver.f_account sa ON a.account_id = sa.\"@ID\"", engine)
    
    def convert_to_xaf(row, value_col):
        rate = rates_map.get(row['currency_code'], 1.0)
        return row[value_col] * rate

    df_acc['balance_xaf'] = df_acc.apply(lambda x: convert_to_xaf(x, 'working_balance'), axis=1)
    
    fact_balances = df_acc.groupby(['customer_id', 'currency_code']).agg({
        'working_balance': 'sum',
        'balance_xaf': 'sum'
    }).reset_index()
    
    fact_balances.to_sql('fact_balances', engine, schema='gold', if_exists='replace', index=False)

    
    df_stmt = pd.read_sql("SELECT \"@ID\" as trans_id, \"ACCOUNT.NUMBER\" as account_id, \"BOOKING.DATE\" as date_day, \"AMOUNT\" as amount FROM silver.f_stmt_entry", engine)
    
    
    df_acc_curr = pd.read_sql("SELECT account_id, currency_code FROM gold.dim_account", engine)
    df_stmt = df_stmt.merge(df_acc_curr, on='account_id', how='left')
    
    
    df_stmt['amount_xaf'] = df_stmt.apply(lambda x: convert_to_xaf(x, 'amount'), axis=1)
    
    df_stmt.to_sql('fact_transactions', engine, schema='gold', if_exists='replace', index=False)


if __name__ == "__main__":
    run_silver_to_gold_facts()
