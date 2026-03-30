import pandas as pd
from sqlalchemy import create_engine
import os


DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin@localhost:5433/lakehouse")
engine = create_engine(DB_URL)

def clean_t24_date(df, columns):
    
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce').dt.date
    return df

def apply_fractional_scaling(df, amount_cols, currency_col, df_currency):
   
    scaling_map = df_currency.set_index('@ID')['FRACTIONAL.DIGITS'].astype(int).to_dict()
    
    def scale_value(row, col):
        curr = row[currency_col]
        digits = scaling_map.get(curr, 0)
        try:
            val = float(row[col])
            return val / (10 ** digits)
        except:
            return 0.0

    for col in amount_cols:
        if col in df.columns:
            df[col] = df.apply(lambda row: scale_value(row, col), axis=1)
    
    return df

def run_bronze_to_silver():
    
    df_currency = pd.read_sql("SELECT * FROM bronze.f_currency", engine)
    df_currency['MID.REVAL.RATE'] = pd.to_numeric(df_currency['MID.REVAL.RATE'], errors='coerce')
    df_currency['FRACTIONAL.DIGITS'] = pd.to_numeric(df_currency['FRACTIONAL.DIGITS'], errors='coerce').fillna(0).astype(int)
    df_currency.to_sql('f_currency', engine, schema='silver', if_exists='replace', index=False)

    
    df_dates = pd.read_sql("SELECT * FROM bronze.f_dates", engine)
    df_dates = clean_t24_date(df_dates, ['TODAY', 'LAST_WORKING_DAY', 'NEXT_WORKING_DAY'])
    df_dates.to_sql('f_dates', engine, schema='silver', if_exists='replace', index=False)

   
    df_customer = pd.read_sql("SELECT * FROM bronze.f_customer", engine)
   
    for col in df_customer.select_dtypes(include=['object']).columns:
        df_customer[col] = df_customer[col].str.strip()
    df_customer.to_sql('f_customer', engine, schema='silver', if_exists='replace', index=False)

   
    df_account = pd.read_sql("SELECT * FROM bronze.f_account", engine)
    df_account = clean_t24_date(df_account, ['OPENING.DATE'])
    df_account = apply_fractional_scaling(df_account, ['ONLINE.ACTUAL.BAL', 'WORKING.BALANCE'], 'CURRENCY', df_currency)
    df_account.to_sql('f_account', engine, schema='silver', if_exists='replace', index=False)

   
    df_ft = pd.read_sql("SELECT * FROM bronze.f_funds_transfer", engine)
    df_ft = clean_t24_date(df_ft, ['CREDIT.VALUE.DATE'])
    df_ft = apply_fractional_scaling(df_ft, ['DEBIT.AMOUNT'], 'DEBIT.CURRENCY', df_currency)
    df_ft.to_sql('f_funds_transfer', engine, schema='silver', if_exists='replace', index=False)

    
    df_stmt = pd.read_sql("SELECT * FROM bronze.f_stmt_entry", engine)
    df_stmt = clean_t24_date(df_stmt, ['BOOKING.DATE', 'VALUE.DATE'])
    
    df_acc_short = df_account[['@ID', 'CURRENCY']]
    df_stmt = df_stmt.merge(df_acc_short, left_on='ACCOUNT.NUMBER', right_on='@ID', how='left', suffixes=('', '_acc'))
    df_stmt = apply_fractional_scaling(df_stmt, ['AMOUNT'], 'CURRENCY', df_currency)
    
    df_stmt = df_stmt.drop(columns=['@ID_acc', 'CURRENCY'])
    df_stmt.to_sql('f_stmt_entry', engine, schema='silver', if_exists='replace', index=False)


if __name__ == "__main__":
    run_bronze_to_silver()
