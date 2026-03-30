import pandas as pd
import random
import os
from datetime import datetime, timedelta
from faker import Faker

WEEKS = 10
DAYS_PER_WEEK = 7
START_DATE = datetime(2026, 3, 22)
INITIAL_CUSTOMERS = 1000
INITIAL_ACCOUNTS = 3000 
NEW_CUSTOMERS_PER_WEEK = 20
NEW_ACCOUNTS_PER_WEEK = 50
TX_PER_DAY = 350 

fake = Faker(['fr_FR'])

def generate_customer(cid):
    name = fake.name().upper()
    mnemonic = name.replace(" ", "")[:10]
    return {
        "@ID": str(cid),
        "MNEMONIC": mnemonic,
        "SHORT.NAME": name[:15],
        "NAME.1": name,
        "SECTOR": random.choice(["1001", "1002", "2001"]),
        "ACCOUNT.OFFICER": random.choice(["1", "2", "3"]),
        "NATIONALITY": "CG",
        "RESIDENCE": "CG"
    }

def generate_account(aid, cid, start_date):
    curr = random.choices(["XAF", "EUR", "USD"], weights=[0.8, 0.1, 0.1])[0]
    raw_bal = random.randint(20000000, 500000000) 
    return {
        "@ID": str(aid), "CUSTOMER": cid, "CATEGORY": random.choice(["1001", "6001"]), "CURRENCY": curr,
        "ONLINE.ACTUAL.BAL": str(raw_bal), "WORKING.BALANCE": str(raw_bal),
        "OPENING.DATE": (start_date - timedelta(days=random.randint(100, 1100))).strftime('%Y%m%d')
    }

def generate_10_weeks():
    
    
    currencies = [
        {"@ID": "XAF", "CURRENCY.MARKET": "1", "MID.REVAL.RATE": "1.0", "FRACTIONAL.DIGITS": "0", "QUOTATION.CODE": "1"},
        {"@ID": "EUR", "CURRENCY.MARKET": "1", "MID.REVAL.RATE": "655.95", "FRACTIONAL.DIGITS": "2", "QUOTATION.CODE": "1"},
        {"@ID": "USD", "CURRENCY.MARKET": "1", "MID.REVAL.RATE": "610.25", "FRACTIONAL.DIGITS": "2", "QUOTATION.CODE": "1"}
    ]
    df_currency = pd.DataFrame(currencies)

    current_customers = [generate_customer(100000 + i) for i in range(INITIAL_CUSTOMERS)]
    cust_ids = [c["@ID"] for c in current_customers]
    
    current_accounts = [generate_account(2000000 + i, random.choice(cust_ids), START_DATE) for i in range(INITIAL_ACCOUNTS)]
    
    accs_by_currency = {}
    for a in current_accounts:
        curr = a["CURRENCY"]
        if curr not in accs_by_currency: accs_by_currency[curr] = []
        accs_by_currency[curr].append(a["@ID"])
    
    customer_name_map = {c["@ID"]: c["NAME.1"] for c in current_customers}
    acc_to_cust = {a["@ID"]: a["CUSTOMER"] for a in current_accounts}

    last_cust_id = 100000 + INITIAL_CUSTOMERS
    last_acc_id = 2000000 + INITIAL_ACCOUNTS

    for week in range(1, WEEKS + 1):
        week_start_date = START_DATE + timedelta(weeks=week-1)
        week_dir = f"data/semaine_{week}"
        os.makedirs(week_dir, exist_ok=True)
        

        if week > 1:
            new_custs = [generate_customer(last_cust_id + i) for i in range(NEW_CUSTOMERS_PER_WEEK)]
            current_customers.extend(new_custs)
            for c in new_custs:
                customer_name_map[c["@ID"]] = c["NAME.1"]
            last_cust_id += NEW_CUSTOMERS_PER_WEEK
            cust_ids = [c["@ID"] for c in current_customers]
            
            new_accs = [generate_account(last_acc_id + i, random.choice(cust_ids), week_start_date) for i in range(NEW_ACCOUNTS_PER_WEEK)]
            current_accounts.extend(new_accs)
            for a in new_accs:
                acc_to_cust[a["@ID"]] = a["CUSTOMER"]
                curr = a["CURRENCY"]
                if curr not in accs_by_currency: accs_by_currency[curr] = []
                accs_by_currency[curr].append(a["@ID"])
            last_acc_id += NEW_ACCOUNTS_PER_WEEK

        dates_list = []
        for i in range(DAYS_PER_WEEK):
            d = week_start_date + timedelta(days=i)
            dates_list.append({
                "@ID": "BNK", "TODAY": d.strftime('%Y%m%d'),
                "LAST_WORKING_DAY": (d - timedelta(days=1)).strftime('%Y%m%d'),
                "NEXT_WORKING_DAY": (d + timedelta(days=1)).strftime('%Y%m%d')
            })
        
        fts = []
        stmts = []
        for d_idx in range(DAYS_PER_WEEK):
            current_dt_str = (week_start_date + timedelta(days=d_idx)).strftime('%Y%m%d')
            for t_idx in range(TX_PER_DAY):
                ft_id = f"FT{week:02d}{d_idx}{t_idx:04d}"
                
                # --- Sélection d une devise au hasard possédant au moins 2 comptes ---
                curr_target = random.choice([c for c, ids in accs_by_currency.items() if len(ids) > 1])
                acc_d = random.choice(accs_by_currency[curr_target])
                acc_c = random.choice([a for a in accs_by_currency[curr_target] if a != acc_d])
                
                raw_amt = random.randint(10000, 5000000)
                sender_name = customer_name_map[acc_to_cust[acc_d]]

                fts.append({
                    "@ID": ft_id, "TRANSACTION.TYPE": "AC", "DEBIT.ACCT.NO": acc_d, "DEBIT.CURRENCY": curr_target,
                    "DEBIT.AMOUNT": str(raw_amt), "CREDIT.ACCT.NO": acc_c, "CREDIT.VALUE.DATE": current_dt_str, "ORDERING.CUST": sender_name
                })

                stmts.append({
                    "@ID": f"S{ft_id}01", "ACCOUNT.NUMBER": acc_d, "BOOKING.DATE": current_dt_str, "VALUE.DATE": current_dt_str,
                    "AMOUNT": f"-{raw_amt}", "TRANSACTION.CODE": "2", "TRANS.REFERENCE": ft_id
                })
                stmts.append({
                    "@ID": f"S{ft_id}02", "ACCOUNT.NUMBER": acc_c, "BOOKING.DATE": current_dt_str, "VALUE.DATE": current_dt_str,
                    "AMOUNT": str(raw_amt), "TRANSACTION.CODE": "1", "TRANS.REFERENCE": ft_id
                })

        pd.DataFrame(current_customers).to_csv(f"{week_dir}/f_customer_bronze.csv", index=False)
        pd.DataFrame(current_accounts).to_csv(f"{week_dir}/f_account_bronze.csv", index=False)
        pd.DataFrame(dates_list).to_csv(f"{week_dir}/f_dates_bronze.csv", index=False)
        df_currency.to_csv(f"{week_dir}/f_currency_bronze.csv", index=False)
        pd.DataFrame(fts).to_csv(f"{week_dir}/f_funds_transfer_bronze.csv", index=False)
        pd.DataFrame(stmts).to_csv(f"{week_dir}/f_stmt_entry_bronze.csv", index=False)


if __name__ == "__main__":
    generate_10_weeks()