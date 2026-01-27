from futu import *
import pandas as pd
import time

# --- Configuration ---
INPUT_FILE = 'self_use.xlsx'
SHEET_NAME = 'futu_us_missing6_basic_info'
OUTPUT_FILE = 'futu_margin_ratios_missing6.1.csv'

# Futu OpenD Config
HOST = '127.0.0.1'
PORT = 11111

# Rate Limiting Config
# Futu limit: 10 requests per 30 seconds → safest is ~1 request every 3.2 seconds
REQUEST_DELAY = 3.2

def fetch_futu_margin_data():
    print("--- Starting Futu Margin Fetch (Individual Mode) ---")
    
    # 1. Read the stock list from Excel
    try:
        df_input = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME, usecols="A")
        stock_list = df_input.iloc[:, 0].astype(str).str.strip().tolist()
        stock_list = ["US.JBSAY.FTOLD"]
        print(f"Loaded {len(stock_list)} stocks from {INPUT_FILE}")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # 2. Initialize Futu Context
    try:
        trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, 
                                      host=HOST, 
                                      port=PORT, 
                                      security_firm=SecurityFirm.FUTUSECURITIES)
        print("Connected to Futu OpenD successfully.")
    except Exception as e:
        print(f"Failed to connect to Futu OpenD: {e}")
        return

    all_margin_dfs = []
    success_count = 0
    fail_count = 0

    print("Processing stocks one by one...\n")

    # 3. Process each stock individually
    for i, code in enumerate(stock_list, 1):
        try:
            ret, data = trd_ctx.get_margin_ratio(code_list=[code])

            if ret == RET_OK:
                if not data.empty:
                    all_margin_dfs.append(data)
                    success_count += 1
                    print(f"[{i}/{len(stock_list)}] SUCCESS: {code}")
                else:
                    fail_count += 1
                    print(f"[{i}/{len(stock_list)}] NO DATA: {code}")
            else:
                fail_count += 1
                print(f"[{i}/{len(stock_list)}] FAILED: {code} - {data}")

        except Exception as e:
            fail_count += 1
            print(f"[{i}/{len(stock_list)}] EXCEPTION: {code} - {e}")

        # Rate limiting: sleep between requests (skip after absolute last one)
        if i < len(stock_list):
            time.sleep(REQUEST_DELAY)

    # 4. Close connection
    trd_ctx.close()
    print("\nFutu connection closed.")

    # 5. Summary & Export
    print(f"\n=== Summary ===")
    print(f"Successful fetches: {success_count}")
    print(f"Failed / No data: {fail_count}")
    print(f"Total processed: {len(stock_list)}")

    if all_margin_dfs:
        final_df = pd.concat(all_margin_dfs, ignore_index=True)
        
        # Keep only desired columns if they exist
        desired_columns = [
            'code', 'is_long_permit', 'is_short_permit', 'short_pool_remain',
            'short_fee_rate', 'alert_long_ratio', 'alert_short_ratio',
            'im_long_ratio', 'im_short_ratio', 'mcm_long_ratio', 
            'mcm_short_ratio', 'mm_long_ratio', 'mm_short_ratio'
        ]
        cols_to_save = [c for c in desired_columns if c in final_df.columns]
        final_df = final_df[cols_to_save]
        
        final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\nDone! Saved {len(final_df)} valid records to '{OUTPUT_FILE}'")
    else:
        print("\nNo data was fetched at all.")

if __name__ == "__main__":
    fetch_futu_margin_data()