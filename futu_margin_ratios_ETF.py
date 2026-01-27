from futu import *
import pandas as pd
import time
import math

# --- Configuration ---
INPUT_FILE = 'self_use.xlsx'
SHEET_NAME = 'futu_us_ETF_basic_info'
OUTPUT_FILE = 'futu_margin_ratios_ETF.csv'

# Futu OpenD Config
HOST = '127.0.0.1'
PORT = 11111

# Rate Limiting Config
# Limit: 10 requests per 30 seconds.
# Safe setting: 1 request every 3.2 seconds.
REQUEST_DELAY = 3.2 
BATCH_SIZE = 100

def fetch_futu_margin_data():
    print("--- Starting Futu Margin Fetch ---")
    
    # 1. Read the stock list from Excel
    try:
        # Reads column A. Assuming row 1 is header, data starts A2.
        # header=0 means the first row is the header.
        df_input = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME, usecols="A")
        
        # Convert column to a clean list of strings
        # Assuming the column name is known or taking the first column found
        stock_list = df_input.iloc[:, 0].astype(str).str.strip().tolist()
        
        print(f"Loaded {len(stock_list)} stocks from {INPUT_FILE}")
        
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # 2. Initialize Futu Context
    # We open the context ONCE to handle all batches
    try:
        trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, 
                                      host=HOST, 
                                      port=PORT, 
                                      security_firm=SecurityFirm.FUTUSECURITIES)
    except Exception as e:
        print(f"Failed to connect to Futu OpenD: {e}")
        return

    all_margin_dfs = []
    total_batches = math.ceil(len(stock_list) / BATCH_SIZE)

    print(f"Processing in {total_batches} batches of {BATCH_SIZE}...")

    # 3. Loop through batches
    try:
        for i in range(0, len(stock_list), BATCH_SIZE):
            batch_codes = stock_list[i : i + BATCH_SIZE]
            current_batch_num = (i // BATCH_SIZE) + 1
            
            try:
                # Call the API
                ret, data = trd_ctx.get_margin_ratio(code_list=batch_codes)

                if ret == RET_OK:
                    print(f"Batch {current_batch_num}/{total_batches}: Success ({len(data)} items)")
                    all_margin_dfs.append(data)
                else:
                    print(f"Batch {current_batch_num}/{total_batches}: Failed - {data}")
            
            except Exception as e:
                print(f"Batch {current_batch_num}/{total_batches}: Exception - {e}")

            # 4. Rate Limit Sleep
            # Don't sleep after the very last batch to save 3 seconds
            if current_batch_num < total_batches:
                time.sleep(REQUEST_DELAY)

    finally:
        # Always close connection even if code crashes
        trd_ctx.close()
        print("Futu connection closed.")

    # 5. Export to CSV
    if all_margin_dfs:
        final_df = pd.concat(all_margin_dfs, ignore_index=True)
        
        # Optional: Select and Rename columns to match your preferred format
        # If you want to keep raw columns, you can skip this renaming block
        output_columns = [
            'code', 'is_long_permit', 'is_short_permit', 'short_pool_remain',
            'short_fee_rate', 'alert_long_ratio', 'alert_short_ratio',
            'im_long_ratio', 'im_short_ratio', 'mcm_long_ratio', 
            'mcm_short_ratio', 'mm_long_ratio', 'mm_short_ratio'
        ]
        
        # Filter for columns that actually exist in the response
        cols_to_save = [c for c in output_columns if c in final_df.columns]
        final_df = final_df[cols_to_save]
        
        final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"Done! Saved {len(final_df)} records to '{OUTPUT_FILE}'")
    else:
        print("No data was fetched.")

if __name__ == "__main__":
    fetch_futu_margin_data()