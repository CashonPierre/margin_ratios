from futu import *
import pandas as pd
import time
import math

# --- Configuration ---
INPUT_FILE = 'self_use.xlsx'
SHEET_NAME = 'futu_margin_ratios_all_target'
OUTPUT_FILE = 'futu_margin_ratios_all_target.csv'
ERROR_FILE = 'futu_invalid_tickers.txt'

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
        df_input = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME, usecols="C")
        # Clean list: remove whitespace and drop empty values
        stock_list = df_input.iloc[:, 0].astype(str).str.strip()
        stock_list = stock_list[stock_list != 'nan'].tolist() # Filter out empty rows
        
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
    except Exception as e:
        print(f"Failed to connect to Futu OpenD: {e}")
        return

    all_margin_dfs = []
    invalid_tickers = [] # List to store bad tickers

    # --- Helper Function for Intelligent Retry ---
    def fetch_safe_batch(codes):
        """
        Tries to fetch a batch. If it fails, splits the batch in half 
        and recurses to isolate the bad ticker.
        """
        # Call API
        ret, data = trd_ctx.get_margin_ratio(code_list=codes)
        
        # Respect Rate Limit (Sleep after every API call)
        time.sleep(REQUEST_DELAY)

        if ret == RET_OK:
            return data
        else:
            # If the batch failed...
            
            # Case 1: It was a single stock. This is definitely a bad ticker.
            if len(codes) == 1:
                print(f"   [!] Found Unfetchable Ticker: {codes[0]} (Error: {data})")
                invalid_tickers.append(codes[0])
                return None
            
            # Case 2: It was a group. One of them is bad. Split and conquer.
            mid = len(codes) // 2
            left = codes[:mid]
            right = codes[mid:]
            
            print(f"   Batch failed. Splitting into {len(left)} and {len(right)} to isolate error...")
            
            res_left = fetch_safe_batch(left)
            res_right = fetch_safe_batch(right)
            
            # Combine results from split
            results = []
            if res_left is not None: results.append(res_left)
            if res_right is not None: results.append(res_right)
            
            if results:
                return pd.concat(results)
            else:
                return None
    # ---------------------------------------------

    total_batches = math.ceil(len(stock_list) / BATCH_SIZE)
    print(f"Processing in {total_batches} batches of {BATCH_SIZE}...")

    # 3. Loop through batches
    try:
        for i in range(0, len(stock_list), BATCH_SIZE):
            batch_codes = stock_list[i : i + BATCH_SIZE]
            current_batch_num = (i // BATCH_SIZE) + 1
            
            print(f"Processing Batch {current_batch_num}/{total_batches} ({len(batch_codes)} items)...")
            
            # Use the new safe fetch function
            batch_result = fetch_safe_batch(batch_codes)
            
            if batch_result is not None:
                all_margin_dfs.append(batch_result)
                print(f"Batch {current_batch_num} Done. Total records so far: {sum(len(df) for df in all_margin_dfs)}")

    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
    except Exception as e:
        print(f"Unexpected global error: {e}")

    finally:
        trd_ctx.close()
        print("Futu connection closed.")

    # 4. Export Valid Data to CSV
    if all_margin_dfs:
        final_df = pd.concat(all_margin_dfs, ignore_index=True)
        
        output_columns = [
            'code', 'is_long_permit', 'is_short_permit', 'short_pool_remain',
            'short_fee_rate', 'alert_long_ratio', 'alert_short_ratio',
            'im_long_ratio', 'im_short_ratio', 'mcm_long_ratio', 
            'mcm_short_ratio', 'mm_long_ratio', 'mm_short_ratio'
        ]
        
        cols_to_save = [c for c in output_columns if c in final_df.columns]
        final_df = final_df[cols_to_save]
        
        final_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"--- SUCCESS ---")
        print(f"Saved {len(final_df)} valid records to '{OUTPUT_FILE}'")
    else:
        print("No valid data was fetched.")

    # 5. Export Invalid Tickers
    if invalid_tickers:
        print(f"--- WARNING ---")
        print(f"Found {len(invalid_tickers)} unfetchable tickers.")
        print(f"Saving list to {ERROR_FILE}...")
        with open(ERROR_FILE, 'w') as f:
            for ticker in invalid_tickers:
                f.write(f"{ticker}\n")
    else:
        print("No invalid tickers found. Perfect run.")

if __name__ == "__main__":
    fetch_futu_margin_data()