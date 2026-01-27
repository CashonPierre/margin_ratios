import pandas as pd
import time
from futu import *   # Change to → from moomoo import *   if you installed moomoo-api instead

# ==================== Config ====================
CSV_PATH    = "checking - Sheet8.csv"                  # Your input CSV (symbols in first column)
OUTPUT_PATH = "us_stocks_basic_info.csv"        # Where results will be saved
FAILED_PATH = "failed_symbols.csv"              # Optional: failed ones saved here

HOST = "127.0.0.1"
PORT = 11111

SLEEP_BETWEEN_REQUESTS = 3.0          # Option A: ~10 calls per 30 seconds (conservative)
# ================================================

def main():
    # 1. Load symbols from first column of CSV
    try:
        df = pd.read_csv(CSV_PATH, header=None)
        symbols = df.iloc[:, 0].dropna().astype(str).str.strip().str.upper().tolist()
        print(f"Loaded {len(symbols)} symbols from {CSV_PATH}")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    if not symbols:
        print("No valid symbols found in the file.")
        return

    # 2. Connect to Futu OpenD / moomoo OpenD
    quote_ctx = OpenQuoteContext(host=HOST, port=PORT)

    # 3. Lists for results
    results = []
    failed = []

    total = len(symbols)

    for i, sym in enumerate(symbols, 1):
        code = f"US.{sym}"

        print(f"[{i:4d}/{total:4d}] Fetching {code} ... ", end="", flush=True)

        ret, data = quote_ctx.get_stock_basicinfo(
            market=Market.US,
            stock_type=SecurityType.STOCK,
            code_list=[code]
        )

        if ret == RET_OK and not data.empty:
            results.append(data)
            print("OK")
        else:
            err_msg = str(data) if ret != RET_OK else "Empty DataFrame"
            print(f"FAILED → {err_msg}")

            # Simple rate-limit detection & retry once
            lower_err = err_msg.lower()
            if any(word in lower_err for word in ["frequency", "limit", "too frequent", "rate", "throttle"]):
                print("   → Suspected rate limit — waiting extra 35s and retrying once...")
                time.sleep(35)
                ret, data = quote_ctx.get_stock_basicinfo(
                    market=Market.US,
                    stock_type=SecurityType.STOCK,
                    code_list=[code]
                )
                if ret == RET_OK and not data.empty:
                    results.append(data)
                    print("   Retry → OK")
                else:
                    err_msg = str(data) if ret != RET_OK else "Empty after retry"
                    failed.append((code, err_msg + " (after retry)"))
                    print("   Retry → FAILED")
            else:
                failed.append((code, err_msg))

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 4. Save successful results
    if results:
        final_df = pd.concat(results, ignore_index=True)

        # Choose useful columns (adjust as needed)
        cols_to_keep = [
            'code', 'name', 'lot_size', 'stock_type', 'listing_date',
            'delisting', 'suspension', 'stock_id', 'last_trade_time'
        ]
        available = [c for c in cols_to_keep if c in final_df.columns]
        if available:
            final_df = final_df[available]

        final_df.to_csv(OUTPUT_PATH, index=False)
        print(f"\nSuccess: Saved {len(final_df)} records to → {OUTPUT_PATH}")
        print(final_df.head(8))   # preview first few rows
    else:
        print("\nNo successful data retrieved.")

    # 5. Save failed list (if any)
    if failed:
        fail_df = pd.DataFrame(failed, columns=["code", "error_reason"])
        fail_df.to_csv(FAILED_PATH, index=False)
        print(f"\n{len(failed)} symbols failed. Saved details to → {FAILED_PATH}")
        print("First few failures:")
        print(fail_df.head(10))

    # 6. Cleanup
    quote_ctx.close()
    print("\nDone.")


if __name__ == "__main__":
    # Better pandas display
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1400)
    pd.set_option('display.max_colwidth', 90)

    main()