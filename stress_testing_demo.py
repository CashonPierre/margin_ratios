import yfinance as yf
import pandas as pd
import numpy as np
import warnings
import os
import datetime

# Suppress Warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# 1. LOAD DATA
# Ensure the CSV is in the same folder as this script
input_file = 'margin_result_0311.csv'
if not os.path.exists(input_file):
    print(f"Error: {input_file} not found.")
    exit()

# Load and clean the CSV
df_margin = pd.read_csv(input_file)
# Mapping your specific headers:
# IM (initial margin) -> IM
# MM (maintenance margin) -> MM
# MCM (margin-call margin) -> MCM
df_margin = df_margin[['symbol', 'IM (initial margin)', 'MM (maintenance margin)', 'MCM (margin-call margin)']].copy()
df_margin.columns = ['Ticker', 'IM_raw', 'MM_raw', 'MCM_raw']

# Clean percentage strings (e.g., '55%' -> 0.55)
def clean_pct(val):
    if isinstance(val, str):
        return float(val.replace('%', '')) / 100
    return float(val)

df_margin['IM'] = df_margin['IM_raw'].apply(clean_pct)
df_margin['MM'] = df_margin['MM_raw'].apply(clean_pct)
df_margin['MCM'] = df_margin['MCM_raw'].apply(clean_pct)

# 2. CALENDAR & ASSETS
stress_events = {
    "Black Monday 1987": ("1987-10-14", "1987-10-26"),
    "DotCom Crash 2000": ("2000-03-20", "2000-04-15"),
    "Lehman/GFC 2008": ("2008-09-10", "2008-10-20"),
    "COVID Crash 2020": ("2020-03-05", "2020-03-25"),
    "Aug 2024 Unwind": ("2024-08-01", "2024-08-10"),
    "Jan 2025 AI Shock": ("2025-01-20", "2025-02-10")
}

def get_beta(symbol, period='1y'):
    try:
        data = yf.download([symbol, 'SPY'], period=period, auto_adjust=True, progress=False)['Close']
        returns = data.pct_change().dropna()
        matrix = np.cov(returns[symbol], returns['SPY'])
        return float(matrix[0, 1] / matrix[1, 1])
    except:
        return 1.5

def run_audit(ticker, im, mm, mcm, beta, beta_period):
    symbol = ticker.split('.')[0] # Remove .US suffix
    results = []
    
    # Mathematical Triggers
    # Logic: Price Ratio (P_current / P_entry) at which margin levels are hit
    im_trigger_ratio = (1 - im) / (1 - im) # Constant 1.0
    mc_trigger_ratio = (1 - im) / (1 - mcm)
    mm_trigger_ratio = (1 - im) / (1 - mm)
    liq_trigger_ratio = (1 - im) / (1 - (mm - 0.10))
    insolvency_ratio = (1 - im)
    
    for event, (start, end) in stress_events.items():
        df = yf.download(symbol, start=start, end=end, auto_adjust=True, progress=False)
        
        if not df.empty and len(df) > 1:
            # Real Data Logic: Comparing Low of day T+2 vs Close of T to capture settlement lag
            price_ratios = (df['Low'].shift(-2) / df['Close']).dropna()
            worst_ratio = float(price_ratios.min().iloc[0]) if hasattr(price_ratios.min(), "__len__") else float(price_ratios.min())
            mode = "Real"
        else:
            # Proxy Logic using SPY/GSPC adjusted by Beta
            bench = "^GSPC" if int(start[:4]) < 1993 else "SPY"
            proxy_df = yf.download(bench, start=start, end=end, auto_adjust=True, progress=False)
            if proxy_df.empty: continue
            bench_ratios = (proxy_df['Low'].shift(-2) / proxy_df['Close']).dropna()
            bench_worst_ratio = float(bench_ratios.min().iloc[0]) if hasattr(bench_ratios.min(), "__len__") else float(bench_ratios.min())
            sim_drop = (1 - bench_worst_ratio) * beta * 1.2 # Adding 20% stress buffer to beta
            worst_ratio = 1 - sim_drop
            mode = f"Sim(B={beta:.2f})"

        # Flag triggers
        is_im = 1 if worst_ratio <= im_trigger_ratio else 0
        is_mc = 1 if worst_ratio <= mc_trigger_ratio else 0
        is_mm = 1 if worst_ratio <= mm_trigger_ratio else 0
        is_liq = 1 if worst_ratio <= liq_trigger_ratio else 0
        is_fail = 1 if worst_ratio <= insolvency_ratio else 0
        
        results.append({
            "Ticker": ticker, 
            "Beta_Period": beta_period,
            "Event": event, 
            "IM_Used": f"{im:.2%}",   # New: Shows the rule used
            "MCM_Used": f"{mcm:.2%}", # New: Shows the rule used
            "MM_Used": f"{mm:.2%}",   # New: Shows the rule used
            "Max_3D_Drop": f"{1-worst_ratio:.2%}",
            "IM_Breach": is_im, 
            "MCM_Breach": is_mc, 
            "MM_Breach": is_mm, 
            "LIQ": is_liq, 
            "FAIL": is_fail, 
            "Mode": mode
        })
    return pd.DataFrame(results)

# 3. BATCH EXECUTION & EXPORT
output_file = f'Backtest_Audit_Results_{datetime.datetime.now().strftime("%Y%m%d")}.csv'
processed_items = set()
beta_periods = ['1y', '3y', '5y']

# Check for existing results to allow for resuming
if os.path.exists(output_file):
    print("Existing results file found. Resuming...")
    try:
        df_existing = pd.read_csv(output_file)
        if 'Ticker' in df_existing.columns and 'Beta_Period' in df_existing.columns:
            processed_items = set(zip(df_existing['Ticker'], df_existing['Beta_Period']))
            print(f"Skipping {len(processed_items)} already processed ticker/beta combinations.")
        else:
            print("Warning: Existing results file is malformed. Starting from scratch.")
    except pd.errors.EmptyDataError:
        print("Warning: Existing results file is empty. Starting from scratch.")
    except Exception as e:
        print(f"Warning: Could not read existing results file ({e}). Starting from scratch.")

if df_margin.empty:
    print("No tickers to process.")
else:
    print(f"Starting Backtest on {len(df_margin)} tickers for {len(beta_periods)} beta periods...")
    
    # Check if the file is new, so we know whether to write the header
    write_header = not os.path.exists(output_file) or os.path.getsize(output_file) == 0

    for _, row in df_margin.iterrows():
        for period in beta_periods:
            if (row['Ticker'], period) in processed_items:
                continue

            print(f"Analyzing {row['Ticker']} with {period} beta...")
            try:
                beta = get_beta(row['Ticker'], period=period)
                ticker_results_df = run_audit(row['Ticker'], row['IM'], row['MM'], row['MCM'], beta, period)
                
                if ticker_results_df is not None and not ticker_results_df.empty:
                    ticker_results_df.to_csv(output_file, mode='a', header=write_header, index=False)
                    # After the first write, subsequent writes in this run should not include the header
                    write_header = False
                    
            except Exception as e:
                print(f"  -> An error occurred while processing {row['Ticker']} for {period} beta: {e}")
                print(f"  -> Skipping to next combination.")
                continue

print("\n--- DONE ---")
print(f"Full results saved to: {output_file}")