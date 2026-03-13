import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import os


# Get API key from environment variable
API_KEY = os.getenv("MASSIVE_API_KEY")
# --- CONFIGURATION ---
EXCEL_FILE = "tickers_for_massive.xlsx"
OUTPUT_DIR = "stock_data"
PAID_PLAN = False  # Set to True if you have a paid plan to remove delays

# Rate Limit Settings
# Free tier: 5 requests per minute (~12 seconds between calls)
DELAY_SECONDS = 13 if not PAID_PLAN else 0.1 

# Date Range: Last 3 years
end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')

def fetch_stock_data(ticker):
    """
    Fetches daily aggregate bars for a given ticker from Massive API.
    """
    url = f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
    
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        
        # Handle Rate Limiting (HTTP 429)
        if response.status_code == 429:
            print(f"Rate limit hit for {ticker}. Waiting 60 seconds...")
            time.sleep(60)
            return fetch_stock_data(ticker) # Retry once
            
        if response.status_code != 200:
            print(f"Error fetching {ticker}: {response.status_code} - {response.text}")
            return None
            
        data = response.json()
        if "results" in data:
            df = pd.DataFrame(data["results"])
            # Convert Unix ms timestamp to readable datetime
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            df.rename(columns={
                'c': 'Close', 'h': 'High', 'l': 'Low', 
                'n': 'Transactions', 'o': 'Open', 
                't': 'Date', 'v': 'Volume', 'vw': 'VWAP'
            }, inplace=True)
            df['Ticker'] = ticker
            return df
        else:
            print(f"No results found for {ticker}.")
            return None
            
    except Exception as e:
        print(f"Exception for {ticker}: {e}")
        return None

def main():
    # 1. Load tickers from Excel
    if not os.path.exists(EXCEL_FILE):
        print(f"Error: {EXCEL_FILE} not found.")
        return
        
    # Read first column (index 0)
    ticker_df = pd.read_excel(EXCEL_FILE)
    tickers = ticker_df.iloc[:, 0].dropna().unique().tolist()
    print(f"Found {len(tickers)} tickers to process.")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 2. Iterate and fetch
    all_data = []
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Fetching {ticker}...")
        
        df = fetch_stock_data(ticker)
        
        if df is not None:
            # Save individual CSV
            df.to_csv(f"{OUTPUT_DIR}/{ticker}_3yr_daily.csv", index=False)
            all_data.append(df)
            
        # 3. Respect Rate Limit
        if i < len(tickers) - 1:
            time.sleep(DELAY_SECONDS)

    # 4. Save combined results
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv("all_stocks_3yr_data.csv", index=False)
        print("Successfully saved all data to all_stocks_3yr_data.csv")

if __name__ == "__main__":
    main()