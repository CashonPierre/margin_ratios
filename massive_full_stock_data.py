
import csv
import os
import requests
import time
from datetime import datetime, timedelta

# --- Configuration ---
STOCK_LIST_FILE = '/Users/matrix/Documents/margin_ratios/stock_list_20260309.csv'
OUTPUT_FILE = 'stock_summary.csv'
API_BASE_URL = 'https://api.massive.com'
# Get API key from environment variable
API_KEY = os.getenv("MASSIVE_API_KEY")
# It's good practice to use an API key, but for this example, we'll assume it's not required.
HEADERS = {'Authorization': f'Bearer {API_KEY}'}

def get_ticker_overview(ticker):
    """Fetches ticker overview data."""
    url = f"{API_BASE_URL}/v3/reference/tickers/{ticker}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        if data and data.get('results'):
            return data['results']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching overview for {ticker}: {e}")
    return None

def get_daily_bars(ticker, from_date, to_date):
    """Fetches daily OHLCV bars for a given period."""
    url = f"{API_BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching daily bars for {ticker}: {e}")
    return []

def main():
    """Main function to process stocks and generate the summary."""
    print(f"Reading stock list from {STOCK_LIST_FILE}...")
    try:
        with open(STOCK_LIST_FILE, 'r', newline='') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            stock_symbols = [row[0] for row in reader]
    except FileNotFoundError:
        print(f"Error: Stock list file not found at {STOCK_LIST_FILE}")
        return

    print(f"Found {len(stock_symbols)} stocks to process.")
    
    results = []
    
    # Define the date range for the last 30 days
    to_date = datetime.now()
    from_date = to_date - timedelta(days=30)
    to_date_str = to_date.strftime('%Y-%m-%d')
    from_date_str = from_date.strftime('%Y-%m-%d')

    for i, symbol in enumerate(stock_symbols):
        print(f"Processing {i+1}/{len(stock_symbols)}: {symbol}")

        # 1. Get Ticker Overview for market cap
        overview = get_ticker_overview(symbol)
        market_cap = overview.get('market_cap') if overview else 'N/A'

        # 2. Get Daily Bars for close price and turnover calculation
        daily_bars = get_daily_bars(symbol, from_date_str, to_date_str)

        if not daily_bars:
            print(f"No daily data for {symbol}, skipping.")
            results.append({
                'symbol': symbol,
                'last_close_price': 'N/A',
                'market_cap': market_cap,
                '30_day_avg_turnover': 'N/A'
            })
            time.sleep(0.2) # Avoid hitting API limits
            continue

        # Sort bars by date to get the last close price
        daily_bars.sort(key=lambda x: x['t'], reverse=True)
        last_close_price = daily_bars[0].get('c', 'N/A')

        # 3. Calculate 30-day average daily turnover
        total_turnover = 0
        days_with_turnover = 0
        for bar in daily_bars:
            vwap = bar.get('vw')
            volume = bar.get('v')
            if vwap is not None and volume is not None:
                total_turnover += vwap * volume
                days_with_turnover += 1
        
        avg_turnover = total_turnover / days_with_turnover if days_with_turnover > 0 else 0

        results.append({
            'symbol': symbol,
            'last_close_price': last_close_price,
            'market_cap': market_cap,
            '30_day_avg_turnover': avg_turnover if avg_turnover > 0 else 'N/A'
        })
        
        # Be a good API citizen
        time.sleep(0.2)

    # 4. Write results to CSV
    print(f"Writing results to {OUTPUT_FILE}...")
    if results:
        with open(OUTPUT_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print("Processing complete.")
    else:
        print("No results to write.")

if __name__ == "__main__":
    main()
