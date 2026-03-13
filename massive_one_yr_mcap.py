
import pandas as pd
import requests
import datetime
import time
import os
import concurrent.futures
import sys

# Get API key from environment variable
API_KEY = os.getenv("MASSIVE_API_KEY")
MAX_WORKERS = 20 # Adjust the number of threads as needed

def get_market_cap(ticker, api_key):
    """
    Fetches the last 300 trading days of market capitalization for a given ticker.
    It also returns the listing date of the stock.
    """
    print(f"Fetching data for {ticker}...")
    # First, get the list date for the ticker
    list_date_str = None
    try:
        url = f"https://api.massive.com/v3/reference/tickers/{ticker}?apiKey={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if 'results' in data and 'list_date' in data['results']:
            list_date_str = data['results']['list_date']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching list date for {ticker}: {e}")
        return ticker, (None, None)
    except Exception as e:
        print(f"An error occurred while fetching list date for {ticker}: {e}")
        return ticker, (None, None)

    if not list_date_str:
        return ticker, ([], None)

    list_date = datetime.datetime.strptime(list_date_str, "%Y-%m-%d").date()

    market_caps = []
    today = datetime.date.today()
    days_fetched = 0
    current_date = today
    consecutive_no_data_days = 0
    
    # We want to go back at most 300 trading days, but not before the list_date
    max_days_back = (today - list_date).days
    
    # Calculate the number of days to fetch
    days_to_fetch = min(300, max_days_back)
    
    # To avoid making too many requests, we can estimate the start date
    # This is not perfect due to holidays, but it's a good starting point
    estimated_start_date = today - datetime.timedelta(days=int(days_to_fetch * 1.5))
    
    # Make sure we don't go past the list date
    start_date = max(list_date, estimated_start_date)

    while current_date >= start_date and days_fetched < 300 and consecutive_no_data_days < 30:
        # Skip weekends (Saturday and Sunday)
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%Y-%m-%d")
            url = f"https://api.massive.com/v3/reference/tickers/{ticker}?date={date_str}&apiKey={api_key}"
            try:
                response = requests.get(url)
                response.raise_for_status()  # Raise an exception for bad status codes
                data = response.json()

                if 'results' in data and 'market_cap' in data['results']:
                    market_caps.append({
                        "date": date_str,
                        "ticker": ticker,
                        "market_cap": data['results']['market_cap']
                    })
                    days_fetched += 1
                    consecutive_no_data_days = 0  # Reset counter on success
                else:
                    consecutive_no_data_days += 1
                
            except requests.exceptions.RequestException as e:
                consecutive_no_data_days += 1
                if response.status_code == 404:
                    # This is a common case for holidays, so we don't need to print it every time
                    pass
                else:
                    print(f"Error fetching data for {ticker} on {date_str}: {e}")
            except Exception as e:
                consecutive_no_data_days += 1
                print(f"An error occurred for {ticker} on {date_str}: {e}")

            time.sleep(0.1)

        current_date -= datetime.timedelta(days=1)
    
    if consecutive_no_data_days >= 30:
        print(f"Skipping {ticker} after 30 consecutive days with no data.")

    return ticker, (market_caps, list_date_str)

def main():
    """
    Main function to read tickers, fetch market cap data, and save to a CSV file.
    """
    if not API_KEY:
        print("Error: MASSIVE_API_KEY environment variable not set.")
        print("Please set the environment variable and try again.")
        sys.exit(1)

    mcap_output_filename = "massive_one_yr_mcap.csv"
    list_dates_output_filename = "stock_list_dates.csv"

    # Check if files exist to determine if we need to write headers
    mcap_file_exists = os.path.exists(mcap_output_filename)
    list_dates_file_exists = os.path.exists(list_dates_output_filename)

    processed_tickers = set()
    if list_dates_file_exists:
        try:
            processed_tickers_df = pd.read_csv(list_dates_output_filename)
            if not processed_tickers_df.empty:
                processed_tickers = set(processed_tickers_df['ticker'].unique())
                print(f"Resuming script. Found {len(processed_tickers)} already processed tickers.")
        except pd.errors.EmptyDataError:
            print(f"{list_dates_output_filename} is empty. Starting from scratch.")
        except Exception as e:
            print(f"Error reading {list_dates_output_filename}: {e}. Starting from scratch.")


    try:
        tickers_df = pd.read_csv("futu_AVS_combine_list.csv")
    except FileNotFoundError:
        print("Error: futu_AVS_combine_list.csv not found.")
        return

    tickers_to_process = [ticker for ticker in tickers_df["symbol"].unique() if ticker not in processed_tickers]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks to the executor
        future_to_ticker = {executor.submit(get_market_cap, ticker, API_KEY): ticker for ticker in tickers_to_process}

        with open(mcap_output_filename, 'a', newline='') as mcap_f, \
             open(list_dates_output_filename, 'a', newline='') as dates_f:

            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker, result = future.result()
                market_caps, list_date = result
                
                try:
                    if market_caps:
                        market_cap_df = pd.DataFrame(market_caps)
                        market_cap_df.to_csv(mcap_f, header=not mcap_file_exists, index=False)
                        mcap_file_exists = True

                    if list_date:
                        list_dates_df = pd.DataFrame([{"ticker": ticker, "list_date": list_date}])
                        list_dates_df.to_csv(dates_f, header=not list_dates_file_exists, index=False)
                        list_dates_file_exists = True

                except Exception as exc:
                    print(f'{ticker} generated an exception during file write: {exc}')

    print("Processing complete.")
    if os.path.exists(mcap_output_filename):
        print(f"Market cap data saved to {mcap_output_filename}")
    if os.path.exists(list_dates_output_filename):
        print(f"Stock list dates saved to {list_dates_output_filename}")

if __name__ == "__main__":
    main()
