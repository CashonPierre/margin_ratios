import requests
import time
import pandas as pd
import random

# --- Configuration ---
# The endpoint you provided
API_BASE_URL = "https://hktrade.skytigris.com/margins"

# Static parameters based on your URL
# Note: 'start' will be updated dynamically in the loop
BASE_PARAMS = {
    'region': 'HKG',
    'lang': 'zh_CN',
    'deviceId': 'web-ff2e7761-8a85-4f80-9f6f-9458804',
    'appVer': '7.26.50',
    'appName': 'web',
    'vendor': 'web',
    'platform': 'web',
    'sec_type': 'STK',
    'limit': '50',
    'market': 'US' 
}

# Headers to mimic a real browser request (good practice)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json'
}

OUTPUT_FILENAME = "tiger_margin_data_all.csv"

def fetch_all_margin_data():
    all_records = []
    
    # User specified pages 0 to 111
    start_page = 0
    end_page = 111
    
    print(f"Starting fetch for {BASE_PARAMS['market']} market (Pages {start_page}-{end_page})...")

    for page in range(start_page, end_page + 1):
        try:
            # Update the 'start' parameter for the current page
            params = BASE_PARAMS.copy()
            params['start'] = page 
            
            print(f"Fetching page {page}/{end_page}...", end=" ")
            
            response = requests.get(API_BASE_URL, params=params, headers=HEADERS, timeout=10)
            
            if response.status_code != 200:
                print(f"[Error] HTTP {response.status_code}")
                continue

            data = response.json()
            
            # Navigate the JSON structure: data -> data -> items
            # We use .get() to avoid crashing if keys are missing
            items = data.get('data', {}).get('items', [])
            
            if not items:
                print("[Info] No items found on this page.")
                # Optional: break if you want to stop when data runs out
                # break 
            
            # Process the batch of 50 items
            for item in items:
                record = {
                    'Symbol': item.get('symbol'),
                    'Name': item.get('nameCN'), # Optional: capture name if available
                    'Long Initial Margin': item.get('longInitialMargin'),
                    'Long Maint Margin': item.get('longMaintenanceMargin'),
                    'Short Initial Margin': item.get('shortInitialMargin'),
                    'Short Maint Margin': item.get('shortMaintenanceMargin'),
                    'Risk Rate': item.get('riskRate') # Optional: useful metric often included
                }
                all_records.append(record)
            
            print(f"Success. ({len(items)} items)")

            # Be polite to the server (random sleep between 1 to 2 seconds)
            time.sleep(random.uniform(1.0, 2.0))

        except Exception as e:
            print(f"\n[Exception] Failed on page {page}: {e}")
            time.sleep(5) # Wait longer if an error occurs

    # --- Save to CSV ---
    print("-" * 30)
    if all_records:
        df = pd.DataFrame(all_records)
        df.to_csv(OUTPUT_FILENAME, index=False, encoding='utf-8-sig')
        print(f"Done! Successfully saved {len(df)} records to '{OUTPUT_FILENAME}'")
        return df
    else:
        print("No records were fetched.")
        return None

# --- Execution ---
if __name__ == "__main__":
    # Ensure pandas is installed: pip install pandas
    fetch_all_margin_data()