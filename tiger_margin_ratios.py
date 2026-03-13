import requests
import time
import pandas as pd

def fetch_and_save_data():
    base_url = "https://hktrade.skytigris.com/margins"
    
    params = {
        "region": "HKG",
        "lang": "zh_CN",
        "deviceId": "web-ff2e7761-8a85-4f80-9f6f-9458804",
        "appVer": "7.26.50",
        "appName": "web",
        "vendor": "web",
        "platform": "web",
        "sec_type": "STK",
        "market": "US",
        "limit": 50 
    }

    all_records = []
    current_start = 0

    while True:
        params['start'] = current_start
        params['_s'] = int(time.time() * 1000)

        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            full_json = response.json()

            # --- DEBUG SECTION ---
            # If this is the first loop, let's see what the API actually looks like
            if current_start == 0:
                print(f"API Response Keys: {full_json.keys()}")
                # Common structure for this API is usually full_json['data']['items'] 
                # or full_json['data']['list']
            # ---------------------

            # Let's try to find the list automatically
            items = []
            if 'data' in full_json:
                data_content = full_json['data']
                if isinstance(data_content, list):
                    items = data_content
                elif isinstance(data_content, dict):
                    # Check common sub-keys
                    items = data_content.get('items', data_content.get('list', data_content.get('data', [])))

            if not items:
                print(f"No items found at start={current_start}. Stopping.")
                break

            all_records.extend(items)
            print(f"Collected {len(all_records)} items...")
            
            current_start += len(items) # Use the actual number of items returned
            time.sleep(0.3)

            # Safety break for your specific case (the last call you mentioned was ~5480)
            if current_start > 10000: 
                break

        except Exception as e:
            print(f"Error: {e}")
            break

    if all_records:
        df = pd.DataFrame(all_records)
        df.to_csv("margin_data.csv", index=False, encoding="utf-8-sig")
        print(f"\nSuccess! Saved {len(all_records)} rows to 'margin_data.csv'.")
    else:
        print("\nStill no data. Please copy-paste the 'API Response Keys' printed above so I can fix the path!")

if __name__ == "__main__":
    fetch_and_save_data()