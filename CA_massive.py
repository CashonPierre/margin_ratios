import requests
import pandas as pd

# Configuration - Replace with your actual API Key
API_KEY = "YOUR_MASSIVE_API_KEY"
BASE_URL = "https://api.massive.com/"


endpoints = {
    "ipos": f"{BASE_URL}vX/reference/ipos",
    "splits": f"{BASE_URL}stocks/v1/splits",
    "dividends": f"{BASE_URL}stocks/v1/dividends"
}



def fetch_data(endpoint_url, label):

    limit = {
        "ipos": 1000,
        "splits": 5000,
        "dividends": 5000
    }

    total_records_needed = {
        "ipos": 1000,
        "splits": 5000,
        "dividends": 5000
    }

    sorting = {
    "ipos": "listing_date",
    "splits": "execution_date.desc",
    "dividends": "ex_dividend_date.desc"
    }

    print(f"Fetching {label}...")
    all_data = []
    params = {
        "limit": limit.get(label, 1000),
        "apiKey": API_KEY,
        "sort": sorting.get(label, None),
        "offset": 0
    }

    while len(all_data) < total_records_needed.get(label, 5000):
        response = requests.get(endpoint_url, params=params)
        
        if response.status_code != 200:
            print(f"Error fetching {label}: {response.status_code}")
            break
        
        data = response.json()
        # Note: Adjust 'results' key based on actual Massive API JSON structure
        records = data.get('results', []) 
        
        if not records:
            break
            
        all_data.extend(records)
        params['offset'] += params['limit']
        
        print(f"Retrieved {len(all_data)} records...")
        
        # Safety break if API returns fewer than 5000
        if len(records) < params['limit']:
            break

    return all_data[:total_records_needed.get(label, 5000)]

# 1. Fetch the data
ipos_list = fetch_data(endpoints["ipos"], "ipos")
splits_list = fetch_data(endpoints["splits"], "splits")
dividends_list = fetch_data(endpoints["dividends"], "dividends")

# 2. Convert to DataFrames
df_ipos = pd.DataFrame(ipos_list)
df_splits = pd.DataFrame(splits_list)
df_dividends = pd.DataFrame(dividends_list)

# 3. Save to 3 individual CSV files
df_ipos.to_csv('ipos.csv', index=False)
df_splits.to_csv('splits.csv', index=False)
df_dividends.to_csv('dividends.csv', index=False)
print("CSVs created successfully.")

# 4. Combine into one Excel sheet with multiple tabs
with pd.ExcelWriter('Corporate_Actions_Summary.xlsx', engine='openpyxl') as writer:
    df_ipos.to_excel(writer, sheet_name='IPOs', index=False)
    df_splits.to_excel(writer, sheet_name='Splits', index=False)
    df_dividends.to_excel(writer, sheet_name='Dividends', index=False)

print("Excel workbook 'Corporate_Actions_Summary.xlsx' created successfully.")