import pandas as pd
import requests
import time
from futu import *


# ----------------------------- CONFIGURATION -----------------------------
# Change these to match your needs
EXCEL_FILE = 'margin_ratios.xlsx'          # Input Excel file name
OUTPUT_FILE = 'margin_ratios.xlsx'          # Output file (can be same as input to overwrite)
SHEET1_NAME = 'input'                           
HEADER_ROW = 0                               # 0 if first row is header, None if no header

# Column index for the list (0 = first column A)
LIST_COLUMN_INDEX = 0

# Sheet names for output
SHEET2_NAME = 'futu'
SHEET3_NAME = 'tiger'

# API settings
API_BASE_URL = 'https://trade.skytigris.cn/margins'
API_PARAMS = {
    'region': 'HKG',
    'lang': 'zh_CN',
    'deviceId': 'web-ff2e7761-8a85-4f80-9f6f-9458804',
    'appVer': '7.26.48',
    'appName': 'web',
    'vendor': 'web',
    'platform': 'web',
    'sec_type': 'STK',
    'start': '0',
    'limit': '10',
    'market': 'US'
}

BREAK_FUTU = 3
REQUEST_DELAY = 0.2
# ------------------------------------------------------------------------

# Step 1: Read the first sheet and get the list from the first column
print(f"Reading '{EXCEL_FILE}'...")
df_sheet1 = pd.read_excel(EXCEL_FILE, sheet_name=SHEET1_NAME, header=HEADER_ROW)

# Extract the list from the first column
item_list = df_sheet1.iloc[:, LIST_COLUMN_INDEX].dropna().tolist()
print(f"Found {len(item_list)} items in the first column:\n{item_list}\n")

# ----------------------------- YOUR FUNCTIONS -----------------------------
# Define your processing functions here.
# They should take one item (from the list) as input and return whatever you want.

def process_for_sheet2(item):
    """
    calling futu api.
    Returns a dictionary or list with results for Sheet2.
    """
    symbol = item.strip().upper()
    try:
        trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.US, host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
        ret, data = trd_ctx.get_margin_ratio(code_list=["US." + item])  
        if ret == RET_OK:
            return {
                'Symbol': symbol,
                'is_long_permit': data['is_long_permit'][0],
                'is_short_permit': data['is_short_permit'][0],
                'short_pool_remain': data['short_pool_remain'][0],
                'short_fee_rate': data['short_fee_rate'][0],
                'alert_long_ratio': data['alert_long_ratio'][0],
                'alert_short_ratio': data['alert_short_ratio'][0],
                'im_long_ratio': data['im_long_ratio'][0],
                'im_short_ratio': data['im_short_ratio'][0],
                'mcm_long_ratio': data['mcm_long_ratio'][0],
                'mcm_short_ratio': data['mcm_short_ratio'][0],
                'mm_long_ratio': data['mm_long_ratio'][0],
                'mm_short_ratio': data['mm_short_ratio'][0],
                'Error': None
            }

        else:
            print('error:', data)
            return {
                'Symbol': symbol,
                'longInitialMargin': None,
                'longMaintenanceMargin': None,
                'shortInitialMargin': None,
                'shortMaintenanceMargin': None,
                'Error': data or 'No data'
            }
    except Exception as e:
        print(f"Futu API call failed for {symbol}: {e}")
        return {
            'Symbol': symbol,
            'longInitialMargin': None,
            'longMaintenanceMargin': None,
            'shortInitialMargin': None,
            'shortMaintenanceMargin': None,
            'Error': str(e)
        }
    finally:
        trd_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽
        # Be polite to the server
        time.sleep(BREAK_FUTU)
    
    

def process_for_sheet3(item):
    """
    Calls the Tiger margin API and extracts the 4 margin fields.
    """
    symbol = item.strip().upper()
    
    # Build URL with symbol
    params = API_PARAMS.copy()
    params['symbol'] = symbol
    
    try:
        response = requests.get(API_BASE_URL, params=params, timeout=10)
        
        if response.status_code != 200:
            print(f"HTTP {response.status_code} for {symbol}")
            return {
                'Symbol': symbol,
                'longInitialMargin': None,
                'longMaintenanceMargin': None,
                'shortInitialMargin': None,
                'shortMaintenanceMargin': None,
                'Error': f'HTTP {response.status_code}'
            }
        
        data = response.json()
        
        if data.get('status') != 'ok' or not data.get('data', {}).get('items'):
            print(f"No data or error for {symbol}: {data.get('msg')}")
            return {
                'Symbol': symbol,
                'longInitialMargin': None,
                'longMaintenanceMargin': None,
                'shortInitialMargin': None,
                'shortMaintenanceMargin': None,
                'Error': data.get('msg') or 'No data'
            }
        
        item_data = data['data']['items'][0]
        
        return {
            'Symbol': symbol,
            'longInitialMargin': item_data.get('longInitialMargin'),
            'longMaintenanceMargin': item_data.get('longMaintenanceMargin'),
            'shortInitialMargin': item_data.get('shortInitialMargin'),
            'shortMaintenanceMargin': item_data.get('shortMaintenanceMargin'),
            'Error': None
        }
        
    except requests.RequestException as e:
        print(f"Request failed for {symbol}: {e}")
        return {
            'Symbol': symbol,
            'longInitialMargin': None,
            'longMaintenanceMargin': None,
            'shortInitialMargin': None,
            'shortMaintenanceMargin': None,
            'Error': str(e)
        }
    
    finally:
        # Be polite to the server
        time.sleep(REQUEST_DELAY)

# You can define more functions if needed
# ------------------------------------------------------------------------

# Step 2: Run the functions on each item and collect results
print("Processing items...")

results_sheet2 = []
results_sheet3 = []

for item in item_list:
    try:
        res2 = process_for_sheet2(item)
        results_sheet2.append(res2)
        
        res3 = process_for_sheet3(item)
        results_sheet3.append(res3)
    except Exception as e:
        print(f"Error processing item '{item}': {e}")
        # Optionally append error info
        results_sheet2.append({'Error': str(e)})
        results_sheet3.append({'Error': str(e)})
    # break #testing only one item

# Convert to DataFrames
df_sheet2 = pd.DataFrame(results_sheet2)
df_sheet3 = pd.DataFrame(results_sheet3)

# Step 3: Write results to new sheets
print(f"Writing results to '{OUTPUT_FILE}'...")

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    # Optionally write the original first sheet back
    df_sheet1.to_excel(writer, sheet_name='input', index=False)
    
    # Write the new sheets
    df_sheet2.to_excel(writer, sheet_name=SHEET2_NAME, index=False)
    df_sheet3.to_excel(writer, sheet_name=SHEET3_NAME, index=False)

print("Done! Check the output file for Sheet2 and Sheet3.")