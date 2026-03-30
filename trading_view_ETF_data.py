import requests
import json
import pandas as pd
import datetime

def fetch_and_export_etfs():
    url = "https://scanner.tradingview.com/america/scan?label-product=screener-etf"
    
    # Define the columns we want to request
    # These will be used as headers in our final Excel file
    columns = [
        "ticker-view", "close", "type", "typespecs", "pricescale",
        "minmov", "fractional", "minmove2", "currency", "aum",
        "fundamental_currency_code", "nav_total_return.3Y",
        "asset_class.tr", "focus.tr", "strategy.tr", "Value.Traded|1M",
        "volume", "volume|1M", "issuer.tr", "brand.tr", "category.tr",
        "exchange.tr", "leverage.tr"
    ]

    payload = {
        "columns": columns,
        "ignore_unknown_fields": False,
        "options": {"lang": "en"},
        "range": [0, 5364],
        "sort": {"sortBy": "aum", "sortOrder": "desc"},
        "markets": ["america"],
        "filter2": {
            "operator": "and",
            "operands": [
                {
                    "operation": {
                        "operator": "or",
                        "operands": [
                            {"operation": {"operator": "and", "operands": [{"expression": {"left": "typespecs", "operation": "has", "right": ["etf"]}}]}},
                            {"operation": {"operator": "and", "operands": [{"expression": {"left": "type", "operation": "equal", "right": "structured"}}]}}
                        ]
                    }
                }
            ]
        }
    }

    print("Fetching data from TradingView...")
    response = requests.post(url, json=payload)

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return

    data_source = response.json()
    items = data_source.get('data', [])
    processed_rows = []

    print(f"Processing {len(items)} rows...")

    for item in items:
        # 's' is the full ticker symbol (e.g., AMEX:SPY)
        row_data = {"Full_Symbol": item.get('s')}
        
        # 'd' is the list of values corresponding to the 'columns' requested
        d_list = item.get('d', [])

        if d_list:
            # The first element d is a dict with Name, Description, etc.
            meta_data = d_list
            if isinstance(meta_data, dict):
                row_data["Ticker"] = meta_data.get("name")
                row_data["Description"] = meta_data.get("description")

            # The rest of the elements d[1:] map 1:1 to our 'columns' list (starting from index 1)
            # We skip 'ticker-view' (index 0 of columns) because we handled it in meta_data
            data_values = d_list[1:]
            
            for i, value in enumerate(data_values):
                # Map the value to the correct column name from our payload
                col_name = columns[i+1] 
                
                # Cleanup: join lists (like ['etf']) into strings
                if isinstance(value, list):
                    value = ", ".join(str(x) for x in value)
                
                row_data[col_name] = value

        processed_rows.append(row_data)

    # Convert to DataFrame and Export
    df = pd.DataFrame(processed_rows)
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
    output_file = f"tradingview_etfs_{timestamp}.xlsx"
    
    df.to_excel(output_file, index=False)
    print(f"Success! File saved as: {output_file}")

if __name__ == "__main__":
    fetch_and_export_etfs()