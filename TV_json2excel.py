import json
import pandas as pd

def json_to_excel(input_file, output_file):
    try:
        # 1. Load the JSON data
        with open(input_file, 'r', encoding='utf-8') as f:
            data_source = json.load(f)
        
        # Access the list of items under the "data" key
        items = data_source.get('data', [])
        
        processed_rows = []

        for item in items:
            # The 's' key likely holds the unique ID or Full Ticker
            row_data = {
                "Full_Symbol": item.get('s')
            }
            
            # The 'd' key is a list containing mixed data
            # d[0] is a dictionary with metadata (Name, Description, etc.)
            # d[1:] are the raw values (Prices, Market Cap, etc.)
            d_list = item.get('d', [])
            
            if d_list:
                # --- PART A: Process the named metadata (Index 0) ---
                meta_data = d_list[0]
                if isinstance(meta_data, dict):
                    # Extract specific fields you likely want
                    row_data["Ticker"] = meta_data.get("name")
                    row_data["Description"] = meta_data.get("description")
                    row_data["Exchange"] = meta_data.get("exchange")
                    row_data["Type"] = meta_data.get("type")
                
                # --- PART B: Process the unnamed data (Index 1 onwards) ---
                # We loop through the rest of the list and assign generic names
                unnamed_values = d_list[1:]
                
                for i, value in enumerate(unnamed_values):
                    # Create a column name like "Column_1", "Column_2", etc.
                    col_name = f"Column_{i+1}"
                    
                    # specific cleanup: if the value is a list (like ["etf"]), join it into a string
                    if isinstance(value, list):
                        value = ", ".join(str(x) for x in value)
                        
                    row_data[col_name] = value

            processed_rows.append(row_data)

        # 2. Create DataFrame
        df = pd.DataFrame(processed_rows)

        # 3. Export to Excel
        df.to_excel(output_file, index=False)
        print(f"Success! Converted {len(df)} rows to '{output_file}'.")

    except Exception as e:
        print(f"An error occurred: {e}")

# --- Execution ---
if __name__ == "__main__":
    # Ensure your json file is named 'trading_view.json'
    json_to_excel('trading_view_3.json', 'trading_view_output3.xlsx')