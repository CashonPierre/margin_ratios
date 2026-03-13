import pandas as pd
import numpy as np

def calculate_volatility(file_path, output_path='stock_volatility_results.csv'):
    print(f"Loading {file_path}...")
    # Read only necessary columns to save memory
    df = pd.read_csv(file_path, parse_dates=['Date'], 
                     usecols=['Symbol', 'Date', 'Close'])
    
    # Ensure data is sorted by stock and then by date
    print("Sorting data...")
    df = df.sort_values(by=['Symbol', 'Date'])
    
    # Calculate daily returns: (Price_t / Price_t-1) - 1
    print("Calculating daily returns...")
    df['Return'] = df.groupby('Symbol')['Close'].pct_change()
    
    # Define volatility windows (trading days)
    # 1 month is typically 21 trading days
    windows = {
        'vol_3d': 3,
        'vol_7d': 7,
        'vol_14d': 14,
        'vol_1m': 21
    }
    
    # Calculate rolling standard deviation for each window
    # We use transform to keep the index aligned
    for col_name, window_size in windows.items():
        print(f"Calculating {window_size}-day volatility...")
        df[col_name] = (
            df.groupby('Symbol')['Return']
            .transform(lambda x: x.rolling(window=window_size).std())
        )
    
    # Extract the most recent calculation for each stock
    print("Extracting latest volatility metrics...")
    latest_volatility = df.groupby('Symbol').tail(1)
    
    # Select final columns and drop rows where volatility couldn't be calculated 
    # (e.g., if a stock has fewer than 21 days of history)
    result = latest_volatility[['Symbol', 'Date'] + list(windows.keys())]
    
    # Save to CSV
    result.to_csv(output_path, index=False)
    print(f"Success! Results saved to {output_path}")
    return result

if __name__ == "__main__":
    # Path to your large CSV file
    input_file = 'full_market_history_raw.csv'
    
    try:
        vol_df = calculate_volatility(input_file)
        print("\nPreview of Results:")
        print(vol_df.head())
    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")