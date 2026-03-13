import pandas as pd

def preprocess_etf_data(input_file, output_file):
    # Load the dataset
    df = pd.read_csv(input_file)
    
    # Convert Date to datetime and sort
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(['Symbol', 'Date'])
    
    # Calculate Last Close Price
    last_close = df.groupby('Symbol')['Close'].last().reset_index()
    last_close.columns = ['Symbol', 'Last_Close']
    
    # Calculate 30-day average turnover (last 30 trading records)
    avg_turnover = df.groupby('Symbol')['Turnover'].apply(lambda x: x.tail(30).mean()).reset_index()
    avg_turnover.columns = ['Symbol', 'Avg_Turnover_30D']
    
    # Merge results
    result = pd.merge(last_close, avg_turnover, on='Symbol')
    
    # Save to CSV
    result.to_csv(output_file, index=False)
    return result

# Usage
result_df = preprocess_etf_data('full_market_history_raw_ETF.csv', 'processed_etf_data.csv')
print(result_df.head())