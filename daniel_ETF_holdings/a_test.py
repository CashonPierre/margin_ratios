import yfinance as yf
import pandas as pd

def get_top_holdings(ticker):
    """
    Fetches top holdings for a given ETF ticker.
    """
    try:
        etf = yf.Ticker(ticker)
        if hasattr(etf.funds_data, 'top_holdings'):
            return etf.funds_data.top_holdings
    except Exception as e:
        print(f"Could not fetch data for {ticker}: {e}")
    return None

def main():
    """
    Reads a list of ETFs, fetches their top holdings, and saves them to a CSV file.
    """
    try:
        etf_list_df = pd.read_csv('ETFs_list.csv')
    except FileNotFoundError:
        print("ETFs_list.csv not found. Please make sure the file is in the same directory.")
        return

    all_holdings = []
    for ticker_us in etf_list_df['Symbol']:
        ticker = ticker_us.replace('.US', '')
        print(f"Fetching holdings for {ticker}...")
        holdings = get_top_holdings(ticker)
        if holdings is not None and not holdings.empty:
            for holding_ticker, holding_info in holdings.iterrows():
                all_holdings.append({
                    'etf_ticker': ticker,
                    'holding_ticker': holding_ticker,
                    'holding_name': holding_info['Name'],
                    'holding_percent': holding_info['Holding Percent']
                })

    if all_holdings:
        holdings_df = pd.DataFrame(all_holdings)
        holdings_df.to_csv('etf_top_holdings.csv', index=False)
        print("Successfully saved top holdings to etf_top_holdings.csv")
    else:
        print("No holdings data was fetched.")

if __name__ == "__main__":
    main()
