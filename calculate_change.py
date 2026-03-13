import pandas as pd

def calculate_one_year_change_and_save():
    try:
        df = pd.read_csv('full_market_history_raw.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        
        results_list = []
        
        for symbol, group in df.groupby('Symbol'):
            group = group.sort_values(by='Date', ascending=True)
            
            if len(group) < 2:
                continue

            latest_date = group['Date'].iloc[-1]
            one_year_ago = latest_date - pd.DateOffset(years=1)
            
            latest_price_row = group.iloc[-1]
            one_year_ago_row = group[group['Date'] <= one_year_ago].iloc[-1] if not group[group['Date'] <= one_year_ago].empty else None
            
            if one_year_ago_row is not None:
                latest_price = latest_price_row['Close']
                one_year_ago_price = one_year_ago_row['Close']
                
                if pd.notna(one_year_ago_price) and one_year_ago_price != 0:
                    percentage_change = ((latest_price - one_year_ago_price) / one_year_ago_price) * 100
                    results_list.append({'Symbol': symbol, '1_Year_Change_%': percentage_change})

        if results_list:
            results_df = pd.DataFrame(results_list)
            results_df.to_csv('one_year_change.csv', index=False)
            print("Successfully saved the results to one_year_change.csv")
        else:
            print("No results to save.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    calculate_one_year_change_and_save()
