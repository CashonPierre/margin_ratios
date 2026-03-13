import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import argparse
from tqdm import tqdm  # optional progress bar (remove if you don't want it)

def calculate_multi_day_var(
    df_symbol,
    var_days,
    data_years,
    confidence=0.95,
    end_date=None
):
    """
    Calculate n-day VaR using historical simulation on given symbol dataframe.
    Returns VaR as positive percentage (potential loss) or None if failed.
    """
    if df_symbol.empty:
        return None

    df = df_symbol.sort_values('Date').reset_index(drop=True)
    df['Date'] = pd.to_datetime(df['Date'])

    if end_date is None:
        end_dt = df['Date'].max()
    else:
        end_dt = pd.to_datetime(end_date)

    # Approximate lookback (using 252 trading days/year)
    trading_days_per_year = 252
    lookback_days = int(data_years * trading_days_per_year)

    # Rough calendar days estimate for filtering
    start_dt_approx = end_dt - timedelta(days=int(lookback_days * 365 / 252 + 60))  # +buffer

    hist = df[(df['Date'] >= start_dt_approx) & (df['Date'] <= end_dt)].copy()

    if len(hist) < lookback_days // 2:  # very rough check
        return None

    # Daily log returns
    hist['log_return'] = np.log(hist['Close'] / hist['Close'].shift(1))

    # Rolling n-day cumulative log return
    hist['n_day_log_ret'] = hist['log_return'].rolling(window=var_days).sum()

    # Drop NaNs
    returns_series = hist['n_day_log_ret'].dropna()

    if len(returns_series) < 30:  # too few points → unreliable
        return None

    # Convert to simple returns
    simple_n_day_returns = np.exp(returns_series) - 1

    # Historical VaR: percentile of worst losses
    percentile = (1 - confidence) * 100
    var_simple = np.percentile(simple_n_day_returns, percentile)

    # VaR as positive percentage loss
    var_pct = -var_simple * 100

    return var_pct


def main():
    parser = argparse.ArgumentParser(description="Batch calculate VaR for all symbols in CSV")
    parser.add_argument("--csv", type=str, default="full_market_history_raw.csv",
                        help="Input CSV file path")
    parser.add_argument("--years", type=str, default="1,2,3",
                        help="Comma-separated years of lookback data, e.g. 1,2,3")
    parser.add_argument("--days", type=str, default="1,5,10",
                        help="Comma-separated VaR horizons (days), e.g. 1,5,10")
    parser.add_argument("--confidence", type=float, default=0.99,
                        help="Confidence level (default 0.99)")
    parser.add_argument("--output_dir", type=str, default="var_results",
                        help="Folder to save output CSVs")
    parser.add_argument("--end_date", type=str, default=None,
                        help="Optional calculation date YYYY-MM-DD (default = latest per symbol)")

    args = parser.parse_args()

    # Parse lists
    years_list = [int(y.strip()) for y in args.years.split(",")]
    days_list = [int(d.strip()) for d in args.days.split(",")]

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Reading data from {args.csv} ...")
    df = pd.read_csv(args.csv, parse_dates=['Date'])

    symbols = sorted(df['Symbol'].unique())
    print(f"Found {len(symbols)} unique symbols")

    results = []

    for symbol in tqdm(symbols, desc="Processing symbols"):
        df_sym = df[df['Symbol'] == symbol].copy()

        for y in years_list:
            for d in days_list:
                try:
                    var_value = calculate_multi_day_var(
                        df_sym,
                        var_days=d,
                        data_years=y,
                        confidence=args.confidence,
                        end_date=args.end_date
                    )

                    if var_value is not None:
                        results.append({
                            'Symbol': symbol,
                            'Data_Years': y,
                            'VaR_Days': d,
                            'Confidence': f"{args.confidence:.0%}",
                            'VaR_%': round(var_value, 4),
                            'Calc_Date': args.end_date if args.end_date else "latest",
                            'Data_Points_Used': len(df_sym)  # rough info
                        })
                    else:
                        print(f"  Skipped {symbol} | {y}y | {d}d → insufficient data")

                except Exception as e:
                    print(f"  Error on {symbol} | {y}y | {d}d : {e}")

    if not results:
        print("No valid VaR calculations were produced.")
        return

    # Save one big combined file
    result_df = pd.DataFrame(results)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_file = os.path.join(
        args.output_dir,
        f"VaR_all_symbols_{args.confidence:.0%}_{timestamp}.csv"
    )
    result_df.to_csv(combined_file, index=False)
    print(f"\nSaved combined results: {combined_file}")

    # Optional: one file per (years, days) combination
    for y in years_list:
        for d in days_list:
            subset = result_df[(result_df['Data_Years'] == y) & (result_df['VaR_Days'] == d)]
            if not subset.empty:
                fname = os.path.join(
                    args.output_dir,
                    f"VaR_{y}years_{d}days_{args.confidence:.0%}_{timestamp}.csv"
                )
                subset.to_csv(fname, index=False)
                print(f"  → {fname} ({len(subset)} symbols)")

    print("\nDone.")


if __name__ == "__main__":
    main()