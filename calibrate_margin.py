import pandas as pd
import numpy as np
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import argparse

print("pandas version:", pd.__version__)

def find_latest_var_file(directory=".", prefix="VaR_all_symbols_95%"):
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No combined VaR file found in {os.path.abspath(directory)}")
    latest = max(files, key=lambda x: x.split('_')[-2] + x.split('_')[-1].replace('.csv', ''))
    return os.path.join(directory, latest)

def main():
    parser = argparse.ArgumentParser(description="Calibrate margin ratios using pre-computed VaR (long-format CSV)")
    parser.add_argument("--var_dir", type=str, default="var_results", help="Folder with VaR CSV files")
    parser.add_argument("--margin_file", type=str, default="long_margin_analysis.csv", help="Path to long_margin_analysis.csv")
    parser.add_argument("--output_dir", type=str, default="../margin_results", help="Where to save output")
    parser.add_argument("--output_name", type=str, default=None, help="Custom output filename")
    
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # 1. Load margin reference (Futu stocks)
    df_margin = pd.read_csv(args.margin_file)
    print(f"Loaded {len(df_margin)} stocks from {args.margin_file}")
    print("Columns:", df_margin.columns.tolist())

    # Standardize ticker
    df_margin = df_margin.rename(columns={'ticker_LB': 'Symbol'})

    # 2. Load combined VaR (long format)
    var_file = find_latest_var_file(args.var_dir)
    print(f"Using VaR file: {var_file}")
    df_var_long = pd.read_csv(var_file)
    print(f"VaR data shape: {df_var_long.shape}, symbols: {df_var_long['Symbol'].nunique()}")

    # Pivot to wide format
    df_var = df_var_long.pivot_table(
        index='Symbol',
        columns=['Data_Years', 'VaR_Days'],
        values='VaR_%',
        aggfunc='first'
    ).reset_index()

    # Flatten MultiIndex columns
    new_cols = []
    for col_tuple in df_var.columns:
        if col_tuple[0] == 'Symbol':
            new_cols.append('Symbol')
        else:
            years_str, days_str = col_tuple
            try:
                y = int(float(years_str))
                d = int(float(days_str))
                new_cols.append(f"VaR_{y}_{d}")
            except (ValueError, TypeError):
                new_cols.append(f"Unknown_{years_str}_{days_str}")

    df_var.columns = new_cols

    var_cols_raw = [c for c in df_var.columns if c.startswith('VaR_')]
    print("Created VaR columns:", var_cols_raw)
    print(f"Number of VaR variants: {len(var_cols_raw)}")
    print("First few rows after pivot:")
    print(df_var.head(3))

    # 4. Merge
    df = pd.merge(
        df_margin[['Symbol', 'IM_long', 'log_adt', 'log_mcap']],
        df_var,
        on='Symbol',
        how='left'
    )
    print(f"After merge: {len(df)} rows ({df['Symbol'].nunique()} unique symbols)")

    # 5. Create transformed versions for each raw VaR column
    transformed = {}
    for col in var_cols_raw:
        # Avoid log(0) or negative → use np.log1p (log(1+x))
        transformed[f"log_{col}"]  = np.log1p(df[col].clip(lower=0))
        transformed[f"sqrt_{col}"] = np.sqrt(df[col].clip(lower=0))

    # Add them to dataframe
    for name, series in transformed.items():
        df[name] = series

    # All candidate features (raw + log + sqrt)
    all_candidates = var_cols_raw + list(transformed.keys())

    # 6. Correlation ranking — all variants
    corrs = {}
    for col in all_candidates:
        valid = df[[col, 'IM_long']].dropna()
        n = len(valid)
        if n >= 10:
            corrs[col] = valid[col].corr(valid['IM_long'])
        else:
            corrs[col] = np.nan
        print(f"  {col:20}  corr={corrs[col]:6.3f}  (n={n})")

    if not any(not np.isnan(v) for v in corrs.values()):
        raise ValueError("No variant has enough overlapping data with IM_long")

    # Select best (highest absolute correlation, but since VaR is positive → higher is better)
    best_col = max(corrs, key=lambda k: corrs[k] if not np.isnan(corrs[k]) else -np.inf)
    best_r = corrs[best_col]
    print(f"\nBest variant overall: {best_col} (corr = {best_r:.3f})")

    # 7. Fit model using the best one
    features = [best_col, 'log_adt', 'log_mcap']
    df_fit = df[features + ['IM_long']].dropna()

    print("\nMissing values summary (for best variant):")
    print(df[features + ['IM_long']].isna().sum())
    print(f"Complete rows for fitting: {len(df_fit)}")

    if len(df_fit) < 10:
        print("Too few complete rows → fallback to simple scaling")
        scale = df['IM_long'].mean() / df[best_col].mean()
        df['Pred_IM_long'] = (df[best_col] * scale).clip(10, 100).round(0).astype('Int64')
    else:
        X = df_fit[features]
        y = df_fit['IM_long']
        model = LinearRegression()
        model.fit(X, y)
        
        preds_train = model.predict(X)
        print(f"  R² (on training data) = {r2_score(y, preds_train):.3f}")
        print("  Coefficients:")
        for f, coef in zip(features, model.coef_):
            print(f"    {f:18} : {coef:8.4f}")
        print(f"  Intercept       : {model.intercept_:8.2f}")

        # Predict only on complete rows
        df['Pred_IM_long'] = pd.NA
        mask = df[features].notna().all(axis=1)

        if mask.any():
            preds_np = model.predict(df.loc[mask, features])
            df.loc[mask, 'Pred_IM_long'] = (
                pd.Series(preds_np, index=df.loc[mask].index)
                .clip(10, 100)
                .round(0)
                .astype('Int64')
            )
            print(f"Predicted {mask.sum()} rows (only those with no missing features)")
        else:
            print("No rows available for prediction")

    # 8. Diagnostics
    df['Diff']     = (df['Pred_IM_long'] - df['IM_long']).fillna(0).astype('int32')
    df['Abs_Diff'] = df['Diff'].abs()

    # 9. Save
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
    suffix = best_col.replace(' ', '_').replace('%', 'pct')
    out_path = os.path.join(
        args.output_dir,
        args.output_name or f"margin_calibrated_{suffix}_{timestamp}.csv"
    )
    
    df.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")
    print(f"Predicted values: {df['Pred_IM_long'].notna().sum()}/{len(df)}")

    print("\nPrediction summary:")
    print(df['Pred_IM_long'].describe().round(1))
    print(f"Mean |diff| vs Futu IM_long: {df['Abs_Diff'].mean():.1f}")

if __name__ == "__main__":
    main()