import pandas as pd
import numpy as np
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_absolute_error

def main():
    # Load data
    df_margin = pd.read_csv("long_margin_analysis.csv")
    df_margin = df_margin.rename(columns={'ticker_LB': 'Symbol'})

    var_file = "var_results/VaR_all_symbols_95%_20260131_162751.csv"  # adjust if filename changed
    df_var_long = pd.read_csv(var_file)

    df_var = df_var_long.pivot_table(
        index='Symbol',
        columns=['Data_Years', 'VaR_Days'],
        values='VaR_%',
        aggfunc='first'
    ).reset_index()

    new_cols = ['Symbol' if c[0] == 'Symbol' else f"VaR_{int(float(c[0]))}_{int(float(c[1]))}" for c in df_var.columns]
    df_var.columns = new_cols

    df = pd.merge(
        df_margin[['Symbol', 'IM_long', 'log_adt', 'log_mcap']],
        df_var,
        on='Symbol',
        how='left'
    )

    # VaR_3_1 to VaR_3_10 log testing 2/2 4:46
    df['log_VaR_3_10'] = np.log1p(df['VaR_3_10'].clip(lower=0))

    # Final fixed round buckets from grid search
    #Var_3_1 edges (from previous testing)
    # VAR_EDGES   = [1, 2, 3, 4, 5, 6, 8, 13]          # %
    # ADT_EDGES   = [10, 20, 50, 100, 200, 400]        # M
    # MCAP_EDGES  = [0.5, 1, 5, 10, 20]                # B
    
    #VaR_3_10 edges
    VAR_EDGES   = [1, 4, 8, 10, 15]          # %
    ADT_EDGES   = [10, 20, 50, 100, 200, 400]        # M
    MCAP_EDGES  = [0.5, 1, 5, 10, 20]                # B

    # Bin creation - use raw values (inverse log where needed)
    df['VaR_bin'] = pd.cut(
        df['VaR_3_10'],
        bins=[0] + VAR_EDGES + [np.inf],
        labels=[f"VaR_{i}" for i in range(len(VAR_EDGES)+1)],
        include_lowest=True,
        right=False
    )

    df['ADT_bin'] = pd.cut(
        np.exp(df['log_adt']) / 1e6,   # million USD
        bins=[0] + ADT_EDGES + [np.inf],
        labels=[f"ADT_{i}" for i in range(len(ADT_EDGES)+1)],
        include_lowest=True,
        right=False
    )

    df['Mcap_bin'] = pd.cut(
        np.exp(df['log_mcap']) / 1e9,  # billion USD
        bins=[0] + MCAP_EDGES + [np.inf],
        labels=[f"Mcap_{i}" for i in range(len(MCAP_EDGES)+1)],
        include_lowest=True,
        right=False
    )

    # One-hot encode
    dummies = pd.get_dummies(df[['VaR_bin', 'ADT_bin', 'Mcap_bin']], prefix='', prefix_sep='')
    X = dummies.astype(float)  # ensure numeric
    y = df['IM_long']

    # Fit
    mask = X.notna().all(axis=1) & y.notna()
    model = LinearRegression().fit(X.loc[mask], y.loc[mask])

    preds = model.predict(X.loc[mask])
    train_r2 = r2_score(y.loc[mask], preds)
    train_mae = mean_absolute_error(y.loc[mask], preds)

    print(f"\nFinal model with round buckets:")
    print(f"  Training R²: {train_r2:.3f}")
    print(f"  Training MAE: {train_mae:.3f}")

    # Predict all
    df['Pred_IM_long'] = pd.NA
    df.loc[mask, 'Pred_IM_long'] = (
        pd.Series(preds, index=df.loc[mask].index)
        .clip(10, 100)
        .round(0)
        .astype('Int64')
    )

    df['Diff']     = (df['Pred_IM_long'] - df['IM_long']).fillna(0).astype('int32')
    df['Abs_Diff'] = df['Diff'].abs()

    # Save
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
    out_path = f"margin_final_round_buckets_{timestamp}.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")
    print(f"Mean |diff| vs Futu: {df['Abs_Diff'].mean():.1f}")
    print("\nPrediction summary:")
    print(df['Pred_IM_long'].describe().round(1))

    # Bin counts for sanity check
    print("\nBin distribution:")
    print("VaR bins:\n", df['VaR_bin'].value_counts().sort_index())
    print("\nADT bins:\n", df['ADT_bin'].value_counts().sort_index())
    print("\nMcap bins:\n", df['Mcap_bin'].value_counts().sort_index())

    # After model = LinearRegression().fit(...)
    print("\n=== Model Coefficients (for presentation / lookup table) ===")

    # Create a nice Series with bin names
    coef_series = pd.Series(model.coef_, index=X.columns)
    coef_series = coef_series.sort_values(ascending=False)

    print(coef_series.round(4))

    # Also print intercept
    print(f"\nIntercept: {model.intercept_:.4f}")

    # Optional: save to CSV for Excel
    coef_df = pd.DataFrame({
        'Bin': coef_series.index,
        'Coefficient': coef_series.values.round(4)
    })
    coef_df.to_csv("margin_coefficients.csv", index=False)
    print("Coefficients saved to margin_coefficients.csv")

if __name__ == "__main__":
    main()