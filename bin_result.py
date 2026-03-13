import pandas as pd
import numpy as np
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import cross_val_score, KFold
import argparse

print("pandas version:", pd.__version__)

def find_latest_var_file(directory=".", prefix="VaR_all_symbols_95%"):
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No combined VaR file found in {os.path.abspath(directory)}")
    latest = max(files, key=lambda x: x.split('_')[-2] + x.split('_')[-1].replace('.csv', ''))
    return os.path.join(directory, latest)

def evaluate_binning(df, feature, bin_type, n_bins, cv=KFold(n_splits=5, shuffle=True, random_state=42)):
    """
    Evaluate binning for a single feature + continuous others using CV MAE.
    If n_bins=1, treat as continuous (no binning).
    """
    df_temp = df.copy().dropna(subset=['IM_long', 'log_VaR_3_1', 'log_adt', 'log_mcap'])
    
    # Other features (continuous)
    other_features = [f for f in ['log_VaR_3_1', 'log_adt', 'log_mcap'] if f != feature]
    
    if n_bins == 1:
        # Continuous: no binning
        X = df_temp[[feature] + other_features]
    else:
        # Bin the feature
        if bin_type == 'quantile':
            df_temp['bin'] = pd.qcut(df_temp[feature], q=n_bins, duplicates='drop', labels=False)
        elif bin_type == 'equal_width':
            df_temp['bin'] = pd.cut(df_temp[feature], bins=n_bins, duplicates='drop', labels=False)
        else:
            raise ValueError(f"Unknown bin_type: {bin_type}")
        
        dummies = pd.get_dummies(df_temp['bin'], prefix=f'{feature}_bin')
        X = pd.concat([dummies, df_temp[other_features]], axis=1)
    
    y = df_temp['IM_long']
    model = LinearRegression()
    mae_scores = -cross_val_score(model, X, y, cv=cv, scoring='neg_mean_absolute_error')
    r2_scores = cross_val_score(model, X, y, cv=cv, scoring='r2')
    
    return {
        'feature': feature,
        'bin_type': bin_type,
        'n_bins': n_bins,
        'cv_mae_mean': mae_scores.mean(),
        'cv_mae_std': mae_scores.std(),
        'cv_r2_mean': r2_scores.mean(),
        'cv_r2_std': r2_scores.std(),
        'effective_bins': 1 if n_bins == 1 else dummies.shape[1]
    }

def main():
    parser = argparse.ArgumentParser(description="Calibrate margin with optimal binning per feature")
    parser.add_argument("--var_dir", type=str, default="var_results")
    parser.add_argument("--margin_file", type=str, default="long_margin_analysis.csv")
    parser.add_argument("--output_dir", type=str, default="../margin_results")
    parser.add_argument("--output_name", type=str, default=None)
    
    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    # ── Load & prepare data (same as before) ─────────────────────────────────
    df_margin = pd.read_csv(args.margin_file)
    df_margin = df_margin.rename(columns={'ticker_LB': 'Symbol'})
    print(f"Loaded {len(df_margin)} stocks")

    var_file = find_latest_var_file(args.var_dir)
    df_var_long = pd.read_csv(var_file)

    df_var = df_var_long.pivot_table(
        index='Symbol',
        columns=['Data_Years', 'VaR_Days'],
        values='VaR_%',
        aggfunc='first'
    ).reset_index()

    new_cols = []
    for col_tuple in df_var.columns:
        if col_tuple[0] == 'Symbol':
            new_cols.append('Symbol')
        else:
            y_str, d_str = col_tuple
            try:
                y = int(float(y_str))
                d = int(float(d_str))
                new_cols.append(f"VaR_{y}_{d}")
            except:
                new_cols.append(f"Unknown_{y_str}_{d_str}")
    df_var.columns = new_cols

    df = pd.merge(
        df_margin[['Symbol', 'IM_long', 'log_adt', 'log_mcap']],
        df_var,
        on='Symbol',
        how='left'
    )

    # Add log_VaR_3_1 (best from previous)
    df['log_VaR_3_1'] = np.log1p(df['VaR_3_1'].clip(lower=0))

    # ── Binning evaluation for each feature ──────────────────────────────────
    print("\nEvaluating binning configurations (5-fold CV MAE & R²)...")
    features_to_test = ['log_VaR_3_1', 'log_adt', 'log_mcap']
    bin_types = ['quantile', 'equal_width']
    bin_options = [1, 3, 4, 5, 6, 8, 10]  # 1 = continuous

    eval_results = []
    for feat in features_to_test:
        for btype in bin_types:
            for nb in bin_options:
                res = evaluate_binning(df, feat, btype, nb)
                eval_results.append(res)
                print(f"Tested {feat} | {btype} | {nb} bins → CV MAE: {res['cv_mae_mean']:.3f} (R²: {res['cv_r2_mean']:.3f})")

    eval_df = pd.DataFrame(eval_results)
    print("\nFull evaluation:")
    print(eval_df.sort_values('cv_mae_mean').to_string(index=False))

    # ── Select best config per feature (lowest CV MAE) ───────────────────────
    best_configs = {}
    for feat in features_to_test:
        feat_df = eval_df[eval_df['feature'] == feat]
        best_idx = feat_df['cv_mae_mean'].idxmin()
        best_configs[feat] = feat_df.loc[best_idx].to_dict()

    print("\nBest configs per feature:")
    for feat, config in best_configs.items():
        print(f"{feat}: {config['bin_type']} with {config['n_bins']} bins (CV MAE: {config['cv_mae_mean']:.3f})")

    # ── Build final X with best binning per feature ──────────────────────────
    df_final = df.copy()
    X_parts = []
    for feat, config in best_configs.items():
        n_bins = int(config['n_bins'])
        btype = config['bin_type']
        
        if n_bins == 1:
            # Continuous
            X_parts.append(df_final[[feat]])
        else:
            # Bin and dummy
            if btype == 'quantile':
                df_final[f'{feat}_bin'] = pd.qcut(df_final[feat], q=n_bins, duplicates='drop', labels=False)
            elif btype == 'equal_width':
                df_final[f'{feat}_bin'] = pd.cut(df_final[feat], bins=n_bins, duplicates='drop', labels=False)
            
            dummies = pd.get_dummies(df_final[f'{feat}_bin'], prefix=f'{feat}_bin')
            X_parts.append(dummies)

    X = pd.concat(X_parts, axis=1)
    y = df_final['IM_long']

    # Fit final model
    mask_fit = X.notna().all(axis=1) & y.notna()
    X_fit = X.loc[mask_fit]
    y_fit = y.loc[mask_fit]

    model = LinearRegression()
    model.fit(X_fit, y_fit)

    train_r2 = r2_score(y_fit, model.predict(X_fit))
    train_mae = mean_absolute_error(y_fit, model.predict(X_fit))
    print(f"\nFinal model (training): R²={train_r2:.3f}, MAE={train_mae:.3f}")
    print("Coefficients:")
    for name, coef in zip(X.columns, model.coef_):
        print(f"  {name:20} : {coef:8.4f}")
    print(f"Intercept: {model.intercept_:.2f}")

    # Predict
    df['Pred_IM_long'] = pd.NA
    mask_pred = X.notna().all(axis=1)
    if mask_pred.any():
        preds_np = model.predict(X.loc[mask_pred])
        df.loc[mask_pred, 'Pred_IM_long'] = (
            pd.Series(preds_np, index=df.loc[mask_pred].index)
            .clip(10, 100)
            .round(0)
            .astype('Int64')
        )
        print(f"Predicted {mask_pred.sum()} rows")

    # Diagnostics & save
    df['Diff']     = (df['Pred_IM_long'] - df['IM_long']).fillna(0).astype('int32')
    df['Abs_Diff'] = df['Diff'].abs()

    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
    out_path = os.path.join(
        args.output_dir,
        args.output_name or f"margin_optimal_binned_{timestamp}.csv"
    )
    df.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")
    print(f"Final mean |diff|: {df['Abs_Diff'].mean():.1f}")
    print(df['Pred_IM_long'].describe().round(1))

if __name__ == "__main__":
    main()