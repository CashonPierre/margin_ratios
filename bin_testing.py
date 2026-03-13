import pandas as pd
import numpy as np
import os
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import cross_val_score, KFold
from itertools import combinations

def find_latest_var_file(directory=".", prefix="VaR_all_symbols_95%"):
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("No VaR file found")
    latest = max(files, key=lambda x: x.split('_')[-2] + x.split('_')[-1].replace('.csv', ''))
    return os.path.join(directory, latest)

def evaluate_custom_edges(df, feature, edges_original, others):
    """
    Evaluate binning with given original-scale edges.
    Returns CV MAE.
    """
    df_temp = df.copy().dropna(subset=['IM_long', feature] + others)
    
    # VaR_3_1 to VaR_3_10 log testing 2/2 4:46
    # Convert original edges to log scale
    if feature == 'log_VaR_3_10':
        edges_log = [-np.inf] + [np.log1p(x) for x in edges_original] + [np.inf]
    elif feature == 'log_adt':
        edges_log = [-np.inf] + [np.log(x * 1e6) for x in edges_original] + [np.inf]  # M → USD
    else:  # log_mcap
        edges_log = [-np.inf] + [np.log(x * 1e9) for x in edges_original] + [np.inf]   # B → USD
    
    # Cut with duplicates='drop' to avoid error
    df_temp['bin'] = pd.cut(
        df_temp[feature],
        bins=edges_log,
        labels=False,
        include_lowest=True,
        duplicates='drop'
    )
    
    dummies = pd.get_dummies(df_temp['bin'], prefix=f'{feature}_bin')
    X = pd.concat([dummies, df_temp[others]], axis=1)
    y = df_temp['IM_long']
    
    cv = KFold(5, shuffle=True, random_state=42)
    model = LinearRegression()
    mae_scores = -cross_val_score(model, X, y, cv=cv, scoring='neg_mean_absolute_error')
    
    return mae_scores.mean(), mae_scores.std(), len(dummies.columns)

def grid_search_nice_candidates(df, feature, candidates_orig):
    """
    Grid search by choosing subsets of 4 to 10 breakpoints from nice candidates.
    """
    others = [f for f in ['log_VaR_3_10', 'log_adt', 'log_mcap'] if f != feature]
    
    results = []
    min_bins = 4
    max_bins = min(10, len(candidates_orig))
    
    print(f"\nGrid search for {feature} — choosing {min_bins} to {max_bins} breakpoints from {len(candidates_orig)} candidates")
    
    for k in range(min_bins, max_bins + 1):
        for comb in combinations(sorted(candidates_orig), k):
            edges = list(comb)
            mae_mean, mae_std, n_bins = evaluate_custom_edges(df, feature, edges, others)
            
            results.append({
                'feature': feature,
                'n_bins': n_bins,
                'edges_original': edges,
                'cv_mae_mean': mae_mean,
                'cv_mae_std': mae_std
            })
            
            print(f"  {feature} | {n_bins} bins | edges={edges} → CV MAE {mae_mean:.3f}")
    
    return pd.DataFrame(results).sort_values('cv_mae_mean')

def main():
    # Load data (same as before)
    df_margin = pd.read_csv("long_margin_analysis.csv")
    df_margin = df_margin.rename(columns={'ticker_LB': 'Symbol'})

    var_file = find_latest_var_file("var_results")
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

    df['log_VaR_3_10'] = np.log1p(df['VaR_3_10'].clip(lower=0))

    # Nice round candidates from your output (remove 0 and huge outliers if needed)
    VAR_CANDIDATES  = [1, 2, 3, 4, 5, 6, 7, 8, 10, 13, 15]     # % 
    ADT_CANDIDATES  = [1, 5, 10, 20, 50, 100, 200, 400]         # M
    MCAP_CANDIDATES = [0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500] # B

    # Run grid search for each feature (can be slow if many candidates — limit k if needed)
    print("\nGrid search using nice round breakpoints as candidates...")
    all_grids = {}

    all_grids['log_VaR_3_10'] = grid_search_nice_candidates(df, 'log_VaR_3_10', VAR_CANDIDATES)
    all_grids['log_adt']     = grid_search_nice_candidates(df, 'log_adt', ADT_CANDIDATES)
    all_grids['log_mcap']    = grid_search_nice_candidates(df, 'log_mcap', MCAP_CANDIDATES)

    # Show best for each
    for feat, res_df in all_grids.items():
        if not res_df.empty:
            best = res_df.iloc[0]
            print(f"\nBest for {feat}:")
            print(f"  {best['n_bins']} bins | CV MAE {best['cv_mae_mean']:.3f}")
            print(f"  Edges (original): {best['edges_original']}")

    # Save all
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
    for feat, df_res in all_grids.items():
        df_res.to_csv(f"grid_{feat}_{timestamp}.csv", index=False)
        print(f"Saved {feat} grid to grid_{feat}_{timestamp}.csv")

if __name__ == "__main__":
    main()