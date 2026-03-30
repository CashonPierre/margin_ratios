import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import traceback

# Suppress pandas 3.0+ FutureWarnings regarding pct_change
warnings.filterwarnings('ignore', category=FutureWarning)

def format_ticker(raw_ticker):
    """Extracts US symbol from internal format and handles BRK.B formatting."""
    if pd.isna(raw_ticker): return None
    ticker = str(raw_ticker).split('/')[-1].strip()
    return ticker.replace('.', '-')

def calculate_cvar(series):
    """Calculates 99% Expected Shortfall (CVaR)."""
    clean_series = series.dropna()
    if clean_series.empty: return -1.0
    # Worst 1% of returns averaged [cite: 36]
    threshold = clean_series.quantile(0.01)
    return clean_series[clean_series <= threshold].mean()

def run_full_stress_test(csv_path):
    # --- 1. Load and Clean Portfolio Data ---
    print(f"Loading {csv_path}...")
    df_portfolio = pd.read_csv(csv_path)
    df_portfolio.columns = df_portfolio.columns.str.strip()
    
    mv_col = '股票市值='
    acc_col = '證券賬戶'
    
    if df_portfolio[mv_col].dtype == 'object':
        df_portfolio[mv_col] = df_portfolio[mv_col].astype(str).str.replace(',', '')
    df_portfolio[mv_col] = pd.to_numeric(df_portfolio[mv_col], errors='coerce').fillna(0.0)
    
    df_portfolio['clean_ticker'] = df_portfolio['股票代碼'].apply(format_ticker)
    unique_tickers = df_portfolio['clean_ticker'].unique().tolist()
    
    # --- 2. Fetch Historical Data (3 Years) ---
    print(f"Fetching history for {len(unique_tickers)} unique symbols...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3*365 + 15)
    
    data_raw = yf.download(unique_tickers, start=start_date, end=end_date, auto_adjust=True)
    
    if 'Close' in data_raw.columns:
        data = data_raw['Close']
    else:
        data = data_raw

    if isinstance(data, pd.Series):
        data = data.to_frame()
    
    data.columns = [str(c) if isinstance(c, tuple) else str(c) for c in data.columns]

    # --- 3. Pre-Calculation Engine ---
    print("Calculating Risk Parameters (Standard Deviations & CVaR)...")
    risk_params = pd.DataFrame(index=unique_tickers)
    three_year_ago = end_date - timedelta(days=3*365)
    two_year_ago = end_date - timedelta(days=2*365)

    for days in [1, 3, 5, 10]:
        # Real rolling n-day returns based on actual price history
        rolling_ret = data.pct_change(periods=days, fill_method=None).loc[three_year_ago:]
        std_dev = rolling_ret.std()
        
        if days == 10:
            risk_params['S3_10d_pct'] = -3.0 * std_dev
        else:
            for s in [1, 2, 3]:
                risk_params[f'S{s}_{days}d_pct'] = -float(s) * std_dev

    # 2-Year 99% CVaR [cite: 35, 36]
    risk_params['CVaR_99_pct'] = data.pct_change(fill_method=None).loc[two_year_ago:].apply(calculate_cvar)
    
    # Export intermediate Risk Parameters
    risk_params.to_csv('Risk_Parameters_Master.csv')
    print("[EXPORT] Risk_Parameters_Master.csv saved.")

    # --- 4. Position-Level & Grouped Analysis ---
    # Merge risk metrics back to the original portfolio
    df_main = df_portfolio.merge(risk_params, left_on='clean_ticker', right_index=True, how='left')
    
    param_pct_cols = risk_params.columns.tolist()
    for col in param_pct_cols:
        df_main[col] = pd.to_numeric(df_main[col], errors='coerce').fillna(-1.0)
        # Store individual dollar losses for transparency [cite: 27, 33, 35]
        loss_name = col.replace('_pct', '_Loss_Val')
        df_main[loss_name] = abs(df_main[mv_col] * df_main[col])

    # Flat 35% check loss [cite: 31]
    df_main['Conc_35pct_Loss_Val'] = abs(df_main[mv_col] * -0.35)

    # Export intermediate Position-level details
    df_main.to_csv('Position_Stress_Details.csv', index=False)
    print("[EXPORT] Position_Stress_Details.csv saved.")

    print("Stressing sub-portfolios...")
    account_results = []
    check_details = []
    loss_cols = [c for c in df_main.columns if '_Loss_Val' in c]
    macro_loss_cols = [c for c in loss_cols if 'S' in c and '10d' not in c]

    for account_id, group in df_main.groupby(acc_col):
        # Check 1: Macro Risk (Max of scenario sums - Strict: No Offset)
        macro_scenario_sums = {col: group[col].sum() for col in macro_loss_cols}
        m_losses = list(macro_scenario_sums.values())
        macro_risk = max(m_losses) if m_losses else 0
        worst_macro_col = max(macro_scenario_sums, key=macro_scenario_sums.get) if macro_scenario_sums else "N/A"

        # Check 2: Concentration 1 (Top 5 holdings at -35%)
        top5_35 = group.nlargest(5, 'Conc_35pct_Loss_Val')[['clean_ticker', 'Conc_35pct_Loss_Val']]
        conc_35 = top5_35['Conc_35pct_Loss_Val'].sum()

        # Check 3: Concentration 2 (Top 3 holdings at -3STD 10-day)
        top3_10d = group.nlargest(3, 'S3_10d_Loss_Val')[['clean_ticker', 'S3_10d_Loss_Val']]
        conc_10d = top3_10d['S3_10d_Loss_Val'].sum()

        # Check 4: CVaR (Total portfolio expected shortfall)
        cvar_total = group['CVaR_99_Loss_Val'].sum()

        # House Margin = Max of the four core checks
        house_margin = max(macro_risk, conc_35, conc_10d, cvar_total)
        names = ["Macro", "Conc_35%", "Conc_10d", "CVaR"]
        vals = [macro_risk, conc_35, conc_10d, cvar_total]
        dominant = names[np.argmax(vals)]

        account_results.append({
            "證券賬戶": account_id,
            "Total_Market_Value": group[mv_col].sum(),
            "House_Margin_Requirement": house_margin,
            "Margin_Ratio_%": (house_margin / group[mv_col].sum() * 100) if group[mv_col].sum() != 0 else 0,
            "Dominant_Risk_Check": dominant,
            "Macro_Risk_Val": macro_risk,
            "Conc_35%_Val": conc_35,
            "Conc_10d_Val": conc_10d,
            "CVaR_Val": cvar_total
        })

        # --- Build step-by-step detail row for this account ---
        detail = {"證券賬戶": account_id, "Total_Market_Value": group[mv_col].sum()}

        # Check 1: each macro scenario sum + which was worst
        for col, val in macro_scenario_sums.items():
            detail[f"C1_{col.replace('_Loss_Val', '')}"] = val
        detail["C1_Worst_Scenario"] = worst_macro_col.replace("_Loss_Val", "")
        detail["C1_Macro_Risk"] = macro_risk

        # Check 2: top-5 individual holdings
        for rank, (_, pos_row) in enumerate(top5_35.iterrows(), start=1):
            detail[f"C2_Top{rank}_Ticker"] = pos_row['clean_ticker']
            detail[f"C2_Top{rank}_Loss"]   = pos_row['Conc_35pct_Loss_Val']
        detail["C2_Conc35_Total"] = conc_35

        # Check 3: top-3 individual holdings
        for rank, (_, pos_row) in enumerate(top3_10d.iterrows(), start=1):
            detail[f"C3_Top{rank}_Ticker"] = pos_row['clean_ticker']
            detail[f"C3_Top{rank}_Loss"]   = pos_row['S3_10d_Loss_Val']
        detail["C3_Conc10d_Total"] = conc_10d

        # Check 4: CVaR
        detail["C4_CVaR_Total"] = cvar_total

        # Final
        detail["House_Margin_Requirement"] = house_margin
        detail["Dominant_Risk_Check"]      = dominant

        check_details.append(detail)

    # Export intermediate core-risk-check breakdown
    df_check_details = pd.DataFrame(check_details)
    df_check_details.to_csv('Core_Risk_Checks_Detail.csv', index=False)
    print("[EXPORT] Core_Risk_Checks_Detail.csv saved.")

    return pd.DataFrame(account_results)

# --- Main Entry Point ---
if __name__ == "__main__":
    FILENAME = 'Client_Position_Details_0327.csv'
    try:
        final_report = run_full_stress_test(FILENAME)
        
        print("\n" + "="*80)
        print(f"{'ACCOUNT STRESS TEST SUMMARY':^80}")
        print("="*80)
        pd.options.display.float_format = '{:,.2f}'.format
        print(final_report[['證券賬戶', 'Total_Market_Value', 'House_Margin_Requirement', 'Margin_Ratio_%', 'Dominant_Risk_Check']].to_string(index=False))
        
        output_name = f"Stress_Test_Result_{datetime.now().strftime('%Y%m%d')}.csv"
        final_report.to_csv(output_name, index=False)
        print(f"\n[SUCCESS] Final account report saved as: {output_name}")

    except Exception as e:
        print(f"\n[ERROR] Process failed: {e}")
        traceback.print_exc()