import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score

# ==========================================
# 1. CONFIGURATION & LOADING
# ==========================================
ANCHOR_DATE = pd.Timestamp("2026-01-20")

# Load Datasets
df_target = pd.read_csv('futu_margin_ratios_all_target.csv')
df_history = pd.read_csv('full_market_history_raw.csv')
df_shares = pd.read_csv('circulating_shares_report.csv')
df_map = pd.read_csv('LB_futu_mapping.csv')

# Load Basic Info & Supplement
df_basic_main = pd.read_csv('futu_us_stock_basic_info.csv')
df_basic_supp = pd.read_csv('stock_static_info_supp.csv')

# --- MERGE STATIC INFO ---
print(f"Basic Info Main: {len(df_basic_main)} rows")
print(f"Basic Info Supp: {len(df_basic_supp)} rows")

# Combine and drop duplicates (Prioritizing the Main file)
df_basic = pd.concat([df_basic_main, df_basic_supp], ignore_index=True)
df_basic = df_basic.drop_duplicates(subset='code', keep='first')

print(f"Combined Basic Info: {len(df_basic)} unique stocks")
print("-" * 30)

# ==========================================
# 2. THE BRIDGE (MAPPING STRATEGY)
# ==========================================
# Create a dictionary for fast lookup: LB_ticker -> futu_ticker
lb_to_futu_dict = dict(zip(df_map['LB_ticker'], df_map['futu_ticker']))

def get_futu_key(lb_symbol):
    if lb_symbol in lb_to_futu_dict:
        return lb_to_futu_dict[lb_symbol]
    if str(lb_symbol).endswith('.US'):
        clean_sym = str(lb_symbol).replace('.US', '')
        return f"US.{clean_sym}"
    return f"US.{lb_symbol}"

print("Bridging LB data to Futu keys...")
df_history['futu_key'] = df_history['Symbol'].apply(get_futu_key)
df_shares['futu_key'] = df_shares['Symbol'].apply(get_futu_key)

# ==========================================
# 3. MASTER TABLE & HARD FILTERS
# ==========================================
# Merge Target with the Combined Basic Info
df_master = pd.merge(df_target, df_basic, on='code', how='left')

# --- Filter 1: Pink Sheets ---
# Handling N/A in exchange_type by treating them as 'Unknown' (safe path) or filling
df_master['exchange_type'] = df_master['exchange_type'].fillna('Unknown')
pink_exchanges = ['US_PINK', 'PINK', 'OTC'] 
df_master['is_pink'] = df_master['exchange_type'].isin(pink_exchanges)

# --- Filter 2: New Listings (< 3 Months) ---
df_master['listing_date'] = pd.to_datetime(df_master['listing_date'], errors='coerce')
df_master['days_listed'] = (ANCHOR_DATE - df_master['listing_date']).dt.days

# Handle missing listing dates:
# If listing date is missing, we usually assume it's NOT new (safest for established stocks), 
# or strictly filter it out. Here we assume False (not new) if unknown, unless you prefer strictness.
df_master['is_new'] = df_master['days_listed'] < 90
df_master['is_new'] = df_master['is_new'].fillna(False) 

# Create Training Set (Eligible Only)
# We exclude Pink and New stocks from the Logistic Regression training
train_mask = (~df_master['is_pink']) & (~df_master['is_new'])
df_train = df_master[train_mask].copy()

print(f"Total Targets: {len(df_master)}")
print(f"Eligible for Model: {len(df_train)} (Excluded {len(df_master) - len(df_train)} via Hard Rules)")

# ==========================================
# 4. FEATURE ENGINEERING
# ==========================================

# --- Feature A: Log(30-Day Avg Turnover) ---
# 1. Filter History by Date
df_hist_filt = df_history[pd.to_datetime(df_history['Date']) < ANCHOR_DATE].copy()

# 2. Sort by Date Descending
df_hist_filt = df_hist_filt.sort_values(['futu_key', 'Date'], ascending=[True, False])

# 3. Take Top 30 per stock
df_hist_top30 = df_hist_filt.groupby('futu_key').head(30)

# 4. Calculate Mean Turnover
turnover_feat = df_hist_top30.groupby('futu_key')['Turnover'].mean().reset_index()
turnover_feat.rename(columns={'Turnover': 'avg_turnover_30d'}, inplace=True)

# --- Feature B: Log(Market Cap) ---
# 1. Get Latest Price (from history, just the top 1 row)
latest_price = df_hist_filt.groupby('futu_key').head(1)[['futu_key', 'Close']].rename(columns={'Close': 'latest_close'})

# 2. Get Shares (from shares report)
shares_feat = df_shares[['futu_key', 'Total Shares']].copy()

# 3. Calculate Cap
cap_df = pd.merge(latest_price, shares_feat, on='futu_key', how='inner')
cap_df['market_cap'] = cap_df['latest_close'] * cap_df['Total Shares']

# --- Merge Features into Master Training Set ---
df_model = pd.merge(df_train, turnover_feat, left_on='code', right_on='futu_key', how='inner')
df_model = pd.merge(df_model, cap_df[['futu_key', 'market_cap']], on='futu_key', how='inner')

# --- Log Transformation ---
# Using log1p to avoid errors with 0
df_model['log_turnover'] = np.log1p(df_model['avg_turnover_30d'])
df_model['log_mkt_cap'] = np.log1p(df_model['market_cap'])

# ==========================================
# 5. LOGISTIC REGRESSION MODEL
# ==========================================
features = ['log_turnover', 'log_mkt_cap']
target = 'is_long_permit'

# Drop any remaining NAs
df_model_clean = df_model.dropna(subset=features + [target])

X = df_model_clean[features]
y = df_model_clean[target].astype(int) # Convert boolean to 0/1

# Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train
clf = LogisticRegression()
clf.fit(X_train, y_train)

# ==========================================
# 6. EVALUATION
# ==========================================
print("\n--- Model Coefficients ---")
print(f"Intercept: {clf.intercept_[0]:.4f}")
print(f"Coeff (Log Turnover): {clf.coef_[0][0]:.4f}")
print(f"Coeff (Log Mkt Cap): {clf.coef_[0][1]:.4f}")

y_pred = clf.predict(X_test)
y_prob = clf.predict_proba(X_test)[:, 1]

print("\n--- Performance Metrics ---")
print(classification_report(y_test, y_pred))
print(f"ROC-AUC Score: {roc_auc_score(y_test, y_prob):.4f}")

# ==========================================
# 7. DEEP DIVE ANALYSIS & INFERENCE REPORT
# ==========================================

# --- A. Calculate False Positive Rate (FPR) ---
tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
fpr = fp / (tn + fp)

print("\n--- Risk Metrics ---")
print(f"False Positive Rate (FPR): {fpr:.2%}")
print(f"Detailed Matrix: TN={tn}, FP={fp} (Risk!), FN={fn}, TP={tp}")
print("Interpretation: {:.2f}% of the restricted stocks were incorrectly flagged as 'Safe'.".format(fpr * 100))

# --- B. Generate Full Inference Sheet (All Eligible Data) ---

# We run prediction on the FULL eligible dataset (df_model_clean), not just the test set
# to generate the report for colleagues.
X_full = df_model_clean[features]
y_full_actual = df_model_clean[target]

# 1. Get Predictions & Probabilities
df_inference = df_model_clean.copy()
df_inference['p_allow_long'] = clf.predict_proba(X_full)[:, 1]
df_inference['pred_allow_long'] = clf.predict(X_full)

# 2. Convert Boolean to int for comparison
df_inference['is_long_permit'] = df_inference['is_long_permit'].astype(int)
df_inference['pred_allow_long'] = df_inference['pred_allow_long'].astype(int)

# 3. Create 'match' column
df_inference['match'] = df_inference['is_long_permit'] == df_inference['pred_allow_long']

# 4. Create 'confusion_matrix' categorical column
def label_confusion(row):
    actual = row['is_long_permit']
    pred = row['pred_allow_long']
    
    if actual == 1 and pred == 1:
        return 'TP (Valid Approval)'
    elif actual == 0 and pred == 1:
        return 'FP (Risky Approval)'
    elif actual == 1 and pred == 0:
        return 'FN (Missed Opp)'
    elif actual == 0 and pred == 0:
        return 'TN (Correct Ban)'
    return 'Error'

df_inference['confusion_category'] = df_inference.apply(label_confusion, axis=1)

# 5. Format Columns for Export
output_columns = [
    'code',               # Ticker
    'log_turnover',       # log_adt
    'log_mkt_cap',        # log_mcap
    'exchange_type',
    'is_long_permit',
    'pred_allow_long',
    'p_allow_long',
    'match',
    'confusion_category', # The readable TP/FP tag
    'listing_date'
]

# Rename columns to match your request exactly
rename_map = {
    'code': 'ticker',
    'log_turnover': 'log_adt',
    'log_mkt_cap': 'log_mcap',
    'confusion_category': 'confusion_matrix'
}

final_report = df_inference[output_columns].rename(columns=rename_map)

# --- C. Export ---
filename = 'margin_model_inference_report.csv'
final_report.to_csv(filename, index=False)
print(f"\nReport Generated: {filename}")
print(final_report[['ticker', 'confusion_matrix', 'p_allow_long']].head())

# output master sheet for reference
df_master.to_csv('futu_margin_ratios_master_sheet.csv', index=False)