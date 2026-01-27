# Logistic Model Project: Margin Trading Permission (v2)

## 1. Project Objective
Predict `is_long_permit` (Target) using a hybrid model (Hard Filters + Logistic Regression).
**Anchor Date:** `2026-01-20` (Data cut-off).

---

## 2. Data Alignment Strategy (The "Bridge")
We have two data universes that need to talk to each other:
1.  **Futu Universe (Targets & Metadata):** Uses format `US.SYMBOL` (sometimes with internal IDs like `US.1061`).
2.  **LB Universe (History & Shares):** Uses format `SYMBOL.US` (standard tickers).

**The Solution:**
We will treat the **Futu Ticker** (e.g., `US.WMT`, `US.1061`) as the **Primary Key**.
We will map the LB data *to* this Primary Key using a tiered approach:
* **Tier 1 (Explicit Map):** Use `LB_futu_mapping.csv`.
* **Tier 2 (Algorithmic Fallback):** If not in the map, convert `XYZ.US` -> `US.XYZ` and hope for a match.

---

## 3. The Pipeline

### Step A: Hard Filters (The "No-Go" Zone)
**Source:** `futu_us_stock_basic_info.csv` merged with Target.
We immediately **exclude** stocks if:
1.  `exchange_type` is **Pink Sheet** (OTC).
2.  `listing_date` is within **90 days** of 2026-01-20.

### Step B: Feature Engineering (The "Gray" Zone)
For remaining stocks, we calculate:

1.  **Log(Turnover):**
    * Filter History (`Date < 2026-01-20`).
    * Match LB History to Futu Target using the Bridge.
    * Average the last 30 entries per stock.
    * Apply `log1p`.

2.  **Log(Market Cap):**
    * Get `Total Shares` from `circulating_shares_report.csv`.
    * Get `Close Price` from History (latest available).
    * Match both to Futu Target using the Bridge.
    * Calculate `Cap = Shares * Price`.
    * Apply `log1p`.

### Step C: Modeling
Train a Logistic Regression on the filtered dataset.