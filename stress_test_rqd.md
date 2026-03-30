Here is the comprehensive technical specification for your custom **Strict Portfolio Stress Test Engine**, based on the RQD House Margin methodology but modified for a conservative, long-only US equity framework.

---

# Technical Specification: Strict Portfolio Stress Test Engine

## 1. Overview
This document outlines the logic for a conservative risk-based margin engine. It adapts the **RQD Core Risk Checks** by removing gains-based offsets and volatility shocks, focusing purely on price-action tail risks for long-only stock portfolios.

## 2. Data Requirements
### 2.1 Historical Price Data
* **Universe:** All US stocks currently held in portfolios (`股票代碼`).
* **Look-back 1:** 3 years of daily adjusted closing prices (for Standard Deviation checks).
* **Look-back 2:** 2 years of daily adjusted closing prices (for CVaR check).

### 2.2 Portfolio Input Fields
From the current portfolio format:
* `股票代碼` (Ticker)
* `股票市值` (Market Value)
* `含貸權益值` (Total Equity/NAV)

---

## 3. Pre-Calculation Engine (Risk Parameter Table)
For every stock, pre-calculate the following percentage drops daily. **No math proxies ($\sqrt{n}$) are used; all calculations utilize real rolling returns.**

| Parameter | Calculation Method | Historical Window |
| :--- | :--- | :--- |
| **S1_1d** | $-1 \times \text{Std Dev}$ of 1-day returns | 3 Years |
| **S2_1d** | $-2 \times \text{Std Dev}$ of 1-day returns | 3 Years |
| **S3_1d** | $-3 \times \text{Std Dev}$ of 1-day returns | 3 Years |
| **S3_2d** | $-3 \times \text{Std Dev}$ of **2-day rolling returns** | 3 Years |
| **S3_3d** | $-3 \times \text{Std Dev}$ of **3-day rolling returns** | 3 Years |
| **S3_5d** | $-3 \times \text{Std Dev}$ of **5-day rolling returns** | 3 Years |
| **S3_10d** | $-3 \times \text{Std Dev}$ of **10-day rolling returns** | 3 Years |
| **CVaR_99** | Average of the worst 1% of 1-day returns (Expected Shortfall)  | 2 Years |

---

## 4. Core Risk Checks (The Stress Test)
The engine calculates four distinct risk values for each `證券賬戶` (Account).

### 4.1 Check #1: Macro Risk (Strict)
Test the portfolio against 6 different price shock scenarios. 
* **Scenarios:** Apply `S1_1d`, `S2_1d`, `S3_1d`, `S3_2d`, `S3_3d`, and `S3_5d`.
* **Calculation:** $\sum (\text{股票市值} \times \text{Scenario \% Drop})$.
* **Strict Rule:** No offsets allowed. Gains in one stock cannot reduce losses in another.
* **Requirement:** The highest loss among these 6 scenarios.

### 4.2 Check #2: Concentration 1 (Flat Shock)
* **Calculation:** Apply a flat **-35%** drop to every position.
* **Requirement:** Sum of the **top 5 largest losses**.

### 4.3 Check #3: Concentration 2 (10-Day Stress)
* **Calculation:** Apply the pre-calculated **S3_10d** (-3STD over 10 days) to every position.
* **Requirement:** Sum of the **top 3 largest losses**.

### 4.4 Check #4: Conditional Value at Risk (CVaR)
* **Calculation:** Apply the pre-calculated **CVaR_99** percentage to every position.
* **Requirement:** Sum of **all** resulting losses.

---

## 5. Final Margin Determination
The **House Margin Requirement** is the maximum of the four checks above:

$$\text{House Margin} = \max(\text{Macro Risk}, \text{Concentration 1}, \text{Concentration 2}, \text{CVaR})$$ 
