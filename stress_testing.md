# Margin Design Backtest Specification

## 1. Objective

To evaluate if the Initial Margin (IM), Maintenance Margin (MM), and Margin-Call Margin (MCM) ratios from the input file are robust enough to protect a broker from negative equity during historical and hypothetical "Black Swan" events.

## 2. Core Logic & Definitions

The script calculates the maximum survivable price drop for a given set of margin ratios and compares it to the actual worst-case price drop observed during a stress event window.

*   **Input Data:** The script loads `margin_result_0311.csv`. It requires the columns `symbol`, `IM (initial margin)`, `MM (maintenance margin)`, and `MCM (margin-call margin)`. These are renamed internally to `Ticker`, `IM_raw`, `MM_raw`, and `MCM_raw`. Percentage strings (e.g., '55%') are converted to floats (e.g., 0.55).

*   **Trigger Ratios:** For each ticker, the script calculates price-drop thresholds. A position is considered breached if the stock price (`P_current`) falls below a certain multiple of its entry price (`P_entry`). The script calculates the critical price ratio (`P_current / P_entry`) for each threshold:
    *   `im_trigger_ratio = (1 - IM) / (1 - IM)` (Evaluates to 1.0; any drop breaches this)
    *   `mc_trigger_ratio = (1 - IM) / (1 - MCM)`
    *   `mm_trigger_ratio = (1 - IM) / (1 - MM)`
    *   `liq_trigger_ratio = (1 - IM) / (1 - (MM - 0.10))`
    *   `insolvency_ratio = (1 - IM)`

## 3. Breach Thresholds

The script checks if the worst observed price ratio (`worst_ratio`) during an event crosses these thresholds.

| Flag | Trigger Condition | Meaning |
| :--- | :--- | :--- |
| **IM_Breach**| `worst_ratio <= im_trigger_ratio`| The point where the position value drops below the initial margin requirement. |
| **MCM_Breach**| `worst_ratio <= mc_trigger_ratio`| The point where a Margin Call is issued. |
| **MM_Breach**| `worst_ratio <= mm_trigger_ratio`| The point where a standard Maintenance Margin breach occurs. |
| **LIQ** | `worst_ratio <= liq_trigger_ratio`| An internal, more aggressive threshold for forced liquidation (MM - 10%). |
| **FAIL** | `worst_ratio <= insolvency_ratio`| The point of zero equity. If the price drops below this, the account is negative. |

---

## 4. Methodology

For each ticker, the script performs the following steps:

1.  **Beta Calculation:** It calculates the ticker's `beta` relative to `SPY` over multiple periods: `1y`, `3y`, and `5y`. Beta measures volatility relative to the market and is used for proxy calculations. If the calculation fails, a default `beta` of `1.5` is assumed.

2.  **Event Analysis:** For each stress event and for each beta period, it determines the worst-case price drop.
    *   **Real Data Mode:** The script first attempts to download the ticker's actual price data for the event window. It finds the worst 3-day drop by calculating the minimum ratio of the low price two days in the future to the current day's close (`Low_T+2 / Close_T`). This minimum ratio becomes the `worst_ratio`.
    *   **Proxy (Sim) Mode:** If no historical data is available, it uses a market benchmark (`^GSPC` for events before 1993, `SPY` otherwise). It calculates the benchmark's `worst_ratio` and simulates the ticker's drop using its `beta` and a stress buffer:
        `sim_drop = (1 - bench_worst_ratio) * beta * 1.2`
        The ticker's `worst_ratio` is then `1 - sim_drop`.

3.  **Threshold Check:** The script compares the `worst_ratio` against the breach thresholds to set the `IM_Breach`, `MCM_Breach`, `MM_Breach`, `LIQ`, and `FAIL` flags (1 for breached, 0 otherwise).

4. **Execution and Resumption**: The script processes each ticker for all specified beta periods. It saves results incrementally to `Backtest_Audit_Results.csv` and can resume where it left off if interrupted, skipping already processed ticker/beta combinations.

## 5. Stress Event Calendar

The script tests against the following historical and hypothetical market crashes:

*   **Black Monday 1987:** `1987-10-14` to `1987-10-26`
*   **DotCom Crash 2000:** `2000-03-20` to `2000-04-15`
*   **Lehman/GFC 2008:** `2008-09-10` to `2008-10-20`
*   **COVID Crash 2020:** `2020-03-05` to `2020-03-25`
*   **Aug 2024 Unwind:** `2024-08-01` to `2024-08-10`
*   **Jan 2025 AI Shock:** `2025-01-20` to `2025-02-10`

* potential date to be added
* **2001:** 9/11 Reopening (`2001-09-10` to `2001-09-28`)
* **2010:** Flash Crash (`2010-05-04` to `2010-05-10`)
* **2015:** China Black Monday (`2015-08-20` to `2015-08-30`)

---

## 6. Output Format

The script appends results to `Backtest_Audit_Results.csv`. The file contains:

| Column | Description |
| :--- | :--- |
| **Ticker** | The stock ticker symbol. |
| **Beta_Period**| The period used for the beta calculation (e.g., '1y', '3y', '5y'). |
| **Event** | The name of the stress event. |
| **IM_Used** | The Initial Margin ratio used for the test. |
| **MCM_Used**| The Margin-Call Margin ratio used for the test. |
| **MM_Used** | The Maintenance Margin ratio used for the test. |
| **Max_3D_Drop** | The worst price drop observed (`1 - worst_ratio`), as a percentage. |
| **IM_Breach** | `1` if the Initial Margin threshold was breached, `0` otherwise. |
| **MCM_Breach** | `1` if the Margin-Call threshold was breached, `0` otherwise. |
| **MM_Breach** | `1` if the Maintenance Margin threshold was breached, `0` otherwise. |
| **LIQ** | `1` if the forced liquidation threshold was breached, `0` otherwise. |
| **FAIL** | `1` if the position's equity reached zero, `0` otherwise. |
| **Mode** | `Real` if using stock data, `Sim(B=X.XX)` if using a beta-simulated proxy. |
