import yfinance as yf
etf = yf.Ticker("TQQQ")
print(etf.info['longName'])  # ProShares UltraPro QQQ
print(etf.info['description']) # Look for leverage multiplier here
