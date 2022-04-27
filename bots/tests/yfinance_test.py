import yfinance as yf


a = yf.Ticker("AAPL").history(start="2022-04-21", interval="5m", actions=False)
b = yf.Ticker("BHP.AX").history(start="2022-04-21", interval="5m", actions=False)
print("banana")
