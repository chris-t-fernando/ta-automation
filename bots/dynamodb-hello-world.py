import boto3
import utils
import yfinance as yf
from datetime import datetime

symbols = [
    "AAPL",
    "AXS",
    "TSLA",
    "FB",
    "GOOG",
    "MSFT",
    "NVDA",
    "NVAX",
    "BUD",
    "AMZN",
    "INFY",
    "RTX",
    "ETH-USD",
    "SOL-USD",
    "DOGE-USD",
    "SHIB-USD",
    "MATIC-USD",
    "WBTC-USD",
    "TRX-USD",
    "UNI-USD",
    "BAT-USD",
    "PAXG-USD",
    "C",
    "PFE",
    "GE",
    "AIG",
    "WMT",
    "IBM",
    "BAC",
    "JNJ",
    "GS",
    "CVX",
    "PG",
    "MO",
    "JPM",
    "COP",
    "VLO",
    "TXN",
    "SLB",
    "HD",
    "UNH",
    "MRK",
    "VZ",
    "CAT",
    "PD",
    "DNA",
    "GM",
    "HPQ",
    "KO",
    "AXP",
    "UPS",
    "MMM",
    "VIA",
    "WFC",
    "HAL",
    "BA",
    "F",
    "X",
    "LLY",
    "RIG",
    "AAPL",
    "GME",
]
dyn = boto3.resource("dynamodb")
table = dyn.Table("hello-world")
interval = "5m"
interval_delta, max_range = utils.get_interval_settings(interval)

for symbol in symbols:
    bars = yf.Ticker(symbol).history(
        start=datetime.now() - max_range,
        interval=interval,
        actions=False,
    )

    pickled_bars = utils.pickle(bars)

    # this doesn't work because max record size is 400KiB
    table.put_item(Item={"symbol": symbol, f"OHLC_data_{interval}": pickled_bars})
    response = table.get_item(Key={"symbol": symbol})
    print("banana")

#
#
print("banan")
