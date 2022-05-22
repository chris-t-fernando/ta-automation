import boto3
import utils
import yfinance as yf

symbols = [
    "AAPL"
    "AXS,"
    "TSLA,"
    "FB,"
    "GOOG,"
    "MSFT,"
    "NVDA,"
    "NVAX,"
    "BUD,"
    "AMZN,"
    "INFY,"
    "RTX,"
    "ETH-USD,"
    "SOL-USD,"
    "DOGE-USD,"
    "SHIB-USD,"
    "MATIC-USD,"
    "WBTC-USD,"
    "TRX-USD,"
    "UNI-USD,"
    "BAT-USD,"
    "PAXG-USD,"
    "C,"
    "PFE,"
    "GE,"
    "AIG,"
    "WMT,"
    "IBM,"
    "BAC,"
    "JNJ,"
    "GS,"
    "CVX,"
    "PG,"
    "MO,"
    "JPM,"
    "COP,"
    "VLO,"
    "TXN,"
    "SLB,"
    "HD,"
    "UNH,"
    "MRK,"
    "VZ,"
    "CAT,"
    "PD,"
    "DNA,"
    "GM,"
    "HPQ,"
    "KO,"
    "AXP,"
    "UPS,"
    "MMM,"
    "VIA,"
    "WFC,"
    "HAL,"
    "BA,"
    "F,"
    "X,"
    "LLY,"
    "RIG,"
    "AAPL,"
    "GME,"
]
dyn = boto3.resource("dynamodb")
table = dyn.Table("hello-world")

for symbol in symbols:
    bars = yf.Ticker(self.symbol).history(
        start=yf_start,
        interval=self.interval,
        actions=False,
    )

# response = table.get_item(Key={"symbol": "AAPL"})
# table.put_item(Item={"symbol": "AAPL", "OHLC": 1})
print("banan")
