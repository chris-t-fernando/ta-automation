import utils
import warnings
import sample_symbols

warnings.simplefilter(action="ignore", category=FutureWarning)

symbols = [
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
    "GME",
]

# all_symbols = (
#    sample_symbols.mixed_symbols
#    + sample_symbols.nyse_symbols_big
#    + sample_symbols.nyse_symbols_medium
#    + sample_symbols.nyse_symbols
#    + sample_symbols.mixed_symbols_small
#    + sample_symbols.crypto_symbol
#    + sample_symbols.crypto_symbols_all
# )

# the_symbols = [{"symbol": "DAI-USD", "api": "alpaca"}]

the_symbols = sample_symbols.input_symbols["everything"]

symbols = []

for s in the_symbols:
    symbols.append(s["symbol"])

interval = "5m"
interval_delta, max_range = utils.get_interval_settings(interval)

utils.save_bars(symbols=symbols, interval=interval, max_range=max_range)
# load_bars(["AAPL"])
# save_bars(["banana"])
# load_bars(["banana"])
