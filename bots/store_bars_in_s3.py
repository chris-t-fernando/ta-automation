import utils
import warnings

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

interval = "5m"
interval_delta, max_range = utils.get_interval_settings(interval)

utils.save_bars(symbols=symbols, interval=interval, max_range=max_range)
# load_bars(["AAPL"])
# save_bars(["banana"])
# load_bars(["banana"])
