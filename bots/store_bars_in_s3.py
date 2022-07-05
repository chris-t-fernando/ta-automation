import utils
import sample_symbols

import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

the_symbols = sample_symbols.input_symbols["crypto_symbols_alpaca_all"]
#the_symbols=[{"symbol": "BTC-USD", "api": "swyftx"}]

symbols = []

for s in the_symbols:
    symbols.append(s["symbol"])

interval = "5m"
interval_delta, max_range = utils.get_interval_settings(interval)

utils.save_bars(symbols=symbols, interval=interval, max_range=max_range, bucket="mfers-tabot", key_base=f"symbol_data/{interval}/")