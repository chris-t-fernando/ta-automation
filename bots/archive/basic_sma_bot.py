from datetime import datetime, timedelta
import math
import time
import boto3
from alpaca_trade_api.rest import REST, TimeFrame

# set up alpaca
ssm = boto3.client("ssm")
alpaca_key_id = (
    ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=False)
    .get("Parameter")
    .get("Value")
)
alpaca_secret_key = (
    ssm.get_parameter(Name="/tabot/alpaca/security_key", WithDecryption=False)
    .get("Parameter")
    .get("Value")
)

api = REST(
    key_id=alpaca_key_id,
    secret_key=alpaca_secret_key,
    base_url="https://paper-api.alpaca.markets",
)


# SYMBOL = "BTCUSD"
SYMBOL = "SOLUSD"
SMA_FAST = 12
SMA_SLOW = 24
QTY_PER_TRADE = 100


# Description is given in the article
def get_pause():
    now = datetime.now()
    next_min = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    pause = math.ceil((next_min - now).seconds)
    print(f"Sleep for {pause}")
    return pause


# Same as the function in the random version
def get_position(symbol):
    positions = api.list_positions()
    for p in positions:
        if p.symbol == symbol:
            return float(p.qty)
    return 0


# Returns a series with the moving average
def get_sma(series, periods):
    return series.rolling(periods).mean()


# Checks whether we should buy (fast ma > slow ma)
def get_signal(fast, slow):
    print(f"Fast {fast[-1]}  /  Slow: {slow[-1]}")
    return fast[-1] > slow[-1]


# Get up-to-date 1 minute data from Alpaca and add the moving averages
def get_bars(symbol):
    bars = api.get_crypto_bars(symbol, TimeFrame.Minute).df
    # for some reason sometimes there is no exchange header on these? no idea why
    if "exchange" in bars.columns:
        bars = bars[bars.exchange == bars.exchange.iloc[0]]
    else:
        print(f"No Exchange info.  Weird. {str(bars.columns)}")

    # bars = bars[bars.exchange == "FTXU"]

    bars[f"sma_fast"] = get_sma(bars.close, SMA_FAST)
    bars[f"sma_slow"] = get_sma(bars.close, SMA_SLOW)

    return bars


held = 0

sells = []
buys = []

while True:
    try:
        # GET DATA
        bars = get_bars(symbol=SYMBOL)

        # sometimes the API bugs out. skip these runs
        if len(bars) > 0:
            # CHECK POSITIONS
            position = get_position(symbol=SYMBOL)
            should_buy = get_signal(bars.sma_fast, bars.sma_slow)
            print(f"Position: {position} / Should Buy: {should_buy}")
            # if position == 0 and should_buy == True:
            if should_buy == True:
                # WE BUY ONE BITCOIN
                try:
                    api.submit_order(SYMBOL, qty=QTY_PER_TRADE, side="buy")
                    print(f"Symbol: {SYMBOL} / Side: BUY / Quantity: {QTY_PER_TRADE}")
                    buys.append(bars.close.iloc[-1] * QTY_PER_TRADE)
                except Exception as e:
                    print(
                        f"Symbol: {SYMBOL} / Side: BUY / Quantity: {QTY_PER_TRADE} FAILED due to exception: {str(e)}"
                    )
            elif position > 0 and should_buy == False:
                # WE SELL ONE BITCOIN
                api.submit_order(SYMBOL, qty=QTY_PER_TRADE, side="sell")
                print(f"Symbol: {SYMBOL} / Side: SELL / Quantity: {QTY_PER_TRADE}")
                sells.append(bars.close.iloc[-1] * QTY_PER_TRADE)

            bought = 0
            sold = 0
            for trade in range(0, len(sells)):
                bought += buys[trade]
                sold += sells[trade]

            print(f"Profit {sold-bought}. Total bought {bought} and total sold {sold}")

        else:
            print(f"API bugged out and returned zero rows.  Skipping this interval")

    except Exception as e:
        print(f"Exception occurred, skipping this interval. Exception was {str(e)}")

    time.sleep(get_pause())
    print("*" * 20)
