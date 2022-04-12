from datetime import datetime, timedelta
import math
import time
import boto3
from alpaca_trade_api.rest import REST, TimeFrame
import sma_bot
from numpy import isnan, nan


def get_pause():
    now = datetime.now()
    next_min = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    pause = math.ceil((next_min - now).seconds)
    print(f"Sleep for {pause}")
    return pause


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

# variables ot be passed to each of the bots
sma_fast = 12
sma_slow = 24
qty_per_trade = 100
symbols = {"SOLUSD", "SUSHIUSD", "DAIUSD", "SHIBUSD", "DOGEUSD", "MKRUSD", "MATICUSD"}

# i'd prefer to use a set here but then i can't sort them by signal pct
bots = []

# instantiate my guys
for symbol in symbols:
    bots.append(
        sma_bot.Bot(
            symbol=symbol,
            sma_fast=sma_fast,
            sma_slow=sma_slow,
            qty_per_trade=qty_per_trade,
        )
    )

# lets roll
while True:
    # todo: not sure what I want to do with this, but i'll hold on to it for now
    this_interval = {}
    buy_signal = []
    sell_signal = []

    # start by looking for the signal
    for this_bot in bots:
        this_interval[this_bot] = this_bot.do_analysis()

    # now look for the buy signals
    bots.sort(key=lambda x: x.last_sma_pct, reverse=True)
    for this_bot in bots:
        # fastest breakout from SMA is the highest %
        if this_bot.last_sma_pct > 1:
            buy_signal.append(this_bot)

    # and then get the sell signals
    bots.sort(key=lambda x: x.last_sma_pct, reverse=False)
    for this_bot in bots:
        # fastest breakout from SMA is the highest %
        if this_bot.last_sma_pct < 1:
            sell_signal.append(this_bot)

    for this_bot in sell_signal:
        this_bot.do_sell()

    for this_bot in buy_signal:
        this_bot.do_buy()

    time.sleep(get_pause())
    print("*" * 20)
