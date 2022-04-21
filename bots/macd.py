import logging
from tracemalloc import start
import boto3
from alpaca_wrapper import AlpacaAPI
from swyftx_wrapper import SwyftxAPI
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pytz
import pandas as pd
import btalib
import time

log_wp = logging.getLogger(__name__)  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("macd_bot.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)12s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)


def merge_bars(bars, new_bars):
    return pd.concat([bars, new_bars])


def add_signals(bars, interval):
    interval_delta, max_range = get_interval_settings(interval)

    start_time = time.time()
    macd = btalib.macd(bars)
    bars["macd_macd"] = macd["macd"]
    bars["macd_signal"] = macd["signal"]
    bars["macd_histogram"] = macd["histogram"]

    bars["macd_crossover"] = False
    bars["macd_signal_crossover"] = False
    bars["macd_above_signal"] = False
    bars["macd_cycle"] = None

    # loops looking for three things - macd-signal crossover, signal-macd crossover, and whether macd is above signal
    cycle = None

    for d in bars.index:
        # start with crossover search
        # convert index to a datetime so we can do a delta against it                           ****************
        previous_key = d - interval_delta
        # previous key had macd less than or equal to signal
        if bars["macd_macd"].loc[d] > bars["macd_signal"].loc[d]:
            # macd is greater than signal - crossover
            bars.at[d, "macd_above_signal"] = True
            try:
                if (
                    bars["macd_macd"].loc[previous_key]
                    <= bars["macd_signal"].loc[previous_key]
                ):
                    cycle = "blue"
                    bars.at[d, "macd_crossover"] = True

            except KeyError as e:
                # ellipsis because i don't care if i'm missing data (maybe i should...)
                ...

        if bars["macd_macd"].loc[d] < bars["macd_signal"].loc[d]:
            # macd is less than signal
            try:
                if (
                    bars["macd_macd"].loc[previous_key]
                    >= bars["macd_signal"].loc[previous_key]
                ):
                    cycle = "red"
                    bars.at[d, "macd_signal_crossover"] = True

            except KeyError as e:
                # ellipsis because i don't care if i'm missing data (maybe i should...)
                ...

        bars.at[d, "macd_cycle"] = cycle
    log_wp.debug(f"MACD complete in {round(time.time() - start_time,1)}s")

    start_time = time.time()
    sma = btalib.sma(bars, period=200)
    bars["sma_200"] = list(sma["sma"])
    log_wp.debug(f"SMA complete in {round(time.time() - start_time,1)}s")

    return bars


def get_bars(symbol, interval, from_date=None, to_date=None, initialised=True):
    tz = pytz.timezone("America/New_York")
    interval_delta, max_range = get_interval_settings(interval)
    if initialised == False:
        # we actually need to grab everything
        yf_start = datetime.now(tz) - max_range
    else:
        # if we've specified a date, we're probably refreshing our dataset over time
        if from_date:
            # widen the window out, just to make sure we don't miss any data in the refresh
            yf_start = from_date.replace(tzinfo=tz) - (interval_delta * 2)
        else:
            # we're refreshing but didn't specify a date, so assume its in the last x minutes/hours
            yf_start = datetime.now(tz) - (interval_delta * 2)

    # didn't specify an end date so go up til now
    if to_date == None:
        yf_end = datetime.now(tz)
    else:
        # specified an end date so use it
        yf_end = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)

    bars = yf.Ticker(symbol).history(
        start=yf_start, end=yf_end, interval=interval, actions=False
    )

    if len(bars) == 0:
        # something went wrong - usually bad symbol and search parameters
        print("error! no data")

    bars = bars.loc[bars.index <= yf_end]
    bars = bars.tz_localize(None)
    return bars


def get_interval_settings(interval):
    minutes_intervals = ["1m", "2m", "5m", "15m", "30m", "60m", "90m"]
    max_period = {
        "1m": 6,
        "2m": 59,
        "5m": 59,
        "15m": 59,
        "30m": 59,
        "60m": 500,
        "90m": 59,
        "1h": 500,
        "1d": 2000,
        "5d": 500,
        "1wk": 500,
        "1mo": 500,
        "3mo": 500,
    }

    if interval in minutes_intervals:
        return (
            relativedelta(minutes=int(interval[:-1])),
            relativedelta(days=max_period[interval]),
        )
    elif interval == "1h":
        return (
            relativedelta(hours=int(interval[:-1])),
            relativedelta(days=max_period[interval]),
        )
    elif interval == "1d" or interval == "5d":
        return (
            relativedelta(days=int(interval[:-1])),
            relativedelta(days=max_period[interval]),
        )
    elif interval == "1wk":
        return (
            relativedelta(weeks=int(interval[:-2])),
            relativedelta(days=max_period[interval]),
        )
    elif interval == "1mo" or interval == "3mo":
        raise ValueError("I can't be bothered implementing month intervals")
        return (
            relativedelta(months=int(interval[:-2])),
            relativedelta(days=max_period[interval]),
        )
    else:
        # got an unknown interval
        raise ValueError(f"Unknown interval type {interval}")


class Symbol:
    def __init__(
        self, symbol, api, interval, real_money_trading, ssm, data_source, to_date
    ):
        self.symbol = symbol
        self.api = api
        self.interval = interval
        self.real_money_trading = real_money_trading
        self.ssm = ssm
        self.data_source = data_source
        self.initialised = False

        bars = get_bars(
            symbol=symbol,
            interval=interval,
            to_date=to_date,
            initialised=False,
        )
        self.bars = add_signals(bars, interval)

    def update_bars(self, from_date=None, to_date=None):
        if from_date == None:
            from_date = self.bars.index[-1]

        new_bars = get_bars(
            symbol=self.symbol,
            interval=self.interval,
            from_date=from_date,
            to_date=to_date,
        )
        new_bars = add_signals(new_bars, interval=self.interval)
        self.bars = merge_bars(self.bars, new_bars)


class MacdBot:
    jobs = None

    def __init__(self, ssm, data_source, start_period=None):
        interval = "5m"
        real_money_trading = False
        self.ssm = ssm
        self.data_source = data_source

        # get jobs
        symbols = [
            {"symbol": "AAPL", "api": "swyftx"},
            {"symbol": "AXS", "api": "swyftx"},
        ]

        # get brokers and then set them up
        self.api_list = []
        for api in symbols:
            self.api_list.append(api["api"])
            log_wp.debug(f"Found broker {api}")
        self.api_dict = self.setup_brokers(api_list=self.api_list, ssm=ssm)

        # set up individual symbols
        self.symbols = {}
        for s in symbols:
            start_time = time.time()
            self.symbols[s["symbol"]] = Symbol(
                symbol=s["symbol"],
                interval=interval,
                real_money_trading=real_money_trading,
                api=self.api_dict[s["api"]],
                ssm=ssm,
                data_source=data_source,
                to_date="2022-04-01 09:00:00",
            )
            log_wp.debug(
                f'Set up {s["symbol"]} in {round(time.time() - start_time,1)}s'
            )

    def setup_brokers(self, api_list, ssm):
        api_set = set(api_list)
        api_dict = {}

        for api in api_set:
            start_time = time.time()
            if api == "swyftx":
                api_key = (
                    ssm.get_parameter(
                        Name="/tabot/swyftx/access_token", WithDecryption=True
                    )
                    .get("Parameter")
                    .get("Value")
                )
                api_dict[api] = SwyftxAPI(api_key=api_key)

            elif api == "alpaca":
                api_key = (
                    ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=True)
                    .get("Parameter")
                    .get("Value")
                )
                secret_key = (
                    ssm.get_parameter(
                        Name="/tabot/alpaca/security_key", WithDecryption=True
                    )
                    .get("Parameter")
                    .get("Value")
                )
                api_dict[api] = AlpacaAPI(
                    alpaca_key_id=api_key, alpaca_secret_key=secret_key
                )
            else:
                raise ValueError(f"Unknown broker specified {api}")

            log_wp.debug(f"Set up {api} in {round(time.time() - start_time,1)}s")

        return api_dict


def main():
    ssm = boto3.client("ssm")
    data_source = yf

    bot_handler = MacdBot(ssm, data_source)

    start_time = time.time()
    bot_handler.symbols["AAPL"].update_bars()
    print("10 minutes: --- %s seconds ---" % (time.time() - start_time))

    print("banana")


if __name__ == "__main__":
    main()
