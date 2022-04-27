from dateutil.relativedelta import relativedelta
import time
import btalib
import pandas as pd
import logging
from datetime import datetime

log_wp = logging.getLogger(__name__)  # or pass an explicit name here, e.g. "mylogger"


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


def merge_bars(bars, new_bars):
    return pd.concat([bars, new_bars[~new_bars.index.isin(bars.index)]])


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

def get_red_cycle_start(df: pd.DataFrame, before_date):
    try:
        return df.loc[(df["macd_cycle"] == "blue") & (df.index < before_date)].index[-1]
    except IndexError as e:
        return False


def get_blue_cycle_start(df: pd.DataFrame):
    try:
        return df.loc[(df.macd_crossover == True) & (df.macd_macd < 0)].index[-1]
    except IndexError as e:
        return False


def calculate_stop_loss_unit_price(df: pd.DataFrame, start_date, end_date):
    return df.loc[start_date:end_date].Close.min()


# TODO there is 100% a better way of doing this
def calculate_stop_loss_date(df: pd.DataFrame, start_date, end_date):
    return df.loc[start_date:end_date].Close.idxmin()


def count_intervals(df: pd.DataFrame, start_date, end_date=None):
    if end_date == None:
        return len(df.loc[start_date:])
    else:
        return len(df.loc[start_date:end_date])

def clean(number):
    number = round(number, 2)
    return "{:,}".format(number)

# simple function to check if a pandas series contains a macd buy signal
def check_buy_signal(df, symbol):
    crossover = False
    macd_negative = False
    sma_trending_up = False

    row = df.iloc[-1]
    if row.macd_crossover == True:
        crossover = True
        # log_wp.debug(f"MACD crossover found")

    if row.macd_macd < 0:
        macd_negative = True
        # log_wp.debug(f"MACD is less than 0: {row.macd_macd}")

    last_sma = get_last_sma(df=df)
    recent_average_sma = get_recent_average_sma(df=df)
    sma_trending_up = check_sma(
        last_sma=last_sma, recent_average_sma=recent_average_sma
    )
    # if sma_trending_up:
    # log_wp.debug(
    #    f"SMA trending up: last {last_sma}, recent average {recent_average_sma}"
    # )

    if crossover and macd_negative and sma_trending_up:
        # all conditions met for a buy
        log_wp.warning(
            f"{symbol}: Found buy signal at {df.index[-1]} (MACD {round(row.macd_macd,4)} vs signal {round(row.macd_signal,4)}, SMA {round(last_sma,4)} vs {round(recent_average_sma,4)})"
        )
        return True

    return False


def get_pause():
    # get current time
    now = datetime.now()
    # convert it to seconds
    now_ts = now.timestamp()
    # how many seconds into the current 5 minute increment are we
    mod = now_ts % 300
    # 5 minutes minus that = seconds til next 5 minute mark
    pause = 300 - mod
    # just another couple seconds to make sure the stock data is available when we run
    pause += 2
    return pause


def get_last_sma(df):
    return df.iloc[-1].sma_200


def get_recent_average_sma(df):
    return df.sma_200.rolling(window=20, min_periods=20).mean().iloc[-1]


def check_sma(last_sma: float, recent_average_sma: float, ignore_sma: bool = False):
    if ignore_sma:
        log_wp.warning(f"Returning True since ignore_sma = {ignore_sma}")
        return True

    if last_sma > recent_average_sma:
        # log_wp.debug(f"True last SMA {last_sma} > {recent_average_sma}")
        return True
    else:
        # log_wp.debug(f"False last SMA {last_sma} > {recent_average_sma}")
        return False