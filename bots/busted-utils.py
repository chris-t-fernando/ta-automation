# external packages
import boto3
import btalib
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import logging
from numpy import NaN
import pandas as pd
import pytz
import time
import uuid
import yfinance as yf

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

# my modules
from iparameter_store import IParameterStore

log_wp = logging.getLogger("utils")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("utils.log")
log_wp.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)


def get_interval_integer(interval):
    if interval in ["1m", "2m", "5m", "15m", "30m"]:
        return int(interval[:-1])

    raise ValueError("I can't be bothered implementing week intervals")


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
        raise ValueError("I can't be bothered implementing hourly intervals")
        return (
            relativedelta(hours=int(interval[:-1])),
            relativedelta(days=max_period[interval]),
        )
    elif interval == "1d" or interval == "5d":
        raise ValueError("I can't be bothered implementing day intervals")
        return (
            relativedelta(days=int(interval[:-1])),
            relativedelta(days=max_period[interval]),
        )
    elif interval == "1wk":
        raise ValueError("I can't be bothered implementing week intervals")
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

    # first check if this is a brand new data frame or if we're just freshening an existing on
    if "macd_macd" not in bars.columns:
        # this is a new dataframe
        # add the new columns to it
        bars = bars.assign(
            macd_macd=NaN,
            macd_signal=NaN,
            macd_histogram=NaN,
            macd_crossover=False,
            macd_signal_crossover=False,
            macd_above_signal=False,
            macd_cycle="",
        )
        analyse_bars = bars
    else:
        # ignore the first couple hundred rows because they can't be analysed
        ignore_date = bars.index[200]
        # get position of first nan
        if len(bars.loc[(bars.macd_macd.isnull()) & (bars.index > ignore_date)]) == 0:
            merge_from = 300
        else:
            merge_from = bars.index.get_loc(
                bars.loc[(bars.macd_macd.isnull()) & (bars.index > ignore_date)].index[
                    0
                ]
            )

        length_of_new_bars = len(bars) - merge_from

        # 300 for SMA plus some historical data/fat
        if length_of_new_bars < 300:
            analyse_bar_length = 300
        else:
            analyse_bar_length = length_of_new_bars

        analyse_bars = bars.iloc[-analyse_bar_length:]

    # do ta against the relevant rows
    macd = btalib.macd(analyse_bars)

    # i don't like the default column names that come back from btalib
    renamed_macd = macd.df.rename(
        columns={
            "macd": "macd_macd",
            "signal": "macd_signal",
            "histogram": "macd_histogram",
        }
    )

    # merge any actual values in where there were NaNs before
    bars.fillna(renamed_macd, inplace=True)

    # now do my hacky one by one iteration looking for crossovers etc
    # loops looking for three things - macd-signal crossover, signal-macd crossover, and whether macd is above signal
    # first default crossovers to False
    bars.macd_crossover.fillna(False, inplace=True)
    bars.macd_above_signal.fillna(False, inplace=True)
    bars.macd_signal_crossover.fillna(False, inplace=True)
    # bars.macd_crossover.loc[bars.macd_crossover.isnull()] = False
    # bars.macd_above_signal.loc[bars.macd_above_signal.isnull()] = False
    # bars.macd_signal_crossover.loc[bars.macd_signal_crossover.isnull()] = False

    cycle = None

    # for d in bars.index:
    for d in analyse_bars.index:
        # start with crossover search
        # convert index to a datetime so we can do a delta against it
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
    # log_wp.debug(f"MACD complete in {round(time.time() - start_time,1)}s")

    start_time = time.time()
    # changed to use EMA instead of SMA
    # TODO update column name - pretty shonky doing it this way
    sma = btalib.sma(bars, period=200)
    bars["sma_200"] = list(sma["sma"])
    # sma = btalib.ema(bars, period=200)
    # bars["sma_200"] = list(sma["ema"])
    # log_wp.debug(f"SMA complete in {round(time.time() - start_time,1)}s")

    return bars



def clean(number):
    number = round(number, 4)
    return "{:,}".format(number)


# simple function to check if a pandas series contains a macd buy signal
def check_buy_signal(df, symbol, bot_telemetry):
    telemetry_reasons = []
    crossover = False
    macd_negative = False
    sma_trending_up = False

    row = df.iloc[-1]
    if row.macd_crossover == True:
        crossover = True
        # log_wp.debug(f"MACD crossover found")
        telemetry_reasons.append("MACD crossover found")
    else:
        telemetry_reasons.append("MACD crossover was not found")

    if row.macd_macd < 0:
        macd_negative = True
        # log_wp.debug(f"MACD is less than 0: {row.macd_macd}")
        telemetry_reasons.append("MACD is negative")
    else:
        telemetry_reasons.append("MACD is not negative")

    last_sma = get_last_sma(df=df)
    recent_average_sma = get_recent_average_sma(df=df)
    sma_trending_up = check_sma(
        last_sma=last_sma, recent_average_sma=recent_average_sma
    )

    if sma_trending_up:
        telemetry_reasons.append("SMA is upward")
    else:
        telemetry_reasons.append("SMA not trending upward")

    # only bother writing to telemetry if we find a signal crossover - otherwise there's too much noise
    if row.macd_crossover:
        # string summary of what we found
        if crossover and macd_negative and sma_trending_up:
            outcome = "buy signal found"
        else:
            outcome = "no signal"

        # flatten the list of reasons why we chose this outcome
        telemetry_reason_string = ", ".join(telemetry_reasons)

        # for insertion into telemetry
        telemetry_row = {
            "symbol": symbol,
            "Open": row.Open,
            "High": row.High,
            "Low": row.Low,
            "Close": row.Close,
            "macd_macd": row.macd_macd,
            "macd_signal": row.macd_signal,
            "macd_histogram": row.macd_histogram,
            "macd_crossover": row.macd_crossover,
            "macd_signal_crossover": row.macd_signal_crossover,
            "macd_above_signal": row.macd_above_signal,
            "macd_cycle": row.macd_cycle,
            "sma_200": row.sma_200,
            "recent_average_sma": recent_average_sma,
            "outcome": outcome,
            "outcome_reason": telemetry_reason_string,
        }

        bot_telemetry.add_cycle_data(telemetry_row)

    # if sma_trending_up:
    # log_wp.debug(
    #    f"SMA trending up: last {last_sma}, recent average {recent_average_sma}"
    # )

    # if crossover and macd_negative and sma_trending_up:
    if crossover and macd_negative:
        # all conditions met for a buy
        log_wp.debug(
            f"{symbol}: FOUND NO SMA BUY SIGNAL AT {df.index[-1]} (MACD {round(row.macd_macd,4)} vs "
            f"signal {round(row.macd_signal,4)}, SMA {round(last_sma,4)} vs {round(recent_average_sma,4)})"
        )
        return True

    # log_wp.debug(
    #    f"{symbol}: No buy signal at {df.index[-1]} (MACD {round(row.macd_macd,4)} vs signal {round(row.macd_signal,4)}, SMA {round(last_sma,4)} vs {round(recent_average_sma,4)}"
    # )
    return False


def generate_id(length: int = 6):
    return uuid.uuid4().hex[:length].upper()


def get_interval_in_seconds(interval):
    interval_int = get_interval_integer(interval)
    seconds = interval_int * 60
    return seconds


def get_pause(interval):
    interval_seconds = get_interval_in_seconds(interval)

    # get current time
    now = datetime.now()
    # convert it to seconds
    now_ts = now.timestamp()
    # how many seconds into the current 5 minute increment are we
    mod = now_ts % interval_seconds
    # 5 minutes minus that = seconds til next 5 minute mark
    pause = interval_seconds - mod
    # sleep for another 90 seconds - this is the yahoo finance gap
    pause += 90
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


def pickle(object):
    pickled_object = json.dumps(object)
    return pickled_object


def unpickle(object):
    return json.loads(object)


def save_bars(symbols: list, interval: str, max_range:float, bucket:str, key_base:str) -> bool:
    for symbol in symbols:
        existing_bars = load_bars(symbols=symbol, bucket=bucket, key_base=key_base)
        if type(existing_bars) == pd.core.frame.DataFrame:
            start = existing_bars.index[-1]
            log_wp.debug(
                f"{symbol}: {len(existing_bars):,d} existing bars found in S3 will be used as starting point"
            )
            existing_rows = len(existing_bars)
        else:
            start = datetime.now() - max_range
            log_wp.debug(
                f"{symbol}: No bars found in S3. Starting point will be YFinance start date {str(start)}"
            )
            existing_rows = 0

        bars = yf.Ticker(symbol).history(start=start, interval=interval, actions=False, debug=False)

        if len(bars) == 0:
            log_wp.warning(f"{symbol}: No YF data - skipping symbol")
            continue

        bars = bars.tz_convert(pytz.utc)

        # trim bars because the last ~3 are weird timestamps with big missing data
        trimmed_bars = bars.iloc[:-4]

        # need to merge old bars with new bars
        if type(existing_bars) == pd.core.frame.DataFrame:
            trimmed_bars = merge_bars(bars=existing_bars, new_bars=trimmed_bars)

        bars_with_signals = add_signals(bars=trimmed_bars, interval=interval)

        # pickled_bars = utils.pickle(bars)
        pickled_bars = bars_with_signals.to_csv()
        if upload_to_s3(
            pickle=pickled_bars, bucket=bucket, key_base=key_base,  key=f"{symbol}.csv", 
        ):
            log_wp.info(f"{symbol}: Saved bars to S3 ({len(bars):,d} records retrieved, {existing_rows:,d} "
            f"were already in S3, {len(trimmed_bars):,d} records saved)")
        else:
            log_wp.error(f"{symbol}: Failed to save bars to S3")

    return True


def upload_to_s3(pickle:str, bucket:str, key_base:str, key):
    s3 = boto3.resource("s3")
    try:
        s3object = s3.Object(bucket, key_base + key)

        s3object.put(
            Body=bytes(pickle.encode("utf-8")),
            StorageClass="ONEZONE_IA",
        )
    except Exception as e:
        log_wp.error(f"Unable to save {key_base + key} to {bucket}: {str(e)}")
        return False

    return True


def load_bars(symbols: list, bucket:str, key_base:str) -> dict:
    single_return = False
    if type(symbols) == str:
        symbols=[symbols]
        single_return = True
    returned_bars = {}
    for symbol in symbols:
        try:
            loaded_csv = pd.read_csv(
                f"s3://{bucket}/{key_base}{symbol}.csv",
                index_col=0,
                parse_dates=True,
                infer_datetime_format=True,
            )
        except FileNotFoundError as e:
            loaded_csv = None

        returned_bars[symbol] = loaded_csv

    if single_return:
        return loaded_csv
    else:
        return returned_bars


