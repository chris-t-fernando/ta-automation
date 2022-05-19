from dateutil.relativedelta import relativedelta
import time
import btalib
import pandas as pd
import logging
import jsonpickle
from datetime import datetime
import warnings
import json

warnings.simplefilter(action="ignore", category=FutureWarning)

log_wp = logging.getLogger("utils")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
log_wp.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)


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
    # log_wp.debug(f"MACD complete in {round(time.time() - start_time,1)}s")

    start_time = time.time()
    # changed to use EMA instead of SMA
    # TODO update column name - pretty shonky doing it this way
    # sma = btalib.sma(bars, period=200)
    # bars["sma_200"] = list(sma["sma"])
    sma = btalib.ema(bars, period=200)
    bars["sma_200"] = list(sma["ema"])
    # log_wp.debug(f"SMA complete in {round(time.time() - start_time,1)}s")

    return bars


def get_red_cycle_start(df: pd.DataFrame, before_date):
    try:
        return df.loc[
            (df["macd_cycle"] == "blue")
            & (df.index < before_date)
            & (df.macd_crossover == True)
        ].index[-1]
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
            f"{symbol}: FOUND BUY SIGNAL AT {df.index[-1]} (MACD {round(row.macd_macd,4)} vs signal {round(row.macd_signal,4)}, SMA {round(last_sma,4)} vs {round(recent_average_sma,4)})"
        )
        return True

    log_wp.debug(
        f"{symbol}: No buy signal at {df.index[-1]} (MACD {round(row.macd_macd,4)} vs signal {round(row.macd_signal,4)}, SMA {round(last_sma,4)} vs {round(recent_average_sma,4)}"
    )
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


def validate_rule(rule):
    required_keys = [
        "symbol",
        "original_stop_loss",
        "current_stop_loss",
        "original_target_price",
        "current_target_price",
        "steps",
        "original_risk",
        "purchase_date",
        "units_held",
        "units_sold",
        "units_bought",
        "win_point_sell_down_pct",
        "win_point_new_stop_loss_pct",
        "risk_point_sell_down_pct",
        "risk_point_new_stop_loss_pct",
    ]

    rule_keys = rule.keys()

    # duplicate key
    duplicate_keys = len(set(rule_keys)) - len(rule_keys)
    if duplicate_keys != 0:
        raise ValueError(
            f'Duplicate rules found for symbol {rule["symbol"]}: {str(set(required_keys) ^ set(rule_keys))}'
        )

    for req_key in required_keys:
        if req_key not in rule_keys:
            raise ValueError(
                f'Invalid rule found for symbol {rule["symbol"]}: {req_key}'
            )


def validate_rules(rules):
    if rules == []:
        log_wp.debug(f"No rules found")
        return True

    found_symbols = []
    for rule in rules:
        validate_rule(rule)
        if rule["symbol"] in found_symbols:
            raise ValueError(f'More than 1 rule found for {rule["symbol"]}')

        log_wp.debug(f'Found valid rule for {rule["symbol"]}')
        found_symbols.append(rule["symbol"])

    log_wp.debug(f"Rules are valid")
    return True


def get_rules(store, back_testing):
    if back_testing:
        path = "/backtest"
    else:
        path = ""

    try:
        return unpickle(
            store.get_parameter(Name=f"/tabot/rules{path}/5m", WithDecryption=False)
            .get("Parameter")
            .get("Value")
        )
    except store.exceptions.ParameterNotFound as e:
        return []


def merge_rules(
    store, symbol: str, action: str, new_rule=None, back_testing: bool = False
):
    if back_testing:
        path = "/backtest"
    else:
        path = ""

    try:
        old_rules = (
            store.get_parameter(Name=f"/tabot/rules{path}/5m")
            .get("Parameter")
            .get("Value")
        )
        rules = unpickle(old_rules)
    except store.exceptions.ParameterNotFound:
        rules = []

    changed = False
    if action == "delete":
        new_rules = []
        for rule in rules:
            if rule["symbol"] != symbol:
                new_rules.append(rule)
            else:
                changed = True

    elif action == "replace":
        new_rules = []
        for rule in rules:
            if rule["symbol"] != symbol:
                new_rules.append(rule)
            else:
                new_rules.append(new_rule)
                changed = True
    elif action == "create":
        new_rules = []
        for rule in rules:
            if rule["symbol"] != symbol:
                new_rules.append(rule)
            else:
                # TODO this can actually happen - then what happens?!
                # raise ValueError(
                #    f"Cannot create {symbol} - symbol already exists in store rules!"
                # )
                ...

        new_rules.append(new_rule)
        changed = True

    else:
        log_wp.debug(f"{symbol}: No action specified")
        raise Exception("No action specified")

    if changed == True:
        log_wp.debug(f"{symbol}: Merged rules successfully")
        return new_rules
    else:
        log_wp.debug(f"{symbol}: No rules changed!")
        return False


def put_rules(store, symbol: str, new_rules: list, back_testing: bool = False):
    # return True
    if back_testing:
        path = "/backtest"
    else:
        path = ""

    store.put_parameter(
        Name=f"/tabot/rules{path}/5m",
        Value=pickle(new_rules),
        Type="String",
        Overwrite=True,
    )
    log_wp.debug(f"{symbol}: Successfully wrote updated rules")

    return True


def get_stored_state(store, back_testing: bool = False):
    if back_testing:
        back_testing_path = "/back_testing"
    else:
        back_testing_path = ""

    try:
        json_stored_state = (
            store.get_parameter(
                Name=f"/tabot{back_testing_path}/state", WithDecryption=False
            )
            .get("Parameter")
            .get("Value")
        )
        return unpickle(json_stored_state)
    except store.exceptions.ParameterNotFound as e:
        return []


def put_stored_state(store, new_state=list, back_testing: bool = False):
    if back_testing:
        back_testing_path = "/back_testing"
    else:
        back_testing_path = ""

    store.put_parameter(
        Name=f"/tabot{back_testing_path}/state",
        Value=pickle(new_state),
        Type="String",
        Overwrite=True,
    )


def trigger_sell_point(rule, last_price, period):
    if rule["current_target_price"] < last_price:
        log_wp.warning(
            f'{rule["symbol"]}: Target price met at {period} (market {last_price} vs rule {rule["current_target_price"]})'
        )
        return True
    else:
        return False


def trigger_risk_point(rule, last_price, period):
    if (last_price + rule["current_risk"]) < last_price:
        log_wp.warning(
            f'{rule["symbol"]}: Risk price met at {period} (market {last_price} vs rule {(last_price + rule["current_risk"])}'
        )
        return True
    else:
        return False


def trigger_stop_loss(rule, last_price, period):
    if rule["current_stop_loss"] >= last_price:
        log_wp.warning(
            f'{rule["symbol"]}: Stop loss triggered at {period} (market {last_price} vs rule {rule["current_stop_loss"]})'
        )
        return True
    else:
        return False


def pickle(object):
    return jsonpickle.encode(object)


def unpickle(object):
    return jsonpickle.decode(object)
