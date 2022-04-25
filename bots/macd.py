import logging
from tracemalloc import start
import boto3
from alpaca_wrapper import AlpacaAPI
from swyftx_wrapper import SwyftxAPI
from back_test_wrapper import BackTestAPI
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pytz
import pandas as pd
import btalib
import time
import math
import json

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


def clean(number):
    number = round(number, 2)
    return "{:,}".format(number)


class BuyOrder:
    def __init__(
        self,
        symbol,
        df,
        profit_target: float = 1.5,
    ):
        self.symbol = symbol
        self.capital = 2000

        self.blue_cycle_start = get_blue_cycle_start(df=df)
        self.red_cycle_start = get_red_cycle_start(
            df=df, before_date=self.blue_cycle_start
        )
        self.blue_cycle_record = df.loc[self.blue_cycle_start]

        self.blue_cycle_macd = self.blue_cycle_record.macd_macd
        self.blue_cycle_signal = self.blue_cycle_record.macd_signal
        self.blue_cycle_histogram = self.blue_cycle_record.macd_histogram
        self.macd_signal_gap = self.blue_cycle_macd - self.blue_cycle_signal

        # then get the lowest close price since the cycle began
        stop_unit = calculate_stop_loss_unit_price(
            df=df,
            start_date=self.red_cycle_start,
            end_date=self.blue_cycle_start,
        )

        stop_unit_date = calculate_stop_loss_date(
            df=df,
            start_date=self.red_cycle_start,
            end_date=self.blue_cycle_start,
        )

        # and for informational/confidence purposes, hold on to the intervals since this happened
        self.intervals_since_stop = count_intervals(df=df, start_date=stop_unit_date)

        # calculate other order variables
        self.entry_unit = df.Close.iloc[-1]
        self.stop_unit = stop_unit

        self.units = math.floor(self.capital / self.entry_unit)
        self.steps = 0
        self.risk_unit = self.entry_unit - self.stop_unit
        self.risk_value = self.units * self.risk_unit
        self.target_profit = profit_target * self.risk_unit
        self.original_risk_unit = self.risk_unit
        self.original_stop = stop_unit

        self.entry_unit = self.entry_unit
        self.entry_unit = self.entry_unit
        self.target_price = self.entry_unit + self.target_profit

        # fmt: off
        log_wp.info(f"{self.symbol} - {self.red_cycle_start}: Found signal")
        log_wp.info(f"{self.symbol} - Strength:\t\tNot sure how I want to do this yet")
        log_wp.info(f"{self.symbol} - MACD:\t\t\t{self.blue_cycle_macd}")
        log_wp.info(f"{self.symbol} - Signal:\t\t{self.blue_cycle_signal}")
        log_wp.info(f"{self.symbol} - Histogram:\t\t{self.blue_cycle_histogram}")
        log_wp.info(f"{self.symbol} - Capital:\t\t${clean(self.capital)}")
        log_wp.info(f"{self.symbol} - Units to buy:\t\t{clean(self.units)} units")
        log_wp.info(f"{self.symbol} - Entry point:\t\t${clean(self.entry_unit)}")
        log_wp.info(f"{self.symbol} - Stop loss:\t\t${clean(stop_unit)}")
        log_wp.info(f"{self.symbol} - Cycle began:\t\t{self.intervals_since_stop} intervals ago")
        log_wp.info(f"{self.symbol} - Unit risk:\t\t${clean(self.risk_unit)} ({round(self.risk_unit/self.entry_unit*100,1)}% of unit cost)")
        log_wp.info(f"{self.symbol} - Unit profit:\t\t${clean(self.target_profit)} ({round(self.target_profit/self.entry_unit*100,1)}% of unit cost)")
        log_wp.info(f"{self.symbol} - Target price:\t\t${clean(self.target_price)} ({round(self.target_price/self.capital*100,1)}% of capital)")
        # fmt: on


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


# symbol can be backtest naive
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

        bars = self.get_bars(
            symbol=self.symbol,
            interval=interval,
            to_date=to_date,
            initialised=False,
        )
        self.bars = add_signals(bars, interval)

    def get_bars(
        self, symbol, interval, from_date=None, to_date=None, initialised=True
    ):
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

    def update_bars(self, from_date=None, to_date=None):
        if from_date == None:
            from_date = self.bars.index[-1]

        new_bars = self.get_bars(
            symbol=self.symbol,
            interval=self.interval,
            from_date=from_date,
            to_date=to_date,
        )
        new_bars = add_signals(new_bars, interval=self.interval)
        self.bars = merge_bars(self.bars, new_bars)


class MacdBot:
    jobs = None

    def __init__(self, ssm, data_source, start_period=None, back_testing=False):
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
        if back_testing:
            self.api_list = ["back_test"]
        else:
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
            if api == "back_test":
                api_dict[api] = BackTestAPI()
                break

            elif api == "swyftx":
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


# simple function to check if a pandas series contains a macd buy signal
def check_buy_signal(df):
    crossover = False
    macd_negative = False
    sma_trending_up = False

    row = df.iloc[-1]
    if row.macd_crossover == True:
        crossover = True
        log_wp.debug(f"MACD crossover found")

    if row.macd_macd < 0:
        macd_negative = True
        log_wp.debug(f"MACD is less than 0: {row.macd_macd}")

    last_sma = get_last_sma(df=df)
    recent_average_sma = get_recent_average_sma(df=df)
    sma_trending_up = check_sma(
        last_sma=last_sma, recent_average_sma=recent_average_sma
    )
    if sma_trending_up:
        log_wp.debug(
            f"SMA trending up: last {last_sma}, recent average {recent_average_sma}"
        )

    if crossover and macd_negative and sma_trending_up:
        # all conditions met for a buy
        log_wp.debug(
            f"BUY found - crossover, macd_negative and sma_trending_up all met"
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
        log_wp.debug(f"True last SMA {last_sma} > {recent_average_sma}")
        return True
    else:
        log_wp.debug(f"False last SMA {last_sma} > {recent_average_sma}")
        return False


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


def write_rules(symbol: str, action: str, new_rule=None):
    ssm = boto3.client("ssm")
    old_rules = ssm.get_parameter(Name="/tabot/rules/5m").get("Parameter").get("Value")
    rules = json.loads(old_rules)

    changed = False
    if action == "delete":
        new_rules = []
        for rule in rules:
            if rule["symbol"].lower() != symbol.lower():
                new_rules.append(rule)
            else:
                changed = True

    elif action == "replace":
        new_rules = []
        for rule in rules:
            if rule["symbol"].lower() != symbol.lower():
                new_rules.append(rule)
            else:
                new_rules.append(new_rule)
                changed = True
    elif action == "create":
        new_rules = []
        for rule in rules:
            if rule["symbol"].lower() != symbol.lower():
                new_rules.append(rule)
            else:
                raise ValueError(
                    f"Symbol already exists in SSM rules! {symbol.lower()}"
                )

        new_rules.append(new_rule)
        changed = True

    else:
        raise Exception("write_rules: No action specified?")

    if changed == True:
        ssm.put_parameter(
            Name="/tabot/rules/5m",
            Value=json.dumps(new_rules),
            Type="String",
            Overwrite=True,
        )
    else:
        print(f"Symbol {symbol} - tried updating rules but nothing to change")

    return True


def apply_rules(rules, positions, last_close_dict):
    stop_loss_triggered = []
    sell_point_triggered = []
    risk_point_triggered = []
    trigger_results = []

    ssm = boto3.client("ssm")
    api_dict = setup_brokers(broker_list=["alpaca", "swyftx"], ssm=ssm)

    for broker in positions:
        for held in positions[broker]:
            held_symbol = held.symbol.lower()
            held_quantity = float(held.quantity)
            last_close = last_close_dict[held_symbol]

            for rule in rules:
                rule_symbol = rule["symbol"].lower()

                if rule_symbol == held_symbol:
                    # matched a rule and a holding
                    trigger_stop = trigger_stop_loss(rule, last_close)
                    trigger_sell = trigger_sell_point(rule, last_close)
                    trigger_risk = trigger_risk_point(rule, last_close)
                    if trigger_stop:
                        # stop loss hit! liquidate
                        close_response = api_dict[broker].close_position(held_symbol)

                        if close_response.success:
                            # also need to write an updated rule to SSM for next run
                            updated_rules = write_rules(
                                action="delete", symbol=held_symbol
                            )

                            # hold on to this for reporting
                            stop_loss_triggered.append(
                                {
                                    "symbol": held_symbol,
                                    "last_close": last_close,
                                    "rule": rule,
                                }
                            )

                            print(
                                f"Symbol {held_symbol} hit stop loss and was liquidated"
                            )
                        else:
                            # need a better way of notifying me of this stuff
                            print(
                                f"CRITICAL - SYMBOL {held_symbol} HIT STOP LOSS BUT FAILED TO BE LIQUIDATED ****** DO NOT IGNORE THIS *****"
                            )
                            trigger_results.append(
                                f"CRITICAL: SYMBOL {held_symbol} HIT STOP LOSS {last_close} BUT FAILED TO BE LIQUIDATED"
                            )

                    elif trigger_sell or trigger_risk:
                        if trigger_sell:
                            new_target_pct = rule["win_point_sell_down_pct"]
                            # reporting
                            sell_point_triggered.append(
                                {
                                    "symbol": held_symbol,
                                    "last_close": last_close,
                                    "rule": rule,
                                }
                            )
                        else:
                            # trigger risk
                            new_target_pct = rule["risk_point_sell_down_pct"]
                            # reporting
                            risk_point_triggered.append(
                                {
                                    "symbol": held_symbol,
                                    "last_close": last_close,
                                    "rule": rule,
                                }
                            )

                        # hit high watermark of target price
                        units_to_sell = held_quantity * new_target_pct
                        sell_response = api_dict[broker].sell_order_market(
                            symbol=held_symbol, units=units_to_sell
                        )
                        sell_value = sell_response.total_value

                        if sell_response.success:
                            print(
                                f'Symbol {held_symbol} hit target sale point. Successfully sold {round(rule["win_point_sell_down_pct"]*100,0)}% of units for total value {round(sell_value,2)}'
                            )

                            new_units_held = (
                                api_dict[broker]
                                .get_position(symbol=held_symbol)
                                .quantity
                            )

                            updated_ssm_rule = rule.copy()

                            new_units_sold = rule["units_sold"] + sell_response.units
                            new_sales_obj = {
                                "units": new_units_sold,
                                "sale_price": sell_response.unit_price,
                            }
                            new_steps = updated_ssm_rule["steps"] + 1
                            new_risk = rule["original_risk"] * new_steps
                            new_stop_loss = sell_response.unit_price + new_risk

                            updated_ssm_rule["current_stop_loss"] = new_stop_loss
                            updated_ssm_rule["current_risk"] = new_risk
                            updated_ssm_rule["sales"].append(new_sales_obj)
                            updated_ssm_rule["units_held"] = new_units_held
                            updated_ssm_rule["units_sold"] = new_units_sold
                            updated_ssm_rule["steps"] += new_steps
                            updated_ssm_rule["current_target_price"] = (
                                updated_ssm_rule["current_target_price"]
                                + updated_ssm_rule["original_risk"]
                            )

                            updated_rules = write_rules(
                                action="replace",
                                symbol=held_symbol,
                                new_rule=updated_ssm_rule,
                            )

                        else:
                            # need a better way of notifying me of this stuff
                            print(
                                f"CRITICAL - SYMBOL {held_symbol} FAILED TO TAKE PROFIT ****** DO NOT IGNORE THIS *****"
                            )
                            trigger_results.append(
                                f"CRITICAL: SYMBOL {held_symbol} FAILED TO TAKE PROFIT"
                            )

                    else:
                        print("do nothing")

    return {
        "stop_loss": stop_loss_triggered,
        "sell_point": sell_point_triggered,
        "risk_point": risk_point_triggered,
    }


def do_back_testing(bot_handler, rules):
    buy_position_taken = False
    log_wp.debug(f"Starting back testing...")
    for symbol in bot_handler.symbols:
        # shorthand for this bot's bars, to make this code more legible
        bars = bot_handler.symbols["AAPL"].bars
        # start at the first row that doesn't have na for sma_200
        backtest_start = bars.loc[bars.sma_200.notnull()].index[0]
        log_wp.debug(f"{symbol} Back testing starts at {backtest_start}")
        bars = bars.loc[backtest_start:]
        for index in bars.index:
            current_bars = bars.loc[:index]
            if not buy_position_taken:
                if check_buy_signal(current_bars):
                    buy_order = BuyOrder(symbol, current_bars)
                    buy_position_taken = True
                    log_wp.debug(f"{symbol} Found buy at {index}")
            else:  # need to bring in rules for sale, and they need to be the same as you'd use in real life
                # in sales
                ...


def main():
    back_testing = True
    poll_time = 5
    log_wp.debug(
        f"Starting up, poll time is {poll_time}m, back testing is {back_testing}"
    )
    ssm = boto3.client("ssm")
    data_source = yf

    rules = get_rules()
    validate_rules(rules)
    bot_handler = MacdBot(ssm, data_source, back_testing=back_testing)

    if back_testing:
        do_back_testing(bot_handle=bot_handler, rules=rules)

    else:
        while True:
            start_time = time.time()
            for symbol in bot_handler.symbols:
                bot_handler.symbols[symbol].update_bars()
            log_wp.debug(f"{round(time.time() - start_time,1)}s to update all symbols")

            pause = get_pause()
            log_wp.debug(f"Sleeping for {round(pause,1)}s")
            time.sleep(pause)


def trigger_sell_point(rule, last_price):
    if rule["current_target_price"] < last_price:
        print(
            f'{rule["symbol"]}: Target price met (market {last_price} vs {rule["current_target_price"]})'
        )
        return True
    else:
        return False


def trigger_risk_point(rule, last_price):
    if (last_price + rule["current_risk"]) < last_price:
        print(
            f'{rule["symbol"]}: Risk price met (market {last_price} vs {(last_price + rule["current_risk"])}'
        )
        return True
    else:
        return False


def trigger_stop_loss(rule, last_price):
    if rule["current_stop_loss"] >= last_price:
        print(
            f'{rule["symbol"]}: Stop loss triggered (market {last_price} vs {rule["current_stop_loss"]})'
        )
        return True
    else:
        return False


def get_rules(ssm):
    return json.loads(
        ssm.get_parameter(Name="/tabot/rules/5m", WithDecryption=False)
        .get("Parameter")
        .get("Value")
    )


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
    found_symbols = []
    for rule in rules:
        validate_rule(rule)
        if rule["symbol"] in found_symbols:
            raise ValueError(f'More than 1 rule found for {rule["symbol"]}')

        found_symbols.append(rule["symbol"])

    return True


if __name__ == "__main__":
    main()
