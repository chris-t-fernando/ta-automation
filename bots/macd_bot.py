from alpaca_wrapper import AlpacaAPI
from swyftx_wrapper import SwyftxAPI
from datasources import MockDataSource, YFinanceFeeder
from datetime import timedelta, datetime
from purchase import Purchase
from math import floor
import pandas as pd
import boto3
import json
from pushover import init, Client
import logging


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


def clean(number):
    number = round(number, 2)
    return "{:,}".format(number)


class BackTrade:
    def __init__(
        self,
        symbol: str,
        capital: float,
        start: str,
        interval: str = "15m",
        end: str = None,
        profit_target: float = 1.5,
        ignore_sma: bool = False,
    ):
        # internal variables, not inputs
        self.position_taken = False
        self.losses = 0
        self.wins = 0
        self.skipped_trades = 0
        self.skipped_trades_sma = 0
        self.complete = False

        ## INPUTS AND CONSTANTS
        self.PROFIT_TARGET = profit_target
        self.capital = capital
        self.starting_capital = self.capital
        self.symbol = symbol
        self.interval = interval
        self.ignore_sma = ignore_sma

        self.start = start
        self.current = self.start

        if end == None:
            self.end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            self.end = end

        # setup
        self.start_dt = datetime.fromisoformat(self.start)
        self.current_dt = datetime.fromisoformat(self.current)
        self.end_dt = datetime.fromisoformat(self.end)

        self.backtest_source = MockDataSource(
            data_source=YFinanceFeeder(),
            real_end=self.end_dt,
        )

        log_wp.debug(
            f"{symbol} - MockDataSource object initilised with start date of {self.start} and real end date {self.end}"
        )

        (
            self.interval_delta,
            self.max_range,
            self.tick_length,
        ) = self.backtest_source.get_interval_settings(interval=self.interval)

        self.bars_start = datetime.now() + timedelta(days=-self.max_range)
        self.bars_end = self.end_dt

        log_wp.debug(
            f"{symbol} - BackTrade object initilised start {self.start_dt} end {self.end_dt}, bars start {self.bars_start} bars end {self.bars_end}"
        )

    def get_last_sma(self, symbol, df):
        return df.iloc[-1].sma_200

    def get_recent_average_sma(self, symbol, df):
        return df.sma_200.rolling(window=20, min_periods=20).mean().iloc[-1]

    def check_sma(self, last_sma: float, recent_average_sma: float, ignore_sma: bool):
        if ignore_sma:
            log_wp.warning(f"Returning True since ignore_sma = {ignore_sma}")
            return True

        if last_sma > recent_average_sma:
            log_wp.debug(f"True last SMA {last_sma} > {recent_average_sma}")
            return last_sma, recent_average_sma
        else:
            log_wp.debug(f"False last SMA {last_sma} > {recent_average_sma}")
            return False

    def get_blue_cycle_start(self, df: pd.DataFrame, before_date):
        return df.loc[(df["macd_cycle"] == "blue") & (df.index < before_date)].index[-1]

    def get_red_cycle_start(self, df: pd.DataFrame):
        return df.loc[(df.macd_crossover == True) & (df.macd_macd < 0)].index[-1]

    def calculate_stop_loss_unit_price(
        self, df: pd.DataFrame, blue_cycle_start, red_cycle_start
    ):
        return df.loc[blue_cycle_start:red_cycle_start].Close.min()

    # TODO there is 100% a better way of doing this
    def calculate_stop_loss_date(
        self, df: pd.DataFrame, blue_cycle_start, red_cycle_start
    ):
        return df.loc[blue_cycle_start:red_cycle_start].Close.idxmin()

    def count_intervals(self, df: pd.DataFrame, start_date, end_date=None):
        if end_date == None:
            return len(df.loc[start_date:])
        else:
            return len(df.loc[start_date:end_date])

    # TODO: get a better signal
    def get_next(self):
        if self.complete == True:
            log_wp.debug(
                f"{self.symbol} - no more bars, now complete {self.bars_start} {self.current_dt}"
            )
            return None

        log_wp.debug(
            f"{self.symbol} - STARTING & GETTING DATA start {self.start} end {self.end} current {self.current_dt}"
        )

        df = self.backtest_source.get_bars(
            symbol=self.symbol,
            start=self.bars_start,
            end=self.current_dt,
            interval=self.interval,
            do_macd=True,
            do_sma=True,
        )

        if len(df) == 0:
            log_wp.error(
                f"{self.symbol} - dataframe is empty but self.complete is {self.complete}. Check symbol exists or reduce search timespan from {self.end_dt}"
            )
            exit()

        # bail out if we've already taken a position
        if self.position_taken:
            log_wp.debug(
                f"{self.symbol} - already taken a position, no signal search needed"
            )
            return False

        crossover_count = len(
            df.loc[
                (df.macd_crossover == True)
                & (df.macd_macd < 0)
                & (df.index == self.current_dt)
            ]
        )

        if crossover_count == 0:
            # no signal
            log_wp.info(f"{self.symbol} - current {self.current_dt} - no signal")
            return False

        # check SMA
        last_sma = self.get_last_sma(symbol=self.symbol, df=df)
        recent_average_sma = self.get_recent_average_sma(symbol=self.symbol, df=df)
        check_sma = self.check_sma(
            last_sma=last_sma,
            recent_average_sma=recent_average_sma,
            ignore_sma=self.ignore_sma,
        )
        if not check_sma:
            log_wp.info(
                f"{self.symbol} - {self.current_dt} - macd good, SMA bad, avoiding trade"
            )
            self.skipped_trades += 1
            self.skipped_trades_sma += 1
            return False

        # SMA is good, MACD is good
        # STEP 4: PREP FOR AN ORDER!
        red_cycle_start = self.get_red_cycle_start(df=df)
        crossover_record = df.loc[red_cycle_start]

        log_wp.debug(f"{self.symbol} - SMA/signal crossover at {red_cycle_start}")

        # first start with calculating risk and stop loss
        # stop loss is based on the lowest unit price since this cycle began
        # first find the beginning of this cycle, which is when the blue line crossed under the red line
        blue_cycle_start = self.get_blue_cycle_start(df=df, before_date=red_cycle_start)

        # then get the lowest close price since the cycle began
        stop_unit = self.calculate_stop_loss_unit_price(
            df=df,
            blue_cycle_start=blue_cycle_start,
            red_cycle_start=red_cycle_start,
        )

        stop_unit_date = self.calculate_stop_loss_date(
            df=df,
            blue_cycle_start=blue_cycle_start,
            red_cycle_start=red_cycle_start,
        )

        original_stop = stop_unit

        # and for informational/confidence purposes, hold on to the intervals since this happened
        intervals_since_stop = self.count_intervals(df=df, start_date=stop_unit_date)

        # calculate other order variables
        entry_unit = df.Close.iloc[-1]
        trade_date = df.index[-1]
        steps = 1
        units = floor(self.capital / entry_unit)
        risk_unit = entry_unit - stop_unit
        original_risk_unit = risk_unit
        risk_value = units * risk_unit
        target_profit = self.PROFIT_TARGET * risk_unit
        target_price = entry_unit + target_profit

        macd_signal_gap = (
            # crossover_record.macd_macd.values[0]
            crossover_record.macd_macd
            # - crossover_record.macd_signal.values[0]
            - crossover_record.macd_signal
        )
        sma_signal_gap = last_sma - recent_average_sma

        self.leftover_capital = self.capital - (units * entry_unit)

        self.order = Purchase(unit_quantity=units, unit_price=entry_unit)

        # fmt: off
        log_wp.info(f"{self.symbol} - {red_cycle_start}: Found signal")
        log_wp.info(f"{self.symbol} - Strength:\t\tNot sure how I want to do this yet")
        log_wp.info(f"{self.symbol} - MACD:\t\t\t{crossover_record.macd_macd.values[0]}")
        log_wp.info(f"{self.symbol} - Signal:\t\t\t{crossover_record.macd_signal.values[0]}")
        log_wp.info(f"{self.symbol} - Histogram:\t\t{crossover_record.macd_histogram.values[0]}")
        log_wp.info(f"{self.symbol} - SMA:\t\t\t{last_sma} vs recent average of {recent_average_sma}")
        log_wp.info(f"{self.symbol} - Capital:\t\t${clean(self.capital)}")
        log_wp.info(f"{self.symbol} - Units to buy:\t\t{clean(units)} units")
        log_wp.info(f"{self.symbol} - Entry point:\t\t${clean(entry_unit)}")
        log_wp.info(f"{self.symbol} - Stop loss:\t\t${clean(stop_unit)}")
        log_wp.info(f"{self.symbol} - Cycle began:\t\t{intervals_since_stop} intervals ago")
        log_wp.info(f"{self.symbol} - Unit risk:\t\t${clean(risk_unit)} ({round(risk_unit/entry_unit*100,1)}% of unit cost)")
        log_wp.info(f"{self.symbol} - Unit profit:\t\t${clean(target_profit)} ({round(target_profit/entry_unit*100,1)}% of unit cost)")
        log_wp.info(f"{self.symbol} - Target price:\t\t${clean(target_price)} ({round(target_price/self.capital*100,1)}% of capital)")
        # fmt: on

        self.position_taken = True

        return True

    def move_to_next_interval(self):
        # get the next interval in the data
        next_key = self.backtest_source.get_next()

        # check if that call failed - if so, we're out of data and should finish up
        if next_key == False:
            log_wp.info(f"{self.symbol} - Finished bar analysis")
            self.complete = True
            return False

        # otherwise, point to the new interval
        self.current_dt = next_key

    # loop through the retrieved data and do the macd analysis against it
    def do_backtest(self):
        while True:
            self.get_next()
            self.move_to_next_interval()
            if self.complete:
                break


def get_jobs():
    capital = 2000
    # start = "2022-04-11 15:45:00"
    # start = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    # end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    start = "2022-03-31 10:25:00"
    end = "2022-03-31 10:35:00"

    interval = "5m"

    symbols = [
        {"symbol": "AAPL", "broker": "swyftx"},
        # {"symbol": "AXS", "broker": "swyftx"},
        # {"symbol": "BTC", "broker": "swyftx"},
        # {"symbol": "ADA", "broker": "swyftx"},
        # {"symbol": "ATOM", "broker": "swyftx"},
        # {"symbol": "ETH", "broker": "swyftx"},
        # {"symbol": "DOT", "broker": "swyftx"},
        # {"symbol": "MATIC", "broker": "swyftx"},
        # {"symbol": "SOL", "broker": "swyftx"},
        # {"symbol": "XRP", "broker": "swyftx"},
        # {"symbol": "MTL", "broker": "swyftx"},
        # {"symbol": "DCR", "broker": "swyftx"},
        # {"symbol": "TWT", "broker": "swyftx"},
        # {"symbol": "FXS", "broker": "swyftx"},
        # {"symbol": "AUDIO", "broker": "swyftx"},
        # {"symbol": "OGN", "broker": "swyftx"},
        # {"symbol": "ALPACA", "broker": "swyftx"},
        # {"symbol": "STX", "broker": "swyftx"},
    ]

    jobs = {
        "capital": capital,
        "start": start,
        "end": end,
        "interval": interval,
        "symbols": symbols,
        "broker": "swyftx",
        "real_money_trading": True,
    }

    return jobs


def _meta_map(jobs):
    job_results = []
    for symbol in jobs["symbols"]:
        job = jobs.copy()
        del job["symbols"]
        job["symbol"] = symbol["symbol"]
        job["broker"] = symbol["broker"]
        job_results.append(do_ta(job))

    return job_results


def do_ta(job):
    symbol = job["symbol"]
    broker = job["broker"]
    capital = job["capital"]
    start = job["start"]
    end = job["end"]
    interval = job["interval"]

    backtest = BackTrade(
        symbol=symbol, capital=capital, start=start, end=end, interval=interval
    )
    backtest.do_backtest()

    if backtest.position_taken:
        log_wp.warning(
            f"{symbol} - found macd and sma signal at {backtest.red_cycle_start}"
        )
        return {
            "type": "buy",
            "symbol": symbol,
            "broker": broker,
            "interval": interval,
            "timestamp": backtest.red_cycle_start,
            "signal_strength": None,
            "macd_value": backtest.crossover_record.macd_macd.values[0],
            "signal_value": backtest.crossover_record.macd_signal.values[0],
            "macd_signal_gap": backtest.macd_signal_gap,
            "histogram_value": backtest.crossover_record.macd_histogram.values[0],
            "sma_value": backtest.last_sma,
            "sma_recent": backtest.recent_average_sma,
            "sma_gap": backtest.sma_signal_gap,
            "stop_loss_price": backtest.stop_unit,
            "target_price": backtest.target_price,
            "unit_risk": backtest.original_risk_unit,
            "last_price": backtest.entry_unit,
            "current_cycle_duration": backtest.intervals_since_stop,
        }

    log_wp.debug(f"{symbol} - finished TA, no macd and sma signal found")
    return {
        "type": "pass",
        "symbol": symbol,
        "broker": broker,
        "interval": interval,
    }


# i don't think i need this - trying to simulate step function data flow
def _meta_map_merge(ta_results):
    return ta_results


def prioritise_buys(buy_orders):
    # prioritise based on gap between macd and signal
    # for debugging purposes, hold on to the original order
    starting_order = [k["symbol"] for k in buy_orders]

    # separate out the buys from the passes (which don't matter in terms of ordering)
    buys = [o for o in buy_orders if o["type"] == "buy"]
    passes = [o for o in buy_orders if o["type"] == "pass"]

    # in-place lambda to order the buy orders
    buys.sort(key=lambda x: x["macd_signal_gap"], reverse=True)

    # for debugging purposes, hold on to the ending order
    ending_order = [k["symbol"] for k in buy_orders]
    log_wp.debug(f"Re-ordered results was {starting_order} to {ending_order}")
    return buys + passes


def setup_brokers(broker_list, ssm=None):
    broker_set = set(broker_list)
    api = {}

    if ssm == None:
        ssm = boto3.client("ssm")

    for broker in broker_set:
        if broker == "swyftx":
            api_key = (
                ssm.get_parameter(
                    Name="/tabot/swyftx/access_token", WithDecryption=True
                )
                .get("Parameter")
                .get("Value")
            )
            api[broker] = SwyftxAPI(api_key=api_key)
        elif broker == "alpaca":
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
            api[broker] = AlpacaAPI(alpaca_key_id=api_key, alpaca_secret_key=secret_key)
        else:
            raise ValueError(f"Unknown broker specified {broker}")

        log_wp.debug(f"Set up broker {broker}")

    return api


def get_funds(jobs):
    balances = {}
    # get unique brokers from job list
    brokers = []

    for symbol in jobs["symbols"]:
        brokers.append(symbol["broker"])

    api_dict = setup_brokers(broker_list=brokers)
    for broker in set(brokers):
        balances[broker] = api_dict[broker].get_account()

    log_wp.debug(f"Got balances {balances}")
    return balances


def execute_orders(balances, jobs):
    log_wp.debug(f"Executing orders {jobs}")

    # get order size
    ssm = boto3.client("ssm")
    order_size = float(
        ssm.get_parameter(Name="/tabot/order_size", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    # get unique brokers from job list
    brokers = []

    for job in jobs:
        if job["type"] == "buy":
            brokers.append(job["broker"])
    log_wp.debug(f"Brokers: {brokers}")

    # instantiate brokers
    api_dict = setup_brokers(broker_list=brokers)

    # iterate through the jobs executing the relevant buy jobs
    for job in jobs:
        if job["type"] == "buy":
            broker = job["broker"]
            api = api_dict[broker]
            symbol = job["symbol"]
            current_balance = balances[broker].assets[api.default_currency]

            if current_balance < order_size:
                print(
                    f"Out of cash! Current balance {api.default_currency}{order_size} vs balance {api.default_currency}{current_balance}"
                )
                break

            order_result = api.buy_order_market(symbol, order_size)
            if not order_result.success:
                print(
                    f"Failed to buy {symbol}. Status: { order_result.status_summary } - { order_result.status_text }"
                )

            job["order_result"] = order_result
            job["unit_price"] = job["order_result"].unit_price
            if job["order_result"].status == 4:
                # no point repeating the filled string
                job["order_status"] = (
                    job["order_result"].status_summary
                    + " (status ID "
                    + str(job["order_result"].status)
                    + ")"
                )
            else:
                job["order_status"] = (
                    job["order_result"].status_summary
                    + " - "
                    + job["order_result"].status_text
                    + " (status ID "
                    + str(job["order_result"].status)
                    + ")"
                )

            currency = api_dict[broker].default_currency
            balances[broker].assets[currency] = current_balance - order_size

            # generate the rule and write it to  SSM
            new_rule = {
                "symbol": symbol.lower(),
                "original_stop_loss": job["stop_loss_price"],
                "current_stop_loss": job["stop_loss_price"],
                "original_target_price": job["target_price"],
                "current_target_price": job["target_price"],
                "steps": 0,
                "original_risk": job["unit_risk"],
                "current_risk": job["unit_risk"],
                "purchase_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "purchase_price": job["order_result"].unit_price,
                "units_held": job["order_result"].quantity,
                "units_sold": 0,
                "units_bought": job["order_result"].quantity,
                "order_id": job["order_result"].order_id,
                "sales": [],
                "win_point_sell_down_pct": 0.5,
                "win_point_new_stop_loss_pct": 0.99,
                "risk_point_sell_down_pct": 0.25,
                "risk_point_new_stop_loss_pct": 0.98,
            }
            write_rules(symbol=symbol, action="create", new_rule=new_rule)

    return jobs


def notify(results):
    ...
    """    print(f"Job report")
    print(f"====================")
    for job in results:
        print(f'Symbol: \t\t{job["symbol"]}')
        if job["type"] == "pass":
            print(f"Skipped - no signal found")

        elif job["type"] == "buy":
            print(f'Broker: \t\t{job["broker"]}')
            print(f'Order status: \t\t{job["order_status"]}')
            print(f'Target buy price: \t{job["last_price"]}')
            print(f'Actual buy price: \t{job["unit_price"]}')
            print(f"- - - - - - -")
    """


def buys():
    ### STATE MACHINE FLOW ###
    # returns a list of jobs
    jobs = get_jobs()

    # returns a list of ta results
    job_ta_results = _meta_map(jobs)

    # temporary logic, aping condition in step functions
    buys = [o for o in job_ta_results if o["type"] == "buy"]

    if len(buys) > 0:
        # returns a dict of ta results
        job_ta_merged = _meta_map_merge(job_ta_results)

        # returns an ordered list of ta results
        job_prioritised = prioritise_buys(job_ta_merged)

        # returns how much cash we have to spend
        job_funds = get_funds(jobs)

        # returns the results of the buy orders
        execution_results = execute_orders(balances=job_funds, jobs=job_prioritised)

        # send me a notification of the outcome
        notify(results=execution_results)
    else:
        notify(results=job_ta_results)


def get_rules():
    ssm = boto3.client("ssm")
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


def get_positions():
    holdings = {}

    ssm = boto3.client("ssm")
    api_dict = setup_brokers(broker_list=["alpaca", "swyftx"], ssm=ssm)

    for broker in api_dict:
        api = api_dict[broker]
        holdings[broker] = api.list_positions()

    return holdings


def trigger_stop_loss(rule, last_price):
    if rule["current_stop_loss"] >= last_price:
        print(
            f'{rule["symbol"]}: Stop loss triggered (market {last_price} vs {rule["current_stop_loss"]})'
        )
        return True
    else:
        return False


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


def get_last_close(positions):
    ssm = boto3.client("ssm")
    api_dict = setup_brokers(broker_list=["alpaca", "swyftx"], ssm=ssm)
    close_dict = {}

    for broker in positions:
        api = api_dict[broker]
        for position in positions[broker]:
            close_dict[position.symbol.lower()] = api.get_last_close(position.symbol)

    return close_dict


def notify_sale(order_results):
    ssm = boto3.client("ssm")
    pushover_api_key = (
        ssm.get_parameter(Name="/tabot/pushover/api_key", WithDecryption=False)
        .get("Parameter")
        .get("Value")
    )
    pushover_user_key = (
        ssm.get_parameter(Name="/tabot/pushover/user_key", WithDecryption=False)
        .get("Parameter")
        .get("Value")
    )
    stop_loss = order_results["stop_loss"]
    sell_point = order_results["sell_point"]
    risk_point = order_results["risk_point"]

    report_string = "MACD BOT SUMMARY\n"
    report_string += "================\n"

    if len(stop_loss) + len(sell_point) + len(risk_point) == 0:
        # nothing happened
        print("No stop loss, risk trigger or target trigger conditions met\n")
        exit()
    else:
        for stop in stop_loss:
            stop_rule = stop["rule"]
            if stop["last_close"] > stop_rule["purchase_price"]:
                deal_outcome = "win"
            else:
                deal_outcome = "loss"

            report_string += (
                f'{stop["symbol"]}: trade terminated at {stop["last_close"]}\n'
            )
            report_string += f'Deal outcome: {deal_outcome} (purchase price {stop_rule["purchase_price"]} vs {stop["last_close"]}\n'

        for sell in sell_point:
            sell_rule = sell["rule"]
            report_string += f'{sell["symbol"]}: 50% selldown at {sell["last_close"]}\n'
            report_string += f'Triggered because {sell["last_close"]} exceeds previous target price of {sell_rule["current_target_price"]}\n'

        for risk in risk_point:
            risk_rule = risk["rule"]
            report_string += f'{risk["symbol"]}: 25% selldown at {risk["last_close"]}\n'
            report_string += f'Triggered because {risk["last_close"]} exceeds risk point price of {(risk_rule["current_risk"]+risk_rule["purchase_price"])}\n'

    init(pushover_api_key)
    Client(pushover_user_key).send_message(
        report_string,
        title=f"MACD bot report",
    )


def sells():
    rules = get_rules()
    validate_rules(rules)
    positions = get_positions()
    last_close = get_last_close(positions)
    order_results = apply_rules(
        rules=rules, positions=positions, last_close_dict=last_close
    )
    notify_sale(order_results)
    # send notifications


buys()
# sells()
