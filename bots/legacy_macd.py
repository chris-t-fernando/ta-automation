from datasources import MockDataSource, YFinanceFeeder
from datetime import timedelta, datetime
from purchase import Purchase
from alpaca_wrapper import AlpacaAPI
from swyftx_wrapper import SwyftxAPI
from math import floor
import pandas as pd
import boto3


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
        fast_mode: bool = True,
        verbose: bool = False,
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
        # this means don't blindly query the DF by incrementing time/date, instead ask the DF for the next row
        self.FAST_MODE = fast_mode
        self.capital = capital
        self.starting_capital = self.capital
        self.symbol = symbol
        self.interval = interval
        self.verbose = verbose
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

        (
            self.interval_delta,
            self.max_range,
            self.tick,
        ) = self.backtest_source.get_interval_settings(interval=self.interval)

        self.bars_start = datetime.now() + timedelta(days=-self.max_range)
        # self.bars_end = datetime.now()
        self.bars_end = self.end_dt

    # todo: get a better signal
    # todo: why is the EMA always <200? even in assets that seem like they're doing alright
    # while True:
    def get_next(self):
        if self.complete == True:
            return None

        df = self.backtest_source.get_bars(
            symbol=self.symbol,
            start=self.bars_start,
            end=self.current_dt,
            interval=self.interval,
            do_macd=True,
            do_sma=True,
        )
        if len(df) == 0:
            print(
                f"Error - dataframe is empty. Check symbol exists or reduce search timespan"
            )
            exit()

        # need to make a copy of df, because the TA library gets pantsy if you add columns to it
        df_output = df.copy(deep=True)

        if self.position_taken == False:
            # STEP 0: GET EMA FOR THE MARKET AS A WHOLE
            # todo

            # STEP 1: are we in a bull market?
            # todo

            # STEP 2: has there been a crossover?
            if (
                len(
                    df_output.loc[
                        (df_output.macd_crossover == True)
                        & (df_output.macd_macd < 0)
                        & (df_output.index == self.current_dt)
                    ]
                )
                == 0
            ):
                # no signal
                if self.verbose:
                    print(f"\r{self.current_dt} - no signal", end="")

            else:
                # is SMA good?
                self.recent_average_sma = (
                    df_output.sma_200.rolling(window=20, min_periods=20).mean().iloc[-1]
                )
                self.last_sma = df_output.iloc[-1].sma_200
                if (self.last_sma > self.recent_average_sma) or self.ignore_sma:

                    # STEP 4: PREP FOR AN ORDER!
                    self.crossover_index = df_output.loc[
                        (df_output.macd_crossover == True) & (df_output.macd_macd < 0)
                    ].index[-1]
                    self.crossover_record = df_output.loc[[self.crossover_index]]
                    self.crossover_index_position = df_output.index.get_loc(
                        self.crossover_index
                    )

                    self.entry_unit = df_output.Close.iloc[-1]
                    # first start with calculating risk and stop loss
                    # stop loss is based on the lowest unit price since this cycle began
                    # first find the beginning of this cycle, which is when the blue line crossed under the red line
                    self.blue_cycle_start = df_output.loc[
                        (df_output["macd_cycle"] == "blue")
                        & (df_output.index < self.crossover_index)
                    ].index[-1]
                    # then get the lowest close price since the cycle began
                    self.stop_unit = df_output.loc[
                        self.blue_cycle_start : self.crossover_index
                    ].Close.min()
                    self.stop_unit_date = df_output.loc[
                        self.blue_cycle_start : self.crossover_index
                    ].Close.idxmin()

                    self.original_stop = self.stop_unit

                    # and for informational/confidence purposes, hold on to the intervals since this happened
                    self.intervals_since_stop = len(
                        df_output.loc[self.stop_unit_date :]
                    )

                    # calculate other order variables
                    self.trade_date = df_output.index[-1]
                    self.steps = 1
                    self.units = floor(self.capital / self.entry_unit)
                    self.risk_unit = self.entry_unit - self.stop_unit
                    self.original_risk_unit = self.risk_unit
                    self.risk_value = self.units * self.risk_unit
                    self.target_profit = self.PROFIT_TARGET * self.risk_unit
                    self.target_price = self.entry_unit + self.target_profit

                    self.macd_signal_gap = (
                        self.crossover_record.macd_macd.values[0]
                        - self.crossover_record.macd_signal.values[0]
                    )
                    self.sma_signal_gap = self.last_sma - self.recent_average_sma

                    self.leftover_capital = self.capital - (
                        self.units * self.entry_unit
                    )

                    self.order = Purchase(
                        unit_quantity=self.units, unit_price=self.entry_unit
                    )

                    print(f"\n{self.crossover_index}: Found signal")
                    print(f"Strength:\t\tNot sure how I want to do this yet")
                    print(f"MACD:\t\t\t{self.crossover_record.macd_macd.values[0]}")
                    print(f"Signal:\t\t\t{self.crossover_record.macd_signal.values[0]}")
                    print(
                        f"Histogram:\t\t{self.crossover_record.macd_histogram.values[0]}"
                    )
                    print(
                        f"SMA:\t\t\t{self.last_sma} vs recent average of {self.recent_average_sma}"
                    )
                    print(f"Capital:\t\t${clean(self.capital)}")
                    print(f"Units to buy:\t\t{clean(self.units)} units")
                    print(f"Entry point:\t\t${clean(self.entry_unit)}")
                    print(f"Stop loss:\t\t${clean(self.stop_unit)}")
                    print(f"Cycle began:\t\t{self.intervals_since_stop} intervals ago")
                    print(
                        f"Unit risk:\t\t${clean(self.risk_unit)} ({round(self.risk_unit/self.entry_unit*100,1)}% of unit cost)"
                    )
                    print(
                        f"Unit profit:\t\t${clean(self.target_profit)} ({round(self.target_profit/self.entry_unit*100,1)}% of unit cost)"
                    )
                    print(
                        f"Target price:\t\t${clean(self.target_price)} ({round(self.target_price/self.capital*100,1)}% of capital)"
                    )

                    self.position_taken = True

                else:
                    print(
                        f"\r{self.current_dt} - found signal but SMA is trending downward - avoiding this trade                                   ",
                        end="",
                    )
                    self.skipped_trades += 1
                    self.skipped_trades_sma += 1

        else:
            # we have taken a position and need to monitor
            last_close = df.Close.iloc[-1]

            # stop loss!
            if last_close <= self.stop_unit:
                self.order.sell_units(sell_price=last_close)
                self.capital = self.order.get_returns() + self.leftover_capital
                if self.stop_unit > self.original_stop:
                    self.wins += 1
                    self.trade_won = "WON"
                else:
                    self.losses += 1
                    self.trade_won = "LOST"

                self.win_rate = self.wins / (self.wins + self.losses) * 100

                self.trade_duration = df_output.index[-1] - self.trade_date
                self.trade_iteration_count = df_output.index.get_loc(
                    df_output.index[-1]
                ) - df_output.index.get_loc(self.trade_date)
                print(
                    f"\rTrade ran for {self.trade_iteration_count} intervals ({self.trade_duration.days} days) and {self.trade_won}. Increased stop loss {self.steps} times before hitting stop loss ({clean(last_close)} vs {clean(self.stop_unit)}). Win rate {round(self.win_rate,1)}%, balance {clean(self.capital)} (gain/loss of {clean(self.capital-self.starting_capital)})",
                    end="",
                )
                print(
                    f"\n==============================================================="
                )
                self.position_taken = False

            # hit win point, take 50% of winnings
            elif last_close >= self.target_price:
                self.held = self.order.get_units()
                self.units_to_sell = floor(self.held * 0.50)
                self.order.sell_units(
                    sell_price=last_close, unit_quantity=self.units_to_sell
                )

                # and update stop loss
                if self.stop_unit < last_close * 0.99:
                    self.stop_unit = last_close * 0.99

                self.steps += 1  #                                                                    ############## BADLY NAMED VARIABLE
                self.risk_unit = self.original_risk_unit * self.steps
                self.target_profit = self.PROFIT_TARGET * self.risk_unit
                self.target_price = self.entry_unit + self.target_profit

                print(
                    f"\r{df_output.index[-1]} Met target price, new target price {clean(self.target_price)}, new stop price {clean(self.stop_unit)}"
                )

            # hit win
            elif last_close >= (self.entry_unit + self.risk_unit):
                # sell 25%
                self.held = self.order.get_units()
                self.units_to_sell = floor(self.held * 0.25)
                self.order.sell_units(
                    sell_price=last_close, unit_quantity=self.units_to_sell
                )

                # and update stop loss
                if self.stop_unit < last_close * 0.99:
                    self.stop_unit = last_close * 0.99
                    #                                                     ************ HARDCODED BE SMARTER AND USE MACD DIFF

                self.steps += 1
                self.risk_unit = self.original_risk_unit * self.steps
                self.target_profit = self.PROFIT_TARGET * self.risk_unit
                self.target_price = self.entry_unit + self.target_profit

                # print(f"Step #{steps}")
                print(
                    f"\r{df_output.index[-1]} Met target price, new target price {clean(self.target_price)}, new stop price {clean(self.stop_unit)}",
                    end="",
                )
            elif self.verbose:
                print(
                    f"\r{self.current_dt} nothing happened, target price {clean(self.target_price)} / stop loss {clean(self.stop_unit)} holds vs last close of {clean(last_close)}",
                    end="",
                )

        # FAST_MODE means just jump to the next entry in the df, as opposed to ticking the clock (even at times when a market is shut so there'll be no data)
        if self.FAST_MODE:
            next_key = self.backtest_source.get_next()

            if next_key == False:
                self.complete = True
                return False

            self.current_dt = next_key
            # self.current_dt = datetime.now() + relativedelta(
            #    minutes=100
            # )  #   just some fake number that's beyond the horizon we're searching for so that the iteration stops
        else:
            self.current_dt += self.tick

    def get_results(self):
        try:
            self.win_rate = round(self.wins / (self.wins + self.losses) * 100, 1)
            self.loss_rate = 100 - self.win_rate

        except ZeroDivisionError:
            self.win_rate = 0
            self.loss_rate = 0

        macd_change = self.capital - self.starting_capital
        hold_units = floor(
            self.starting_capital / self.backtest_source.bars.Close.iloc[0]
        )
        hold_change = (
            self.backtest_source.bars.Close.iloc[-1]
            - self.backtest_source.bars.Close.iloc[0]
        ) * hold_units
        if hold_change > macd_change:
            better_strategy = "hold"
        else:
            better_strategy = "macd"

        return pd.Series(
            {
                "start": self.start_dt,
                "end": self.current_dt,
                "capital_start": self.starting_capital,
                "capital_end": self.capital,
                "capital_change": macd_change,
                "capital_change_pct": round(
                    (self.capital / self.starting_capital * 100) - 100, 1
                ),
                "intervals": self.interval,
                "trades_total": self.wins + self.losses,
                "trades_won": self.wins,
                "trades_won_rate": self.win_rate,
                "trades_lost": self.losses,
                "trades_lost_rate": self.loss_rate,
                "trades_skipped": self.skipped_trades,
                "hold_units": hold_units,
                "hold_start_buy": self.backtest_source.bars.Close.iloc[0],
                "hold_end_buy": self.backtest_source.bars.Close.iloc[-1],
                "hold_change": hold_change,
                "hold_change_pct": round(
                    (
                        self.backtest_source.bars.Close.iloc[-1]
                        / self.backtest_source.bars.Close.iloc[0]
                        * 100
                    )
                    - 100,
                    1,
                ),
                "better_strategy": better_strategy,
            }
        )

    def do_backtest(self):
        while True:
            self.get_next()
            if self.complete:
                break


def get_jobs():
    capital = 2000
    start = "2022-04-11 15:45:00"
    end = "2022-04-11 15:50:00"
    interval = "5m"

    symbols = [
        {"symbol": "IVV.AX", "broker": "swyftx"},
        {"symbol": "BHP.AX", "broker": "swyftx"},
        {"symbol": "ACN", "broker": "alpaca"},
        {"symbol": "AAPL", "broker": "alpaca"},
        {"symbol": "MSFT", "broker": "alpaca"},
        {"symbol": "RIO", "broker": "alpaca"},
    ]
    # symbols = ["BHP.AX"]

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
        return {
            "type": "buy",
            "symbol": symbol,
            "broker": broker,
            "interval": interval,
            "timestamp": backtest.crossover_index,
            "signal_strength": None,
            "macd_value": backtest.crossover_record.macd_macd.values[0],
            "signal_value": backtest.crossover_record.macd_signal.values[0],
            "macd_signal_gap": backtest.macd_signal_gap,
            "histogram_value": backtest.crossover_record.macd_histogram.values[0],
            "sma_value": backtest.last_sma,
            "sma_recent": backtest.recent_average_sma,
            "sma_gap": backtest.sma_signal_gap,
            "stop_loss_price": backtest.stop_unit,
            "last_price": backtest.entry_unit,
            "current_cycle_duration": backtest.intervals_since_stop,
        }

    return {"type": "pass"}


# i don't think i need this
def _meta_map_merge(ta_results):
    return ta_results


def prioritise_buys(buy_orders):
    # prioritise based on gap between macd and signal
    buys = [o for o in buy_orders if o["type"] == "buy"]
    passes = [o for o in buy_orders if o["type"] == "pass"]
    buys.sort(key=lambda x: x["macd_signal_gap"], reverse=True)
    return buys + passes


def get_funds(jobs):
    balances = {}
    # get unique brokers from job list
    brokers = []

    for symbol in jobs["symbols"]:
        brokers.append(symbol["broker"])

    broker_set = set(brokers)

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
            api = SwyftxAPI(api_key=api_key)
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
            api = AlpacaAPI(alpaca_key_id=api_key, alpaca_secret_key=secret_key)
        else:
            raise ValueError(f"Unknown broker specified {broker}")

        balances[broker] = api.get_account()

    return balances


def execute_orders(balances, jobs):
    # get order size
    ssm = boto3.client("ssm")
    ORDER_SIZE = float(
        ssm.get_parameter(Name="/tabot/order_size", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    # get unique brokers from job list
    brokers = []
    broker_api = {}

    for job in jobs:
        if job["type"] == "buy":
            brokers.append(job["broker"])

    broker_set = set(brokers)

    # instantiate brokers
    for broker in broker_set:
        if broker == "swyftx":
            api_key = (
                ssm.get_parameter(
                    Name="/tabot/swyftx/access_token", WithDecryption=True
                )
                .get("Parameter")
                .get("Value")
            )
            broker_api[broker] = SwyftxAPI(api_key=api_key)
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
            broker_api[broker] = AlpacaAPI(
                alpaca_key_id=api_key, alpaca_secret_key=secret_key
            )
        else:
            raise ValueError(f"Unknown broker specified {broker}")

    # now we have instantiated the brokers, we can iterate through the jobs executing the relevant buy jobs
    for job in jobs:
        if job["type"] == "buy":
            broker = job["broker"]
            api = broker_api[broker]
            symbol = job["symbol"]
            current_balance = balances[broker].assets[api.default_currency]

            if current_balance < ORDER_SIZE:
                print(
                    f"Out of cash! Current balance {api.default_currency}{ORDER_SIZE} vs balance {api.default_currency}{current_balance}"
                )
                break

            order_result = api.buy_order_market(symbol, ORDER_SIZE)
            if not order_result.success:
                print(
                    f"Failed to buy {symbol}. Status: { order_result.status_summary } - { order_result.status_text }"
                )

            job["order_result"] = order_result

            balances[broker].assets["usd"] = current_balance - ORDER_SIZE

    return jobs


def notify(results):
    for job in results:
        print(f"Job report")
        print(f"====================")
        print(f'Symbol: \t\t\t{job["symbol"]}')
        print(f'Broker: \t\t\t{job["broker"]}')
        print(f'Target buy price: \t{job["last_price"]}')
        print(f'Actual buy price: \t{job["unit_price"]}')

    """
        df_report.loc[symbol] = backtest.get_results()
        df_report = pd.DataFrame(
            columns=[
                "start",
                "end",
                "capital_start",
                "capital_end",
                "capital_change",
                "capital_change_pct",
                "intervals",
                "trades_total",
                "trades_won",
                "trades_won_rate",
                "trades_lost",
                "trades_lost_rate",
                "trades_skipped",
                "hold_units",
                "hold_start_buy",
                "hold_end_buy",
                "hold_change",
                "hold_change_pct",
                "better_strategy",
            ],
            index=symbols,
        )
    """


fake_buy = {
    "type": "buy",
    "symbol": "XRP",
    "broker": "swyftx",
    "interval": "5m",
    "timestamp": "2022-04",
    "signal_strength": None,
    "macd_value": -0.05596663025085036,
    "signal_value": -0.1,  # bigger signal gap
    "macd_signal_gap": -0.04403337,
    "histogram_value": 0.005450574852315156,
    "sma_value": 51.72239999771118,
    "sma_recent": 48,
    "sma_gap": 3.722399998,  # steeper climb
    "stop_loss_price": 51.975,
    "last_price": 51.63999938964844,
    "current_cycle_duration": 11,
}


def buys():
    ### STATE MACHINE FLOW ###
    # returns a list of jobs
    jobs = get_jobs()

    # returns a list of ta results
    job_ta_results = _meta_map(jobs)

    # returns a dict of ta results
    job_ta_merged = _meta_map_merge(job_ta_results)

    # add in a fake buy                                     ##### DELETE AFTER TESTING!!
    job_ta_merged.append(fake_buy)
    job_ta_merged.pop(1)

    # returns an ordered list of ta results
    job_prioritised = prioritise_buys(job_ta_merged)

    # returns how much cash we have to spend
    job_funds = get_funds(jobs)

    # returns the results of the buy orders
    execution_results = execute_orders(balances=job_funds, jobs=job_prioritised)

    # send me a notification of the outcome
    notify(results=execution_results)


def sells():
    ...
