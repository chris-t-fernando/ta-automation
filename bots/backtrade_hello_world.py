from feeder import Mocker, YFinanceFeeder
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from purchase import Purchase
from math import floor


def clean(number):
    number = round(number, 2)
    return "{:,}".format(number)


class BackTrade:
    def __init__(self):
        # internal variables, not inputs
        self.position_taken = False
        self.losses = 0
        self.wins = 0
        self.skipped_trades = 0
        self.skipped_trades_sma = 0
        self.complete = False

        ## INPUTS AND CONSTANTS
        self.PROFIT_TARGET = 1.5
        self.FAST_MODE = (
            True  # don't use tick to get next period, use next entry in dataframe
        )
        self.capital = 2000
        self.starting_capital = self.capital
        self.symbol = "IVV.AX"
        self.interval = "60m"

        self.start = "2021-04-01"
        self.current = self.start
        self.end = "2022-04-13"

        # setup
        self.start_dt = datetime.fromisoformat(self.start)
        self.current_dt = datetime.fromisoformat(self.current)
        self.end_dt = datetime.fromisoformat(self.end)

        self.backtest_source = Mocker(
            data_source=YFinanceFeeder(),
            real_end=self.end_dt,
        )

        (
            self.interval_delta,
            self.max_range,
            self.tick,
        ) = self.backtest_source.get_interval_settings(interval=self.interval)
        self.bars_start = datetime.now() + timedelta(days=-self.max_range)
        self.bars_end = datetime.now()

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

            # STEP 2: has there been a crossover since our last run?

            # STEP 3: DID WE FIND A SIGNAL? BAIL OUT IF NOT
            window_start = df_output.index[-1]

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
                print(f"\r{self.current_dt} - no signal", end="")
            else:
                # is SMA good?
                if df_output.iloc[-1].sma_200 > df_output.iloc[-5].sma_200:
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
                    print(f"Capital:\t\t${clean(self.capital)}")
                    print(f"Units to buy:\t\t{clean(self.units)} units")
                    print(f"Entry point:$\t\t{clean(self.entry_unit)}")
                    print(f"Stop loss:$\t\t{clean(self.stop_unit)}")
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
            # we are in sell/stop loss mode
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
            else:
                print(
                    f"\r{self.current_dt} nothing happened, target price {clean(self.target_price)} / stop loss {clean(self.stop_unit)} holds vs last close of {clean(last_close)}",
                    end="",
                )

        # FAST_MODE means just jump to the next entry in the df, as opposed to ticking the clock (even at times when a market is shut so there'll be no data)
        if self.FAST_MODE:
            self.current_dt = self.backtest_source.get_next()
            if self.current_dt == False:
                self.complete = True
                return False
                # self.current_dt = datetime.now() + relativedelta(
                #    minutes=100
                # )  #   just some fake number that's beyond the horizon we're searching for so that the iteration stops
        else:
            self.current_dt += self.tick


backtest = BackTrade()
while True:
    if backtest.get_next() == False:
        if backtest.position_taken:
            backtest.current_holding_value = backtest.order.get_held_value(
                backtest.df_output["Close"].iloc[-1]
            )
        else:
            backtest.current_holding_value = 0

        if (
            backtest.capital + backtest.current_holding_value
        ) >= backtest.starting_capital:
            backtest.outcome_text = "gained"
        else:
            backtest.outcome_text = "lost"

        try:
            backtest.win_rate = round(
                backtest.wins / (backtest.wins + backtest.losses) * 100, 1
            )
            backtest.loss_rate = 100 - backtest.win_rate
        except ZeroDivisionError:
            backtest.win_rate = 0
            backtest.loss_rate = 0

        print(f"\n===============================================================")
        print(
            f"Backtrading of {backtest.symbol} between {backtest.start_dt} and {backtest.current_dt} using {backtest.interval} intervals complete"
        )
        print(f"Starting capital:\t${clean(backtest.starting_capital)}")
        print(f"Ending capital:\t\t${clean(backtest.capital)}")
        print(
            f"Change:\t\t\t${clean(backtest.capital-backtest.starting_capital)} ({backtest.outcome_text} capital)"
        )
        print(
            f"% change:\t\t{round((backtest.capital/backtest.starting_capital*100)-100,1)}%"
        )
        print(f"Total trades:\t\t{backtest.wins+backtest.losses}")
        print(f"Wins:\t\t\t{backtest.wins} ({backtest.win_rate}%)")
        print(f"Losses:\t\t\t{backtest.losses} ({backtest.loss_rate}%)")
        print(
            f"Skipped:\t\t{backtest.skipped_trades} trades (low SMA: {backtest.skipped_trades_sma})"
        )
        break

        """capital = 2000
        self.entry_unit = 0.35
        units = capital / self.entry_unit = 700
        stop_unit = 0.307
        risk = self.entry_unit - stop_unit 0.35 - 0.30 = 0.05
        risk_value = capital * stop_unit = 2000 * 0.30 = 600
        profit_target = 1.5 * risk_value
        entry as macd cross over
        profit should be more than the risk taken on the trade
        stop loss at the pullback of the trend

        25% off the trade when the trade reaches 1 times the risk and move stop loss ot break even
        then set a profit target of 2 times the original risk i took on the trade

        profit = 1.5 times risk taken

        get 200 period EMA for the market segment we're looking at
        if
            EMA is above 200 
        and
            macd blue is higher than signal red
        and
            crossover happens way below zero line on histogram

        get 2% of our funds
        profit target = 1.5 times risk
        stop loss = below the pullback of the trend (i think find the last time red > blue and get close price)
        risk = entry - stop loss

        if we earn the risk amount:
            take 25% of the profit
            move stop loss to where we are now/move our break even to where we are now
            new profit target of 2 times the original risk

        capital = 2000 (2%)
        self.entry_unit = whatever the latest close price was
        units = capital / self.entry_unit = 700
        stop_unit = what is the lowest price its been since the last time the price was the same as now? = 0.307
        risk = self.entry_unit - stop_unit 0.35 - 0.30 = 0.05
        risk_value = capital * stop_unit = 2000 * 0.30 = 600
        profit_target = 1.5 * risk_value
        """
