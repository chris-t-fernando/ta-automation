import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
import btalib
from numpy import NaN
from feeder import Mocker, YFinanceFeeder
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import matplotlib.pyplot as plt
import time
from buyorder import Purchase
from math import floor

PROFIT_TARGET = 1.5
FAST_MODE = True  # don't use tick to get next period, use next entry in dataframe
capital = 2000
starting_capital = capital
symbol = "IVV.AX"
interval = "1d"

start = "2022-02-18"
current = start
end = "2022-04-17"

start_dt = datetime.fromisoformat(start)
current_dt = datetime.fromisoformat(current)
end_dt = datetime.fromisoformat(end)


# internal variables, not inputs
position_taken = False
losses = 0
wins = 0
partial_wins = 0


def clean(number):
    number = round(number, 2)
    return "{:,}".format(number)


def get_interval_settings(interval):
    minutes_intervals = ["1m", "2m", "5m", "15m", "30m", "60m", "90m"]
    max_period = {
        "1m": 7,
        "2m": 60,
        "5m": 60,
        "15m": 60,
        "30m": 60,
        "60m": 500,
        "90m": 60,
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
            max_period[interval],
            timedelta(minutes=int(interval[:-1])),
        )
    elif interval == "1h":
        return (
            relativedelta(hours=int(interval[:-1])),
            max_period[interval],
            timedelta(hours=int(interval[:-1])),
        )
    elif interval == "1d" or interval == "5d":
        return (
            relativedelta(days=int(interval[:-1])),
            max_period[interval],
            timedelta(days=int(interval[:-1])),
        )
    elif interval == "1wk":
        return (
            relativedelta(weeks=int(interval[:-2])),
            max_period[interval],
            timedelta(weeks=int(interval[:-2])),
        )
    elif interval == "1mo" or interval == "3mo":
        raise ValueError("I can't be bothered implementing month intervals")
        return (
            relativedelta(months=int(interval[:-2])),
            max_period[interval],
            timedelta(months=int(interval[:-1])),
        )
    else:
        # got an unknown interval
        raise ValueError(f"Unknown interval type {interval}")


mocker = Mocker(
    data_source=YFinanceFeeder(),
    real_end=end_dt,
)

# technology sector
tech_mocker = Mocker(end_dt)

interval_delta, max_range, tick = get_interval_settings(interval=interval)
bars_start = datetime.now() + timedelta(days=-max_range)
bars_end = datetime.now()

# todo: change this to use the actual requested start date
# todo: get a better signal
# todo: why is the EMA always <200? even in assets that seem like they're doing alright
while True:
    df = mocker.get_bars(
        symbol=symbol,
        start=bars_start,
        end=current_dt,
        interval=interval,
    )
    if len(df) == 0:
        new_range = max_range

    while len(df) == 0:
        new_range -= 1
        if new_range == 0:
            print(f"New range got to zero?!")
            exit()
        print(f"Bad start date. Trying again with range {new_range}")
        mocker = Mocker(
            data_source=YFinanceFeeder(),
            real_end=end_dt,
        )
        bars_start = datetime.now() + timedelta(days=-new_range)
        df = mocker.get_bars(
            symbol=symbol,
            start=bars_start,
            end=current_dt,
            interval=interval,
        )

    df_output = df.copy(deep=True)

    if position_taken == False:
        # STEP 0: GET EMA FOR THE MARKET AS A WHOLE
        # todo

        # STEP 1: are we in a bull market?
        # todo

        # STEP 2: has there been a crossover since our last run?
        macd = btalib.macd(df)
        df_output["macd_macd"] = macd["macd"]
        df_output["macd_signal"] = macd["signal"]
        df_output["macd_histogram"] = macd["histogram"]
        df_output["macd_crossover"] = False
        df_output["macd_signal_crossover"] = False
        df_output["macd_above_signal"] = False
        df_output["macd_cycle"] = None

        # loops looking for three things - macd-signal crossover, signal-macd crossover, and whether macd is above signal
        cycle = None

        for d in df_output.index:
            # start with crossover search
            # convert index to a datetime so we can do a delta against it                           ****************
            last_key = d - interval_delta
            # previous key had macd less than or equal to signal
            if df_output["macd_macd"].loc[d] > df_output["macd_signal"].loc[d]:
                # macd is greater than signal - crossover
                df_output.at[d, "macd_above_signal"] = True
                try:
                    if (
                        df_output["macd_macd"].loc[last_key]
                        <= df_output["macd_signal"].loc[last_key]
                    ):
                        cycle = "blue"
                        df_output.at[d, "macd_crossover"] = True

                except KeyError as e:
                    # ellipsis because i don't care if i'm missing data (maybe i should...)
                    ...

            if df_output["macd_macd"].loc[d] < df_output["macd_signal"].loc[d]:
                # macd is less than signal
                try:
                    if (
                        df_output["macd_macd"].loc[last_key]
                        >= df_output["macd_signal"].loc[last_key]
                    ):
                        cycle = "red"
                        df_output.at[d, "macd_signal_crossover"] = True

                except KeyError as e:
                    # ellipsis because i don't care if i'm missing data (maybe i should...)
                    ...

            df_output.at[d, "macd_cycle"] = cycle
            # df_output["macd_cycle"].loc[d] = cycle

        # STEP 3: DID WE FIND A SIGNAL? BAIL OUT IF NOT
        window_start = df_output.index[-1]

        if (
            len(
                df_output.loc[
                    (df_output.macd_crossover == True)
                    & (df_output.macd_macd < 0)
                    & (df_output.index == current_dt)
                ]
            )
            == 0
        ):
            # no signal
            print(f"{current_dt} - no signal\r", end="")
        else:
            # STEP 4: PREP FOR AN ORDER!
            crossover_index = df_output.loc[
                (df_output.macd_crossover == True) & (df_output.macd_macd < 0)
            ].index[-1]
            crossover_record = df_output.loc[[crossover_index]]
            crossover_index_position = df_output.index.get_loc(crossover_index)

            entry_unit = df_output.Close.iloc[-1]
            # first start with calculating risk and stop loss
            # stop loss is based on the lowest unit price since this cycle began
            # first find the beginning of this cycle, which is when the blue line crossed under the red line
            blue_cycle_start = df_output.loc[
                (df_output["macd_cycle"] == "blue")
                & (df_output.index < crossover_index)
            ].index[-1]
            # then get the lowest close price since the cycle began
            stop_unit = df_output.loc[blue_cycle_start:crossover_index].Close.min()
            stop_unit_date = df_output.loc[
                blue_cycle_start:crossover_index
            ].Close.idxmin()

            original_stop = stop_unit

            # and for informational/confidence purposes, hold on to the intervals since this happened
            intervals_since_stop = len(df_output.loc[stop_unit_date:])

            # calculate other order variables
            trade_date = df_output.index[-1]
            steps = 1
            units = floor(capital / entry_unit)
            risk_unit = entry_unit - stop_unit
            original_risk_unit = risk_unit
            risk_value = units * risk_unit
            target_profit = PROFIT_TARGET * risk_unit
            target_price = entry_unit + target_profit

            leftover_capital = capital - (units * entry_unit)

            order = Purchase(unit_quantity=units, unit_price=entry_unit)

            print(f"{crossover_index}: Found signal")
            print(f"Strength:\t\tNot sure how I want to do this yet")
            print(f"MACD:\t\t\t{crossover_record.macd_macd.values[0]}")
            print(f"Signal:\t\t\t{crossover_record.macd_signal.values[0]}")
            print(f"Histogram:\t\t{crossover_record.macd_histogram.values[0]}")
            print(f"Capital:\t\t{clean(capital)}")
            print(f"Units to buy:\t\t{clean(units)}")
            print(f"Entry point:\t\t{clean(entry_unit)}")
            print(f"Stop loss:\t\t{clean(stop_unit)}")
            print(f"Cycle began:\t\t{intervals_since_stop} intervals ago")
            print(
                f"Unit risk:\t\t{clean(risk_unit)} ({round(risk_unit/entry_unit*100,1)}% of unit cost)"
            )
            print(
                f"Unit profit:\t\t{clean(target_profit)} ({round(target_profit/entry_unit*100,1)}% of unit cost)"
            )
            print(
                f"Target price:\t\t{clean(target_price)} ({round(target_price/capital*100,1)}% of capital)"
            )

            position_taken = True

    else:
        # we are in sell/stop loss mode
        last_close = df.Close.iloc[-1]

        # stop loss!
        if last_close <= stop_unit:
            order.sell_units(sell_price=last_close)
            capital = order.get_returns() + leftover_capital
            if stop_unit > original_stop:
                wins += 1
                trade_won = "WON"
            else:
                losses += 1
                trade_won = "LOST"

            win_rate = wins / (wins + losses) * 100

            trade_duration = df_output.index[-1] - trade_date
            print(
                f"Trade ran for {trade_duration.days} days and {trade_won} and hit stop loss ({clean(last_close)} vs {clean(stop_unit)}). Win rate {round(win_rate,1)}%, balance {clean(capital)} (gain/loss of {clean(capital-starting_capital)})"
            )
            print(f"======================")
            position_taken = False

        # hit win point, take 50% of winnings
        elif last_close >= target_price:
            held = order.get_units()
            units_to_sell = floor(held * 0.50)
            order.sell_units(sell_price=last_close, unit_quantity=units_to_sell)

            # and update stop loss
            if stop_unit < last_close * 0.99:
                stop_unit = last_close * 0.99

            steps += 1  #                                                                    ############## BADLY NAMED VARIABLE
            risk_unit = original_risk_unit * steps
            target_profit = PROFIT_TARGET * risk_unit
            target_price = entry_unit + target_profit

            print(
                f"{df_output.index[-1]} Met target price, new target price {clean(target_price)}, new stop price {clean(stop_unit)}"
            )

        # hit win
        elif last_close >= (entry_unit + risk_unit):
            # sell 25%
            held = order.get_units()
            units_to_sell = floor(held * 0.25)
            order.sell_units(sell_price=last_close, unit_quantity=units_to_sell)

            # and update stop loss
            if stop_unit < last_close * 0.99:
                stop_unit = last_close * 0.99
                #                                                     ************ HARDCODED BE SMARTER AND USE MACD DIFF

            steps += 1
            risk_unit = original_risk_unit * steps
            target_profit = PROFIT_TARGET * risk_unit
            target_price = entry_unit + target_profit

            print(f"Step #{steps}")
            print(
                f"{df_output.index[-1]} Met target price, new target price {clean(target_price)}, new stop price {clean(stop_unit)}"
            )
        else:
            print(
                f"{current_dt} nothing happened, target price {clean(target_price)} / stop loss {clean( stop_unit)} holds vs last close of {clean(last_close)}\r",
                end="",
            )

    pauses = ["2022-02-23 11:00:00", "2022-03-08 16:00:00", "2022-03-11 15:00:00"]
    p_dates = []
    for p in pauses:
        p_dates.append(datetime.fromisoformat(p))

    if current_dt in p_dates:
        print("banana")

    # FAST_MODE means just jump to the next entry in the df, as opposed to ticking the clock (even at times when a market is shut so there'll be no data)
    if FAST_MODE:
        current_dt = mocker.get_next()
        if current_dt == False:
            current_dt = datetime.now() + relativedelta(minutes=100)
    else:
        current_dt += tick

    if current_dt > datetime.now():
        # BUG this if statement also need to include the value of currently held units
        if position_taken:
            current_holding_value = order.get_held_value()
        else:
            current_holding_value = 0
        if (capital + current_holding_value) <= starting_capital:
            outcome_text = "gained"
        else:
            outcome_text = "lost"

        try:
            win_rate = round(wins / (wins + losses) * 100, 1)
            loss_rate = 100 - win_rate
        except ZeroDivisionError:
            win_rate = 0
            loss_rate = 0

        print(f"================")
        print(f"Simulation complete on {current_dt}")
        print(f"Starting capital:\t{clean(starting_capital)}")
        print(f"Ending capital:\t\t{clean(capital)}")
        print(
            f"Change:\t\t\t{clean(capital-starting_capital)} ({outcome_text} capital)"
        )
        print(f"% change:\t\t{round((capital/starting_capital*100)-100,1)}")
        print(f"Total trades:\t\t{wins+losses}")
        print(f"Wins:\t\t\t{wins} ({win_rate}%)")
        print(f"Losses:\t\t\t{losses} ({loss_rate}%)")

        break

    """capital = 2000
    entry_unit = 0.35
    units = capital / entry_unit = 700
    stop_unit = 0.307
    risk = entry_unit - stop_unit 0.35 - 0.30 = 0.05
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
    entry_unit = whatever the latest close price was
    units = capital / entry_unit = 700
    stop_unit = what is the lowest price its been since the last time the price was the same as now? = 0.307
    risk = entry_unit - stop_unit 0.35 - 0.30 = 0.05
    risk_value = capital * stop_unit = 2000 * 0.30 = 600
    profit_target = 1.5 * risk_value
    """
    # GRAVEYARD
    # get index position
    # print(f"{df_output.index.get_loc(d)} and previous is {df_output.index.get_loc(d)-1}")
    # df_output.ema.loc[current_dt:]
    # data frame since last run
    # df_last_run = df_output.loc[df_output.index <= current_dt].loc[
    #    df_output.macd_macd > df_output.macd_signal
    # ]
    # this holds the date of the most recent crossover
    # last_crossover = df_output.index[-1]
    # df.at["2022-03-29 11:30:00-04:00", "Close"] = 10000
    # ssorted_df = df_output.sort_values(by=["Close"]) #inplace=true
    # index = df_raw.index[-1]
    # print(f'{index} Current sma: {df_raw["sma"].loc[index]}')

    # df_graph = df_output.loc[nearest_close["lower"] :].copy()
    # df_graph.plot(y="Close", kind="line")
    # plt.show()

    # last_run = datetime.fromisoformat(start)
    #            last_crossover = df_output.loc[df_output.macd_crossover == True]
    #            last_crossover_index_position = df_output.index.get_loc(
    #                last_crossover.index[-1]
    #            )
    #            intervals_between_crossovers = (
    #                last_crossover_index_position - crossover_index_position
    #            )
    """max_period = {
        "1m": 7,
        "2m": 60,
        "5m": 60,
        "15m": 60,
        "30m": 60,
        "60m": 729,
        "90m": 60,
        "1h": 729,
        "1d": 10000,
        "5d": 10000,
        "1wk": 10000,
        "1mo": 10000,
        "3mo": 10000,
    }
    """
