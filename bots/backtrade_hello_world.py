import btalib
from numpy import NaN
from feeder import Mocker
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import matplotlib.pyplot as plt

PROFIT_TARGET = 1.5


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
        "60m": 729,
        "90m": 60,
        "1h": 729,
        "1d": 10000,
        "5d": 10000,
        "1wk": 10000,
        "1mo": 10000,
        "3mo": 10000,
    }

    if interval in minutes_intervals:
        return relativedelta(minutes=int(interval[:-1])), max_period[interval]
    elif interval == "1h":
        return relativedelta(hours=int(interval[:-1])), max_period[interval]
    elif interval == "1d" or interval == "5d":
        return relativedelta(days=int(interval[:-1])), max_period[interval]
    elif interval == "1wk":
        return relativedelta(weeks=int(interval[:-2])), max_period[interval]
    elif interval == "1mo" or interval == "3mo":
        return relativedelta(months=int(interval[:-2])), max_period[interval]
    else:
        # got an unknown interval
        raise ValueError(f"Unknown interval type {interval}")


def find_neighbours(value, df, colname, ignore_index):
    return_dict = {}

    exactmatch = df[df[colname] == value]
    exactmatch.drop([ignore_index], axis=0, inplace=True)
    if not exactmatch.empty:
        return_dict["higher"] = exactmatch.index
        return_dict["lower"] = exactmatch.index
        return return_dict
    else:
        return_dict["higher"] = df[df[colname] > value][colname].idxmin()
        return_dict["lower"] = df[df[colname] < value][colname].idxmax()
        return return_dict


capital = 2000
symbol = "AAPL"
interval = "1d"
start = "2021-01-01"
current = "2021-01-01"
end = "2022-04-15"
# start = "2021-09-01T00:00:00+10:00"
# current = "2021-09-01T00:00:00+10:00"
# end = "2022-04-15T00:00:00+10:00"
position_taken = False

start_dt = datetime.fromisoformat(start)
current_dt = datetime.fromisoformat(current)
end_dt = datetime.fromisoformat(end)

losses = 0
wins = 0

mocker = Mocker(
    real_end=end_dt,
)

# technology sector
tech_mocker = Mocker(end_dt)

interval_delta, max_range = get_interval_settings(interval=interval)
bars_start = datetime.now() + timedelta(days=-max_range)
bars_end = datetime.now()

# todo: change this to use the actual requested start date
# todo: get a better signal
# todo: why is the EMA always <200? even in assets that seem like they're doing alright
while True:

    # STEP 0: GET EMA FOR THE MARKET AS A WHOLE
    # get tech stock for use as EMA signal
    df_tech = tech_mocker.get_bars(
        # todo: dynamic lookup of market comparison                                             *****************
        symbol="XLK",
        # todo: fix hardcoded days                                                              *****************
        start=bars_start,
        # i dunno about having these two dataframes with different end dates                    *****************
        end=current_dt,
        interval=interval,
    )

    df = mocker.get_bars(
        symbol=symbol,
        start=bars_start,
        end=current_dt,
        interval=interval,
    )
    df_output = df.copy(deep=True)

    if position_taken == False:
        # STEP 1: are we in a bull market?
        # this should be done against the whole market but i don't know how to do that for crypto stocks?
        ema = btalib.ema(df)
        df_output["ema"] = ema.df

        # todo: delete these lines once you work out what kind of market this is                    ******************
        # if ema.df["ema"].iloc[-1] < 200:
        #    print(f"Breaking, not a bull market)
        #    break

        # STEP 2: has there been a crossover since our last run?
        macd = btalib.macd(df)
        df_output["macd_macd"] = macd["macd"]
        df_output["macd_signal"] = macd["signal"]
        df_output["macd_histogram"] = macd["histogram"]
        df_output["macd_crossover"] = False
        df_output["macd_above_signal"] = False

        # todo: take out this fudged attempt at showing crossover                                   *******************
        # fake_loc = df_output.index.get_loc("2022-03-22 10:00:00-04:00")
        # df_output.macd_histogram.iloc[fake_loc] = -1

        # loops looking for two things - crossover and whether macd is above signal
        for d in df_output.index:
            # start with crossover search
            # convert index to a datetime so we can do a delta against it                           ****************
            last_key = d - interval_delta
            # previous key had macd less than or equal to signal
            if df_output["macd_macd"].loc[d] > df_output["macd_signal"].loc[d]:
                # macd is greater than signal
                df_output["macd_above_signal"].loc[d] = True

                try:
                    if (
                        df_output["macd_macd"].loc[last_key]
                        <= df_output["macd_signal"].loc[last_key]
                    ):
                        df_output["macd_crossover"].loc[d] = True

                except KeyError as e:
                    # ellipsis because i don't care if i'm missing data (maybe i should...)
                    ...

        # STEP 3: DID WE FIND A SIGNAL? BAIL OUT IF NOT
        if (
            len(
                df_output.loc[df_output.macd_histogram > 0].loc[
                    df_output.macd_crossover == True
                ]
            )
            == 0
        ):
            # no signal
            print("breaking, didn't find a signal")
            break

        # STEP 3.5: DID WE FIND MORE THAN ONE SIGNAL?
        # todo: just grab the most recent one

        crossover_index = (
            df_output.loc[df_output.macd_histogram > 0]
            .loc[df_output.macd_above_signal == True]
            .index[-1]
        )
        crossover_record = df_output.loc[[crossover_index]]
        crossover_index_position = df_output.index.get_loc(crossover_index)

        # STEP 4: PREP FOR AN ORDER!
        entry_unit = crossover_record.Close.values[0]
        # first start with calculating risk and stop loss
        # stop loss is based on the lowest unit price since this cycle began
        # first need to get the last time the asset closed at this price
        nearest_close = find_neighbours(
            entry_unit,
            df_output.iloc[: (crossover_index_position + 1)],
            "Close",
            crossover_index,
        )
        stop_unit = df_output.Close.loc[nearest_close["lower"] : crossover_index].min()

        units = capital / entry_unit
        risk_unit = entry_unit - stop_unit
        risk_value = units * risk_unit
        profit_target = PROFIT_TARGET * risk_value

        print(f"Found signal")
        print(f"Strength:\t\tNot sure how I want to do this yet")
        print(f"Capital:\t\t{clean(capital)}")
        print(f"Units to buy:\t\t{clean(units)}")
        print(f"Entry point:\t\t{clean(entry_unit)}")
        print(f"Stop loss:\t\t{clean(stop_unit)}")
        print(
            f"Days since stop loss:\t{(datetime.now() - nearest_close['lower'].tz_localize(None)).days}"
        )
        print(f"Risk:\t\t\t{clean(risk_value)} ({round(risk_value/capital,1)*100}%)")
        print(
            f"Reward:\t\t\t{clean(profit_target)} ({round(profit_target/capital,1)*100}%)"
        )

        position_taken = True
    else:
        # we are in sell/stop loss mode
        last_close = df.Close.iloc[-1]
        print(f"Checking {df.index[-1]}...")
        # first check to see if last close is below stop loss

        if last_close <= stop_unit:
            losses += 1
            print(
                f"STOP LOSS TRIGGERED - close {last_close} vs stop loss {stop_unit}. {losses} losses so far"
            )
            position_taken = False
        elif last_close == risk_unit:
            # move 25%
            wins += 1
            print(
                f"Move 25% and move stop loss and set profit 2 * risk. {wins} wins so far"
            )
            position_taken = False

    current_dt += timedelta(days=1)
    print(current_dt)

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
    stop loss = below the pullback of the trend (i think find the last time red > blue)
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
