import btalib
from numpy import NaN
from feeder import Mocker
from datetime import timedelta, datetime
import pandas as pd

PROFIT_TARGET = 1.5


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


symbol = "AAPL"
interval = "30m"
# start = "2022-01-01"
# current = "2022-02-15"
# end = "2022-04-15"
start = "2022-03-01T00:00:00+10:00"
current = "2022-04-15T00:00:00+10:00"
end = "2022-03-31T00:00:00+10:00"


start_dt = datetime.fromisoformat(start)
current_dt = datetime.fromisoformat(current)
end_dt = datetime.fromisoformat(end)


mocker = Mocker(
    real_end=end_dt,
)

capital = 2000

# technology sector
tech_mocker = Mocker(end_dt)

# last_run = datetime.fromisoformat(start)

# todo: change this to use the actual requested start date
# todo: get a better signal
# todo: why is the EMA always <200? even in assets that seem like they're doing alright
while True:
    # get tech stock for use as EMA signal
    df_tech = tech_mocker.get_bars(
        symbol="XLK",
        start=datetime.now() + timedelta(days=-59),
        end=datetime.now(),
        interval=interval,
    )

    df = mocker.get_bars(
        symbol=symbol,
        start=datetime.now() + timedelta(days=-59),
        end=current_dt,
        interval=interval,
    )
    df_output = df.copy(deep=True)

    # STEP 1: are we in a bull market?
    # this should be done against the whole market but i don't know how to do that for crypto stocks?
    ema = btalib.ema(df)
    df_output["ema"] = ema.df
    # todo: delete these lines
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

    # todo: take out this fudged attempt at showing crossover
    fake_loc = df_output.index.get_loc("2022-03-22 10:00:00-04:00")
    df_output.macd_histogram.iloc[fake_loc] = -1

    # loops looking for two things - crossover and whether macd is above signal
    for d in df_output.index:
        # start with crossover search
        # convert index to a datetime so we can do a delta against it
        last_key = d - timedelta(minutes=30)
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
            df_output.loc[df_output.macd_histogram < 0].loc[
                df_output.macd_above_signal == True
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
        df_output.loc[df_output.macd_histogram < 0]
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

    ...

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

    """capital = 2000
    entry_unit = 0.35
    units = capital / entry_unit = 700
    stop_unit = 0.307
    risk = entry_unit - stop_unit 0.35 - 0.30 = 0.05
    risk_value = capital * stop_unit = 2000 * 0.30 = 600
    profit_target = 1.5 * risk_value"""

    # sma_fast = btalib.sma(df_raw, period=7)
    # sma_slow = btalib.sma(df_raw, period=28)
    # df_output["sma_fast"] = sma_fast.df
    # df_output["sma_slow"] = sma_slow.df
    # df_output["sma_comparison"] = df_output.sma_fast / df_output.sma_slow
    # df_output.loc[df_output.sma_fast / df_output.sma_slow, "sma_comparison"]
    # uo = btalib.ultimateoscillator(df_raw)
    # rsi = btalib.rsi(df_raw)

    current_dt += timedelta(days=1)
    # index = df_raw.index[-1]
    # print(f'{index} Current sma: {df_raw["sma"].loc[index]}')


mocker.get_bars(
    symbol="AAPL",
    start="2022-02-01T00:00:00+10:00",
    end="2022-03-02",
    interval="1d",
)


"""
entry as macd cross over
profit should be more than the risk taken on the trade
stop loss at the pullback of the trend

25% off the trade when the trade reachines 1 times the risk and move stop loss ot break even
then set a profit target of 2 times the original risk i took on the trade

profit = 1.5 times risk taken

1-2% per trade



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

print("banana")
