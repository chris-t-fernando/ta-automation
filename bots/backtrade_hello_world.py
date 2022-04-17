from feeder import Mocker, YFinanceFeeder
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from purchase import Purchase
from math import floor


def clean(number):
    number = round(number, 2)
    return "{:,}".format(number)


# internal variables, not inputs
position_taken = False
losses = 0
wins = 0
skipped_trades = 0
skipped_trades_sma = 0

## INPUTS AND CONSTANTS
PROFIT_TARGET = 1.5
FAST_MODE = True  # don't use tick to get next period, use next entry in dataframe
capital = 2000
starting_capital = capital
symbol = "ADA-USD"
interval = "15m"

start = "2022-03-18"
current = start
end = "2022-04-13"

# setup
start_dt = datetime.fromisoformat(start)
current_dt = datetime.fromisoformat(current)
end_dt = datetime.fromisoformat(end)

backtest_source = Mocker(
    data_source=YFinanceFeeder(),
    real_end=end_dt,
)

interval_delta, max_range, tick = backtest_source.get_interval_settings(
    interval=interval
)
bars_start = datetime.now() + timedelta(days=-max_range)
bars_end = datetime.now()


# todo: get a better signal
# todo: why is the EMA always <200? even in assets that seem like they're doing alright
while True:
    df = backtest_source.get_bars(
        symbol=symbol,
        start=bars_start,
        end=current_dt,
        interval=interval,
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

    if position_taken == False:
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
                    & (df_output.index == current_dt)
                ]
            )
            == 0
        ):
            # no signal
            print(f"\r{current_dt} - no signal", end="")
        else:
            # is SMA good?
            if df_output.iloc[-1].sma_200 > df_output.iloc[-5].sma_200:
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

                print(f"\n{crossover_index}: Found signal")
                print(f"Strength:\t\tNot sure how I want to do this yet")
                print(f"MACD:\t\t\t{crossover_record.macd_macd.values[0]}")
                print(f"Signal:\t\t\t{crossover_record.macd_signal.values[0]}")
                print(f"Histogram:\t\t{crossover_record.macd_histogram.values[0]}")
                print(f"Capital:\t\t${clean(capital)}")
                print(f"Units to buy:\t\t{clean(units)} units")
                print(f"Entry point:$\t\t{clean(entry_unit)}")
                print(f"Stop loss:$\t\t{clean(stop_unit)}")
                print(f"Cycle began:\t\t{intervals_since_stop} intervals ago")
                print(
                    f"Unit risk:\t\t${clean(risk_unit)} ({round(risk_unit/entry_unit*100,1)}% of unit cost)"
                )
                print(
                    f"Unit profit:\t\t${clean(target_profit)} ({round(target_profit/entry_unit*100,1)}% of unit cost)"
                )
                print(
                    f"Target price:\t\t${clean(target_price)} ({round(target_price/capital*100,1)}% of capital)"
                )

                position_taken = True
            else:
                print(
                    f"\r{current_dt} - found signal but SMA is trending downward - avoiding this trade                                   ",
                    end="",
                )
                skipped_trades += 1
                skipped_trades_sma += 1

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
            trade_iteration_count = df_output.index.get_loc(
                df_output.index[-1]
            ) - df_output.index.get_loc(trade_date)
            print(
                f"\rTrade ran for {trade_iteration_count} intervals ({trade_duration.days} days) and {trade_won}. Increased stop loss {steps} times before hitting stop loss ({clean(last_close)} vs {clean(stop_unit)}). Win rate {round(win_rate,1)}%, balance {clean(capital)} (gain/loss of {clean(capital-starting_capital)})",
                end="",
            )
            print(f"\n===============================================================")
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
                f"\r{df_output.index[-1]} Met target price, new target price {clean(target_price)}, new stop price {clean(stop_unit)}"
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

            # print(f"Step #{steps}")
            print(
                f"\r{df_output.index[-1]} Met target price, new target price {clean(target_price)}, new stop price {clean(stop_unit)}",
                end="",
            )
        else:
            print(
                f"\r{current_dt} nothing happened, target price {clean(target_price)} / stop loss {clean( stop_unit)} holds vs last close of {clean(last_close)}",
                end="",
            )

    # FAST_MODE means just jump to the next entry in the df, as opposed to ticking the clock (even at times when a market is shut so there'll be no data)
    if FAST_MODE:
        current_dt = backtest_source.get_next()
        if current_dt == False:
            current_dt = datetime.now() + relativedelta(minutes=100)
    else:
        current_dt += tick

    if current_dt > datetime.now():
        # BUG this if statement also need to include the value of currently held units
        if position_taken:
            current_holding_value = order.get_held_value(df_output["Close"].iloc[-1])
        else:
            current_holding_value = 0

        if (capital + current_holding_value) >= starting_capital:
            outcome_text = "gained"
        else:
            outcome_text = "lost"

        try:
            win_rate = round(wins / (wins + losses) * 100, 1)
            loss_rate = 100 - win_rate
        except ZeroDivisionError:
            win_rate = 0
            loss_rate = 0

        print(f"\n===============================================================")
        print(
            f"Backtrading of {symbol} between {start_dt} and {current_dt} using {interval} intervals complete"
        )
        print(f"Starting capital:\t${clean(starting_capital)}")
        print(f"Ending capital:\t\t${clean(capital)}")
        print(
            f"Change:\t\t\t${clean(capital-starting_capital)} ({outcome_text} capital)"
        )
        print(f"% change:\t\t{round((capital/starting_capital*100)-100,1)}%")
        print(f"Total trades:\t\t{wins+losses}")
        print(f"Wins:\t\t\t{wins} ({win_rate}%)")
        print(f"Losses:\t\t\t{losses} ({loss_rate}%)")
        print(f"Skipped:\t\t{skipped_trades} trades (low SMA: {skipped_trades_sma})")

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
