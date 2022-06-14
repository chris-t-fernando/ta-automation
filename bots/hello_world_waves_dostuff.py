from numpy import NaN
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pytz

from broker_back_test import BackTestAPI
import utils

portfolio_df = pd.read_csv("portfolio_df.csv")

total_profit = 0
plays = 0
wins = 0
losses = 0
skipped = 0

state = "no position"
position_taken_ignore_stop_loss = 0

for i in portfolio_df.index[100:]:
    this_timestamp = portfolio_df.timestamp.iloc[i]
    this_value = portfolio_df.portfolio_value.iloc[i]
    this_diff = portfolio_df.portfolio_diff.iloc[i]
    this_diff_pct = portfolio_df.portfolio_diff_pct.iloc[i]
    this_sma = portfolio_df.sma.iloc[i]

    if state == "no position" and this_sma != NaN:
        if this_diff_pct > this_sma:
            # take position
            state = "waiting stop loss"
            position_taken_ignore_stop_loss = 2
            play_starting_value = this_value
            position_taken_at = i
            plays+=1
            stop_loss_updates = 0
            print(f"{this_timestamp}\tplay #{plays} position taken at {this_value:,.4f} waiting stop loss")
    
    elif state == "waiting stop loss":
        position_taken_ignore_stop_loss -= 1
        if position_taken_ignore_stop_loss == 0:
            if play_starting_value > this_value:
                # already hit stop loss
                
                skipped += 1
                print(f"{this_timestamp}\timmediately hit stop loss since last value {this_value:,.4f} is lower than starting value {play_starting_value:,.4f}")
                state = "no position"
                del play_starting_value
            else:
                # set stop loss
                
                play_stop_loss = play_starting_value
                #play_starting_value = this_value
                state = "position taken"
                print(f"{this_timestamp}\tstop loss set to {play_starting_value:,.4f}, waiting stop loss")
    
    elif state == "position taken":
        pos_duration = i - position_taken_at
        # first check if stop loss is hit
        if play_stop_loss > this_value:
            # triggered stop loss
                state = "no position"
                
                profit = this_value - play_starting_value
                if profit > 0:
                    wins += 1
                else:
                    losses += 1
                
                total_profit += profit

                print(f"{this_timestamp}\tPlay {plays} profit {profit}. After {pos_duration} periods and {stop_loss_updates} stop loss updates, hit stop loss {this_value:,.4f} vs {play_stop_loss:,.4f}")

                del stop_loss_updates
                del play_starting_value
                del play_stop_loss

        # next generate new stop loss
        else:
            profit = this_value - play_starting_value
            if profit < 0:
                print("banana")
            profit_95 = profit*.5
            new_stop_loss = play_starting_value + profit_95
            if new_stop_loss > play_stop_loss:
                #print(f"{this_timestamp}\tafter {pos_duration} changing stop loss from {play_stop_loss:,.4f} to {new_stop_loss:,.4f}")
                stop_loss_updates += 1
                play_stop_loss = new_stop_loss
                
print("**********")
print(f"Total plays: {plays}")
print(f"Total profit: {total_profit}")
print(f"Wins: {wins}")
print(f"Skipped: {skipped}")
print(f"Losses: {losses}")
print("**********")

"""
if we're out
-if pct crosses over sma100
--move to buy in

if we've bought in
-initial stop loss should be buy-in price after 10 minutes
-after that stop loss should be 10% of gain during this play
"""
