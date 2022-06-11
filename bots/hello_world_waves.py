from numpy import NaN
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pytz

from broker_back_test import BackTestAPI
import utils

record_interval = relativedelta(minutes=5)

holdings = [{"symbol":"BTC-USD","quantity":0.01722374},
    {"symbol":"ETH-USD","quantity":0.281983},
    {"symbol":"ADA-USD","quantity":449.32},
    {"symbol":"XRP-USD","quantity":494.84},
    {"symbol":"DOT-USD","quantity":21.3202},
    {"symbol":"SOL-USD","quantity":4.0831},
    {"symbol":"BNB-USD","quantity":0.52247},
    {"symbol":"MATIC-USD","quantity":119.11},
    {"symbol":"AVAX-USD","quantity":2.9144},
    {"symbol":"AXS-USD","quantity":3.4485},
    {"symbol":"ATOM-USD","quantity":6.1787}
    ]


for this_holding in holdings:
    this_holding["bars"] = utils.load_bars(this_holding["symbol"], bucket="mfers-tabot", key_base="symbol_data/5m/")

start = holdings[0]["bars"].index[-1]

current_record = pd.Timestamp('2022-05-10 07:30:00+00:00')

starting_value = 0
for this_holding in holdings:
    symbol = this_holding['symbol']
    close = this_holding['bars'].Close[current_record]
    quantity = this_holding['quantity']
    value = close * quantity
    starting_value += value

columns = [
    "timestamp",
    "portfolio_value",
    "portfolio_diff",
    "portfolio_diff_pct",
]
# set up the destination dataframe
portfolio_df = pd.DataFrame(columns=columns)


sma = []
while current_record < datetime.now().astimezone(pytz.utc):

    this_value = 0
    all_good = True
    for this_holding in holdings:
        try:
            symbol = this_holding['symbol']
            close = this_holding['bars'].Close[current_record]
            quantity = this_holding['quantity']
            value = close * quantity
            this_value += value
            #print(f"{symbol}\t{quantity}\t{close}\t{value}")
        except Exception as e:
            all_good = False

    if all_good:
        diff = this_value - starting_value
        diff_pct = this_value / starting_value
        #print(f"Value at {current_record} is {this_value:,.0f}\t{diff:,.0f}\t{diff_pct:,.2f}%")

        # work out sma

        new_row = pd.DataFrame(
            {
                "timestamp":current_record,
                "portfolio_value":this_value,
                "portfolio_diff":diff,
                "portfolio_diff_pct":diff_pct,
            },
            columns=columns,
            index=[0],
        )
        portfolio_df = pd.concat([portfolio_df, new_row], ignore_index=True)

        if len(portfolio_df) > 100:
            avg = portfolio_df.iloc[-100:].portfolio_diff_pct.mean()
            sma.append(avg)
        else:
            sma.append(NaN)
         
    current_record += record_interval

    #if len(portfolio_df) > 1000:
    #    break

portfolio_df["sma"] = pd.Series(sma).values
#df['new_col'] = pd.Series(mylist).values

print("banana")
