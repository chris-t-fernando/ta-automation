from numpy import NaN
from symtable import Symbol
from itradeapi import (
    ITradeAPI,
    MARKET_BUY,
    MARKET_SELL,
    LIMIT_BUY,
    LIMIT_SELL,
    STOP_LIMIT_BUY,
    STOP_LIMIT_SELL,
UnknownSymbolError,
 DelistedAssetError,
 UntradeableAssetError,
 MalformedOrderResult,
 ZeroUnitsOrderedError,
 ApiRateLimitError,
 BuyImmediatelyTriggeredError
)

import logging
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

import time
import utils
import sample_symbols
import notification_services
from parameter_stores import Ssm
from broker_alpaca import AlpacaAPI
import yfinance as yf
import pytz

log_wp = logging.getLogger("tides")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("tides.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)

_PREFIX = "tabot"
PATH_PAPER_ALPACA_API_KEY = f"/{_PREFIX}/paper/alpaca/api_key"
PATH_PAPER_ALPACA_SECURITY_KEY = f"/{_PREFIX}/paper/alpaca/security_key"
slack_bot_key_path = f"/{_PREFIX}/slack/bot_key"
slack_channel_path=f"/{_PREFIX}/paper/slack/announcements_channel"

record_interval = relativedelta(minutes=5)

benchmark = [
    {"symbol":"ETH-USD","quantity":1},
    {"symbol":"ALGO-USD","quantity":1014},
    {"symbol":"SHIB-USD","quantity":61200000},
    {"symbol":"DOGE-USD","quantity":8923},
    {"symbol":"SOL-USD","quantity":16},
    {"symbol":"MATIC-USD","quantity":1280},
    {"symbol":"AVAX-USD","quantity":31},
]

class RecordOutOfBoundsError(Exception):...

class MarketData():
    def __init__(self, yf_symbol:str, interval:str="5m"):
        self.yf_symbol = yf_symbol
        self.interval = interval
        self.bars = self.get()


    def merge_bars(bars, new_bars):
        return pd.concat([bars, new_bars[~new_bars.index.isin(bars.index)]])

    def round_time(self, date: pd.Timestamp, interval=5):
        minutes = (date.minute % interval) * 60
        seconds = date.second
        total_seconds = minutes + seconds
        
        interval_seconds = interval * 60
        interval_midpoint = interval_seconds / 2
        
        if total_seconds < interval_midpoint:
            # round down
            delta = -relativedelta(seconds=total_seconds)
            
        else:
            # round up
            padding = interval_seconds - total_seconds
            delta = relativedelta(seconds = padding)

        rounded_timestamp = date + delta
        return rounded_timestamp


    def get(self, start: pd.Timestamp=None, end: pd.Timestamp=None):
        cache_miss = False

        if not hasattr(self, "bars") or len(self.bars) == 0:
            cache_miss = True
            log_wp.debug(f"Cache miss - bars len 0")
            
            start_date_unaware = datetime.now() - relativedelta(days=59)
            start_date_melbourne = start_date_unaware.replace(tzinfo=pytz.timezone("Australia/Melbourne"))

            # if there's no - then assume its NYSE, else assume its crypto
            if self.yf_symbol.find("-") == -1:
                tz = "US/Eastern"
            else:
                tz = "UTC"

            start_date = start_date_melbourne.astimezone(pytz.timezone(tz))
            rounded_start = self.round_time(start_date)
            rounded_end = rounded_start + relativedelta(days=59)

            query_start = rounded_start

            self.bars = pd.DataFrame()

        # has a bars attribute so its safe to inspect it
        else:
            query_start = self.bars.index[-1]
            if start == None:
                rounded_start = self.bars.index[0]
            else:
                rounded_start = self.round_time(start)
                if rounded_start < self.bars.index[0]:
                    cache_miss = True
                    log_wp.debug(f"Cache miss - start earlier than bars")
                elif rounded_start > self.bars.index[-1]:
                    cache_miss = True
                    log_wp.debug(f"Cache miss - start later than bars")

            if end == None:
                rounded_end = self.bars.index[-1]
            else:
                rounded_end = self.round_time(end)
                if rounded_end > self.bars.index[-1]:
                    cache_miss = True
                    log_wp.debug(f"Cache miss - end later than bars")
        
        if cache_miss:
            log_wp.debug(f"Querying to update cache")
            new_bars = yf.Ticker(self.yf_symbol).history(
                start=query_start,
                interval=self.interval,
                actions=False,
                debug=False,
            )


            self.bars = pd.concat([self.bars, new_bars[~new_bars.index.isin(self.bars.index)]]).sort_index()

        # hackity hack - we just changed the length/last record in the dataframe
        if end == None:
            rounded_end = self.bars.index[-1]

        return_records = self.bars.loc[(self.bars.index >= rounded_start) & (self.bars.index <= rounded_end)]
        return return_records


def analyse_interval(starting_value):
    start_date = datetime.now() - relativedelta(minutes=(5*120))

    portfolio_values = []

    columns = [
        "timestamp",
        "portfolio_value",
        "portfolio_diff",
        "portfolio_diff_pct",
    ]
    # set up the destination dataframe
    portfolio_df = pd.DataFrame(columns=columns)

    # hold on to the bars
    for holding in benchmark:
        symbol = holding["symbol"]
        quote = yf.Ticker(symbol).history(
            start=start_date,
            interval="5m",
            actions=False,
            debug=False,
        )
        quote["portfolio_value"] = quote["Close"] * holding["quantity"]
        holding["bars"] = quote

    #while current_record <= holding["bars"].index[-1]:
    #for this_index in holding["bars"].index:
    #bar_length = len(holding["bars"].index)
    #for i in range(0, bar_length):
    analyse_date = holding["bars"].index[-110]
    analyse_end = holding["bars"].index[-1]
    for holding in benchmark:
        this_end = holding["bars"].index[-1]
        if this_end > analyse_end:
            analyse_end = this_end
    
    #now analyse_end is the latest record to finish
    while analyse_date <= analyse_end:
        portfolio_value = 0
        for holding in benchmark:
            try_date = analyse_date
            attempts = 0
            while True:
                try:
                    portfolio_value += holding["bars"]["portfolio_value"].loc[try_date]
                    break
                except KeyError as e:
                    attempts +=1
                    if attempts > 5:
                        raise

                    try_date = try_date - record_interval
                    #log_wp.log(9, f"{holding['symbol']} does not have a record for {try_date}, falling back on {fallback_date}")
                    #portfolio_value += holding["bars"]["portfolio_value"].loc[fallback_date]
                    # if we're missing more than one cycle of data, then I give up TODO this means no mixing crypto with normies

        diff = portfolio_value - starting_value
        diff_pct = portfolio_value / starting_value
        
        new_row = pd.DataFrame(
            {
                "timestamp":analyse_date,
                "portfolio_value":portfolio_value,
                "portfolio_diff":diff,
                "portfolio_diff_pct":diff_pct,
            },
            index=[1],
            columns=columns,
        )
        portfolio_df = pd.concat([portfolio_df, new_row], ignore_index=True)
        analyse_date += record_interval

    sma = []
    for index in portfolio_df.index:
        if index < 100:
            sma.append(NaN)
        else:
            location = portfolio_df.index.get_loc(index)
            sma.append(portfolio_df.iloc[-location:].portfolio_diff_pct.mean())

    portfolio_df["sma"] = pd.Series(sma).values
    return portfolio_df

def main(args):
    store = Ssm()
    alpaca_api_key = store.get(path=PATH_PAPER_ALPACA_API_KEY)
    alpaca_security_key = store.get(path=PATH_PAPER_ALPACA_SECURITY_KEY)

    api = AlpacaAPI(
        alpaca_key_id=alpaca_api_key,
        alpaca_secret_key=alpaca_security_key
    )

    slack_bot_key = store.get(path=slack_bot_key_path)
    slack_announcements_channel = store.get(path=slack_channel_path)
    notification_service = notification_services.Slack(
        bot_key=slack_bot_key, channel=slack_announcements_channel
    )

    starting_value = 4086

    # okay so we've set our starting point, now keep grabbing data and checking if we should buy in
    position_taken = False
    while True:
        #print(f"Processing...")
        porfolio_analysis = analyse_interval(starting_value)
        if not position_taken:
            this_sma = round(porfolio_analysis.sma.iloc[-1],3)
            this_diff_pct = round(porfolio_analysis.portfolio_diff_pct.iloc[-1],3)
            #this_sma = porfolio_analysis.sma.iloc[-1]
            #this_diff_pct = porfolio_analysis.portfolio_diff_pct.iloc[-1]
            if this_diff_pct > this_sma:
                # the latest diff pct is better than the sma100 diff pct - its getting better, and this is our buy signal
                buy_value = 0
                for asset in benchmark:
                    symbol = asset["symbol"].replace("-", "")
                    units_to_buy = asset["quantity"]
                    buy = api.buy_order_market(symbol, units_to_buy)
                    buy_value += buy.filled_total_value
                stop_loss = porfolio_analysis.portfolio_value.iloc[-1]
                position_taken = True
                message = f"Took position valued at {buy_value}. Last close {this_diff_pct} > SMA {this_sma} value/stop loss of {stop_loss:,.4f}"
                print(message)
                notification_service.send(message)
            else:
                log_wp.debug(f"No crossover found (last close {this_diff_pct}, SMA {this_sma})")

        else:
            # first check if stop loss has been hit, and if so then liquidate
            current_value = porfolio_analysis.portfolio_value.iloc[-1]
            if current_value < stop_loss:
                # stop loss hit
                sell_value = 0
                for asset in benchmark:
                    symbol = asset["symbol"].replace("-", "")
                    units_to_sell = asset["quantity"]
                    sell = api.sell_order_market(symbol, units_to_sell)
                    sell_value += sell.filled_total_value

                profit = sell_value - buy_value
                message = f"Hit stop loss of {stop_loss:,.4f} vs stop loss of {stop_loss:,.4f}. Buy value was {buy_value}, sell value was {sell_value}, profit was {profit}"
                print(message)
                notification_service.send(message)
                del stop_loss
                position_taken = False
            else:
                profit = current_value - stop_loss
                if profit < 0:
                    print("banana")
                profit_50 = profit*.5
                new_stop_loss = stop_loss + profit_50
                if new_stop_loss > stop_loss:
                    message = f"Changing stop loss from {stop_loss:,.4f} to {new_stop_loss:,.4f}"
                    print(message)
                    notification_service.send(message)
                    stop_loss = new_stop_loss

        pause = utils.get_pause("5m")
        log_wp.debug(f"Finished analysing, sleeping for {round(pause,0)}s")
        time.sleep(pause)

    print("banana")


if __name__ == "__main__":
    #a= MarketData("SOL-USD")
    #a.bars = a.bars.loc[(a.bars.index < pd.Timestamp("2022-06-10 23:00:00-04:00"))]
    #a.get(start=pd.Timestamp("2022-06-14 12:32:39-04:00"))
    args = None
    
    main(args)
