from numpy import NaN
from symbol_objects import SymbolCollection

import logging
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

import time
import utils
import notification_services
from parameter_stores import Ssm
from broker_alpaca import AlpacaAPI
import yfinance as yf

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
slack_channel_path = f"/{_PREFIX}/paper/slack/announcements_channel"

record_interval = relativedelta(minutes=5)

benchmark = [
    {"symbol": "ETH-USD", "quantity": 1},
    {"symbol": "ALGO-USD", "quantity": 1014},
    {"symbol": "SHIB-USD", "quantity": 61200000},
    {"symbol": "DOGE-USD", "quantity": 8923},
    {"symbol": "SOL-USD", "quantity": 16},
    {"symbol": "MATIC-USD", "quantity": 1280},
    {"symbol": "AVAX-USD", "quantity": 31},
]

benchmark = {
    "ETH-USD": 1,
    "ALGO-USD": 1014,
    "SHIB-USD": 61200000,
    "DOGE-USD": 8923,
    "SOL-USD": 16,
    "MATIC-USD": 1280,
    "AVAX-USD": 31,
}


class PortfolioValue(SymbolCollection):
    class Decorators:
        @classmethod
        def add_portfolio_values(cls, decorated):
            def inner(*args, **kwargs):
                returned_values = decorated(*args, **kwargs)
                return args[0]._add_holding_values(returned_values)

            return inner

    def __init__(
        self, portfolio: dict, symbols: list = None, interval: str = "5m", log_level=logging.DEBUG
    ):
        super().__init__(symbols, interval, log_level)
        self.portfolio = portfolio

    def _add_holding_values(self, collection):
        for k, s in collection.items():
            s["Units_held"] = self.portfolio[k]
            s["Close_value"] = s["Close"] * s["Units_held"]
        return collection

    @Decorators.add_portfolio_values
    def get_range(self, start: pd.Timestamp = None, end: pd.Timestamp = None):
        return super().get_range(start, end)

    @Decorators.add_portfolio_values
    def get_all(self, foward_fill: bool = True):
        return super().get_all(foward_fill)

    @Decorators.add_portfolio_values
    def get_one(self, date: pd.Timestamp, approximate: bool = True):
        return super().get_one(date, approximate)

    @Decorators.add_portfolio_values
    def get_latest(self):
        return super().get_latest()

    @Decorators.add_portfolio_values
    def get_first(self):
        return super().get_first()

    def sum_first(self) -> float:
        first_values = self.get_first()
        sum_total = 0
        for s in first_values:
            sum_total += s.Close_value
        return sum_total

    def sum_latest(self) -> float:
        first_values = self.get_latest()
        sum_total = 0
        for s in first_values:
            sum_total += s.Close_value
        return sum_total

    def sum_range(self, start: pd.Timestamp = None, end: pd.Timestamp = None) -> pd.DataFrame:
        range_values = self.get_range(start, end)
        return_df = pd.DataFrame
        for k, s in range_values.items():
            if return_df.empty:
                return_df = s["Close_value"].to_frame()
            else:
                return_df = return_df["Close_value"].to_frame() + s["Close_value"].to_frame()

        return return_df

    def sum_all(self) -> pd.DataFrame:
        all_values = self.get_all()
        return_df = pd.DataFrame
        for k, s in all_values.items():
            if return_df.empty:
                return_df = s["Close_value"].to_frame()
            else:
                return_df = return_df["Close_value"].to_frame() + s["Close_value"].to_frame()

        return return_df

    @property
    def portfolio_df(self):
        # TODO only recalculate the bits that have changed since last run
        portfolio_df = self.sum_all()
        starting_value = portfolio_df.iloc[0].Close_value
        portfolio_df = add_sma(portfolio_df)
        add_comparisons(portfolio_df, starting_value)
        return portfolio_df

        # this_sma = round(portfolio_analysis.sma.iloc[-1], 3)
        # this_diff_pct = round(portfolio_analysis.portfolio_diff_pct.iloc[-1], 3)


def new_analyse_interval(starting_value, symbol_collection: SymbolCollection):
    idx = symbol_collection.get_latest()

    latest = symbol_collection.get()
    total_value = 0
    for holding in benchmark:
        this_symbol = holding["symbol"]
        this_quantity = holding["quantity"]
        this_close = latest[this_symbol].Close
        this_value = this_quantity * this_close
        total_value += this_value
    print(f"Current value: {total_value}")


def analyse_interval(starting_value):
    start_date = datetime.now() - relativedelta(minutes=(5 * 300))
    # start_date = datetime.now() - relativedelta(days=50)

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

    # while current_record <= holding["bars"].index[-1]:
    # for this_index in holding["bars"].index:
    # bar_length = len(holding["bars"].index)
    # for i in range(0, bar_length):
    analyse_date = holding["bars"].index[50]
    analyse_end = holding["bars"].index[-1]
    for holding in benchmark:
        this_end = holding["bars"].index[-1]
        if this_end > analyse_end:
            analyse_end = this_end

        # this_start = holding["bars"].index[0]
        # if this_start < analyse_date:
        #    analyse_date = this_start

    # now analyse_end is the latest record to finish
    while analyse_date <= analyse_end:
        portfolio_value = 0
        for holding in benchmark:
            try_date = analyse_date
            attempts = 0
            while True:
                try:
                    portfolio_value += holding["bars"].Close_value.loc[try_date]
                    break
                except KeyError as e:
                    attempts += 1
                    if attempts > 20:
                        raise

                    try_date = try_date - record_interval
                    # log_wp.log(9, f"{holding['symbol']} does not have a record for {try_date}, falling back on {fallback_date}")
                    # portfolio_value += holding["bars"]["portfolio_value"].loc[fallback_date]
                    # if we're missing more than one cycle of data, then I give up TODO this means no mixing crypto with normies
                except Exception as e:
                    print("wut")

        diff = portfolio_value - starting_value
        diff_pct = portfolio_value / starting_value

        new_row = pd.DataFrame(
            {
                "timestamp": analyse_date,
                "portfolio_value": portfolio_value,
                "portfolio_diff": diff,
                "portfolio_diff_pct": diff_pct,
            },
            index=[1],
            columns=columns,
        )
        portfolio_df = pd.concat([portfolio_df, new_row], ignore_index=True)
        analyse_date += record_interval

    sma = []
    for index in portfolio_df.index:
        if index <= 100:
            sma.append(NaN)
        else:
            sma_end = (
                portfolio_df.index.get_loc(index) + 1
            )  # want it to be inclusive of current record
            sma_start = sma_end - 100
            sma.append(portfolio_df.iloc[sma_start:sma_end].portfolio_diff_pct.mean())

    portfolio_df["sma"] = pd.Series(sma).values
    return portfolio_df


def add_sma(portfolio_df, sma_intervals: int = 100, sma_period=20):
    if len(portfolio_df) < sma_intervals:
        raise KeyError(
            f"Cannot calculate SMA for {sma_intervals} intervals, since length of dataframe is only {len(portfolio_df)}"
        )

    slice_length = sma_intervals + sma_period

    df = portfolio_df.iloc[-slice_length:].copy()
    sma = []

    for index in df.index:
        if index <= df.index[sma_period]:
            sma.append(NaN)
        else:
            sma_end = df.index.get_loc(index) + 1  # want it to be inclusive of current record
            sma_start = sma_end - sma_period
            sma.append(df.iloc[sma_start:sma_end].mean().Close_value)

    df["sma"] = sma
    # df["sma"] = pd.Series(sma).values
    # df["sma"] = sma
    # trim the NaNs
    # df = df[-sma_intervals:]
    df = df.loc[df.sma.isna() == False]
    portfolio_df = portfolio_df.assign(sma=df["sma"])
    log_wp.debug(f"Added {len(sma)} SMA values to portfolio_df")
    return portfolio_df

    # if "sma" not in portfolio_df.columns:
    #    portfolio_df["sma"] = NaN
    # portfolio_df[portfolio_df["sma"].isnull()] = df
    # return portfolio_df
    # return pd.concat([portfolio_df, df[~df.index.isin(portfolio_df.index)]]).sort_index()
    # pd.concat([bars, new_bars[~new_bars.index.isin(bars.index)]]).sort_index()


def add_comparisons(portfolio_df, starting_value):
    # check if the portfolio_df already has columns for portfolio_diff and portfolio_diff_pct
    if "portfolio_diff" not in portfolio_df.columns:
        # add columns to dataframe
        portfolio_df["portfolio_diff"] = NaN
        portfolio_df["portfolio_diff_pct"] = NaN

    # now get all the rows where these columns are NaN
    rows_to_calculate = portfolio_df.loc[portfolio_df["portfolio_diff"].isna()]
    rows_to_calculate["portfolio_diff"] = rows_to_calculate["Close_value"] - starting_value
    rows_to_calculate["portfolio_diff_pct"] = rows_to_calculate["Close_value"] / starting_value
    portfolio_df["portfolio_diff"].fillna(rows_to_calculate["portfolio_diff"], inplace=True)
    portfolio_df["portfolio_diff_pct"].fillna(rows_to_calculate["portfolio_diff_pct"], inplace=True)


def main(args):
    store = Ssm()
    alpaca_api_key = store.get(path=PATH_PAPER_ALPACA_API_KEY)
    alpaca_security_key = store.get(path=PATH_PAPER_ALPACA_SECURITY_KEY)

    api = AlpacaAPI(alpaca_key_id=alpaca_api_key, alpaca_secret_key=alpaca_security_key)

    slack_bot_key = store.get(path=slack_bot_key_path)
    slack_announcements_channel = store.get(path=slack_channel_path)
    notification_service = notification_services.Slack(
        bot_key=slack_bot_key, channel=slack_announcements_channel
    )

    symbol_collection = PortfolioValue(benchmark, [k for k, v in benchmark.items()])

    # okay so we've set our starting point, now keep grabbing data and checking if we should buy in
    position_taken = False
    while True:
        current_portfolio = symbol_collection.portfolio_df
        if not position_taken:
            this_sma = round(current_portfolio.sma.iloc[-1], 3)
            this_close = round(current_portfolio.Close_value.iloc[-1], 3)
            # this_diff_pct = round(current_portfolio.portfolio_diff_pct.iloc[-1], 3)
            # this_sma = porfolio_analysis.sma.iloc[-1]
            # this_diff_pct = porfolio_analysis.portfolio_diff_pct.iloc[-1]
            if this_close > this_sma:
                # the latest diff pct is better than the sma100 diff pct - its getting better, and this is our buy signal
                buy_value = 0
                for asset, quantity in benchmark.items():
                    symbol = asset.replace("-", "")
                    units_to_buy = quantity
                    buy = api.buy_order_market(symbol, units_to_buy)
                    buy_value += buy.filled_total_value
                stop_loss = current_portfolio.Close_value.iloc[-1]
                position_taken = True
                message = f"Took position valued at {buy_value}. Last close {this_close} > SMA {this_sma} value/stop loss of {stop_loss:,.4f}"
                print(message)
                notification_service.send(message)
            else:
                log_wp.debug(f"No crossover found (last close {this_close}, SMA {this_sma})")

        else:
            # first check if stop loss has been hit, and if so then liquidate
            current_value = current_portfolio.Close_value.iloc[-1]
            if current_value < stop_loss:
                # stop loss hit
                sell_value = 0
                for asset, quantity in benchmark.items():
                    symbol = asset.replace("-", "")
                    units_to_sell = quantity
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
                profit_50 = profit * 0.5
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
    # a= MarketData("SOL-USD")
    # a.bars = a.bars.loc[(a.bars.index < pd.Timestamp("2022-06-10 23:00:00-04:00"))]
    # a.get(start=pd.Timestamp("2022-06-14 12:32:39-04:00"))
    args = None

    main(args)
