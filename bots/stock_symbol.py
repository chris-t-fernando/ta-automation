from datetime import datetime
from itradeapi import ITradeAPI
from utils import get_interval_settings, add_signals, merge_bars
import yfinance as yf
import logging

log_wp = logging.getLogger(__name__)  # or pass an explicit name here, e.g. "mylogger"

# symbol can be backtest naive
class Symbol:
    def __init__(
        self,
        symbol: str,
        api: ITradeAPI,
        interval: str,
        real_money_trading: bool,
        ssm,
        data_source,
        to_date: str = None,
    ):
        self.symbol = symbol
        self.api: ITradeAPI = api
        self.interval = interval
        self.real_money_trading = real_money_trading
        self.ssm = ssm
        self.data_source = data_source
        self.initialised = False
        self.last_date_processed = None

        bars = self.get_bars(
            symbol=self.symbol,
            interval=interval,
            to_date=to_date,
            initialised=False,
        )
        self.bars = add_signals(bars, interval)

    def get_bars(
        self, symbol, interval, from_date=None, to_date=None, initialised=True
    ):
        interval_delta, max_range = get_interval_settings(interval)
        if initialised == False:
            # we actually need to grab everything
            yf_start = datetime.now() - max_range
        else:
            # if we've specified a date, we're probably refreshing our dataset over time
            if from_date:
                # widen the window out, just to make sure we don't miss any data in the refresh
                yf_start = from_date - (interval_delta * 2)
            else:
                # we're refreshing but didn't specify a date, so assume its in the last x minutes/hours
                yf_start = datetime.now() - (interval_delta * 2)

        # didn't specify an end date so go up til now
        if to_date == None:
            yf_end = datetime.now()
        else:
            # specified an end date so use it
            yf_end = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")

        # no end required - we want all of the data
        bars = yf.Ticker(symbol).history(
            start=yf_start,
            interval=interval,
            actions=False,
        )

        if len(bars) == 0:
            # something went wrong - usually bad symbol and search parameters
            log_wp.debug(
                f"{symbol}: No data returned for start {yf_start} end {yf_end}"
            )

        bars = bars.tz_localize(None)
        bars = bars.loc[bars.index <= yf_end]

        return bars

    def update_bars(self, from_date=None, to_date=None):
        if from_date == None:
            from_date = self.bars.index[-1]

        new_bars = self.get_bars(
            symbol=self.symbol,
            interval=self.interval,
            from_date=from_date,
            to_date=to_date,
        )

        if len(new_bars) > 0:
            # pad new bars to 200 rows so that macd and sma200 work
            if len(new_bars) < 200:
                new_bars = merge_bars(new_bars, self.bars.iloc[-200:])

            new_bars = add_signals(new_bars, interval=self.interval)
            self.bars = merge_bars(self.bars, new_bars)

        else:
            log_wp.debug(f"{self.symbol}: No new data since {from_date}")
