from abc import ABC, abstractmethod
import yfinance as yf
from datetime import datetime
from pandas import DataFrame as df
import btalib
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta


class IFeeder(ABC):
    @abstractmethod
    def __init__(self):
        ...


class YFinanceFeeder:
    def get_bars(
        self, symbol: str, start: datetime, end: datetime = None, interval: str = "1d"
    ):
        intervals = [
            "1m",
            "2m",
            "5m",
            "15m",
            "30m",
            "60m",
            "90m",
            "1h",
            "1d",
            "5d",
            "1wk",
            "1mo",
            "3mo",
        ]
        if interval not in intervals:
            raise ValueError(
                f"Interval was {str(interval)} but must be one of {str(intervals)}"
            )

        if end == None:
            # end = datetime.now().astimezone()
            end = datetime.now()
            yfend = end
        else:
            # weird yfinance bug will return less rows than we asked for???
            yfend = end + relativedelta(days=2)

        # assumes anything with a length longer than 4 characters is a crypto stock
        if len(symbol) > 4:
            if symbol[:4] == "-USD":
                ...
            elif symbol[:3] == "USD":
                # alpaca
                symbol = symbol[:-3] + "-USD"
            else:
                symbol = symbol + "-USD"

        bars = yf.Ticker(symbol).history(
            start=start, end=yfend, interval=interval, actions=False
        )
        bars = bars.tz_localize(None)
        bars = bars.loc[bars.index <= end]

        return bars


class MockDataSourceException(Exception):
    ...


class MockDataSource:
    symbol: str
    start: datetime
    end: datetime
    current: datetime
    interval: str
    initialised: bool = False
    bars: df

    def __init__(
        self,
        data_source,
        real_end: datetime = None,
    ):
        if real_end == None:
            # self.read_end = datetime.now().astimezone()
            self.read_end = datetime.now()
        else:
            # self.real_end = real_end.astimezone()
            self.real_end = real_end

        self.data_source = data_source

    def get_bars(
        self,
        symbol: str,
        start: str,
        end: str,
        interval: str = "1d",
        do_macd=False,
        do_sma=False,
    ):
        # yf/pandas will drop time and timezone if interval is greater than 24 hours
        if not self.initialised:
            self.bars = self.data_source.get_bars(
                symbol=symbol, start=start, end=self.real_end, interval=interval
            )

            interval_delta, max_range, tick = self.get_interval_settings(
                interval=interval
            )

            # if len(self.bars) == 0:
            #    raise ValueError("No rows. Check symbol exists and dates provided")
            if len(self.bars) == 0:
                new_range = max_range
                # something went wrong - usually bad symbol and search parameters
                while len(self.bars) == 0:
                    new_range -= 1
                    if new_range == 0:
                        print(f"New range got to zero?!")
                        print(symbol)
                        exit()
                    print(f"Bad start date. Trying again with range {new_range}")

                    bars_start = datetime.now() + timedelta(days=-new_range)

                    self.bars = self.data_source.get_bars(
                        symbol=symbol,
                        start=bars_start,
                        end=self.real_end,
                        interval=interval,
                    )

            self.symbol = symbol
            self.start = start
            self.current = end
            self.interval = interval
            self.initialised = True

            if do_sma:
                sma = btalib.sma(self.bars, period=200)
                self.bars["sma_200"] = sma["sma"]

            # if i was smart i'd make this a separate function so i could reuse it...
            if do_macd:

                macd = btalib.macd(self.bars)
                self.bars["macd_macd"] = macd["macd"]
                self.bars["macd_signal"] = macd["signal"]
                self.bars["macd_histogram"] = macd["histogram"]
                self.bars["macd_crossover"] = False
                self.bars["macd_signal_crossover"] = False
                self.bars["macd_above_signal"] = False
                self.bars["macd_cycle"] = None

                # loops looking for three things - macd-signal crossover, signal-macd crossover, and whether macd is above signal
                cycle = None

                for d in self.bars.index:
                    # start with crossover search
                    # convert index to a datetime so we can do a delta against it                           ****************
                    previous_key = d - interval_delta
                    # previous key had macd less than or equal to signal
                    if self.bars["macd_macd"].loc[d] > self.bars["macd_signal"].loc[d]:
                        # macd is greater than signal - crossover
                        self.bars.at[d, "macd_above_signal"] = True
                        try:
                            if (
                                self.bars["macd_macd"].loc[previous_key]
                                <= self.bars["macd_signal"].loc[previous_key]
                            ):
                                cycle = "blue"
                                self.bars.at[d, "macd_crossover"] = True

                        except KeyError as e:
                            # ellipsis because i don't care if i'm missing data (maybe i should...)
                            ...

                    if self.bars["macd_macd"].loc[d] < self.bars["macd_signal"].loc[d]:
                        # macd is less than signal
                        try:
                            if (
                                self.bars["macd_macd"].loc[previous_key]
                                >= self.bars["macd_signal"].loc[previous_key]
                            ):
                                cycle = "red"
                                self.bars.at[d, "macd_signal_crossover"] = True

                        except KeyError as e:
                            # ellipsis because i don't care if i'm missing data (maybe i should...)
                            ...

                    self.bars.at[d, "macd_cycle"] = cycle

                if (
                    self.symbol != symbol
                    or self.start != start
                    or self.interval != interval
                ):
                    raise MockDataSourceException(
                        "Can't change symbol, start or interval once instantiated!"
                    )

        self.last_end = end

        return self.bars.loc[:end]

    def get_next(self):
        try:
            return self.bars.loc[self.bars.index > self.last_end].index[0]
        except:
            return False

    def get_interval_settings(self, interval):
        minutes_intervals = ["1m", "2m", "5m", "15m", "30m", "60m", "90m"]
        max_period = {
            "1m": 6,
            "2m": 59,
            "5m": 59,
            "15m": 59,
            "30m": 59,
            "60m": 500,
            "90m": 59,
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
