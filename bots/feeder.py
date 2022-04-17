from abc import ABC, abstractmethod
import yfinance as yf
from datetime import datetime
from pandas import DataFrame as df


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
            end = datetime.now().astimezone()

        #### datetime.now().astimezone()

        # start = datetime.fromisoformat(start)
        # end = datetime.fromisoformat(end)

        return yf.Ticker(symbol).history(
            start=start, end=end, interval=interval, actions=False
        )


class MockerException(Exception):
    ...


class Mocker:
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
            self.read_end = datetime.now().astimezone()
        else:
            self.real_end = real_end.astimezone()

        self.data_source = data_source

    def get_bars(self, symbol: str, start: str, end: str, interval: str = "1d"):
        # yf/pandas will drop time and timezone if interval is greater than 24 hours
        if not self.initialised:
            self.bars = self.data_source.get_bars(
                symbol=symbol, start=start, end=self.real_end, interval=interval
            )

            self.symbol = symbol
            self.start = start
            self.current = end
            self.interval = interval
            self.initialised = True

            self.bars = self.bars.tz_localize(None)

            # self.bars['time'].dt.tz_localize(None)

        if self.symbol != symbol or self.start != start or self.interval != interval:
            raise MockerException(
                "Can't change symbol, start or interval once instantiated!"
            )

        #
        #       this logic wouldn't even work anyway since there is a months symbol....
        #        if len(end) > 10 and "m" not in interval:
        #            raise ValueError(
        #                f"Interval must be <24 hours when specifying a real_end that contains time and timezone. Found {interval} interval and {end} date/time"
        #            )
        #        elif len(end) < 25 and "m" in interval:
        #            raise ValueError(
        #                f"When interval is set to minutes, date/time must be specified similar to 2022-03-30T00:00:00+10:00. Found {end}"
        #            )
        self.last_end = end

        return self.bars.loc[:end]

    def get_next(self):
        return self.bars.loc[self.bars.index > self.last_end].index[0]


# self.bars.index
# self.bars.keys
# self.bars["column"]
