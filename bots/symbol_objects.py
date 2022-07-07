from tracemalloc import start
from numpy import NaN
import logging
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pytz
import time
import utils


class SymbolAlreadyInCollectionError(Exception):
    ...


class SymbolError(Exception):
    ...


class SymbolCollection:
    symbols: dict
    interval: str

    def __init__(self, symbols: list = None, interval: str = "5m", log_level=logging.CRITICAL):
        self._configure_logging(level=log_level)
        self.interval = interval
        self.interval_minutes = int(interval[:-1])
        self.interval_delta = relativedelta(minutes=self.interval_minutes)
        self.symbols = {}
        self._do_add_symbols(symbols)

    def _configure_logging(self, level):
        log_wp = logging.getLogger("SymbolCollection")
        hdlr = logging.StreamHandler()
        fhdlr = logging.FileHandler(f"SymbolCollection.log")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
        )
        hdlr.setFormatter(formatter)
        log_wp.addHandler(hdlr)
        log_wp.addHandler(fhdlr)
        log_wp.setLevel(level)
        self.log_wp = log_wp

    def __contains__(self, symbol):
        return symbol in self.symbols.keys()

    def _do_add_symbols(self, symbols):
        if type(symbols) == list:
            for s in symbols:
                self._do_add_symbols(s)
        elif type(symbols) == str:
            new_symbol = SymbolData(symbols, interval=self.interval)
            self.symbols[new_symbol.yf_symbol] = new_symbol
        elif type(symbols) == SymbolData:
            self.symbols[symbols.yf_symbol] = symbols

    def add_symbol(self, symbol):
        if symbol not in self:
            self._do_add_symbols(symbol)
        else:
            raise SymbolAlreadyInCollectionError()

    def get_first_index(self):
        oldest = None
        for k, s in self.symbols.items():
            if oldest == None or oldest < s.bars.index[0]:
                oldest = s.bars.index[0]
        return oldest

    def get_first(self):
        return_dict = {}
        for k, s in self.symbols.items():
            return_dict[k] = s.bars.iloc[0].copy()

        return return_dict

    def get_latest(self):
        return_dict = {}
        for k, s in self.symbols.items():
            return_dict[k] = s.bars.iloc[-1].copy()

        return return_dict

    def get_range(self, start: pd.Timestamp = None, end: pd.Timestamp = None):
        return_dict = {}
        for k, s in self.symbols.items():
            return_dict[k] = s.get_range(start, end)

        return return_dict

    def get_one(self, date: pd.Timestamp, approximate: bool = True):
        rounded_date = round_time(date, self.interval_minutes)

        return_dict = {}
        for k, s in self.symbols.items():
            if rounded_date in s.bars.index:
                idx = rounded_date
            else:
                idx = rounded_date
                attempts = 0
                if approximate:
                    interval_range = 2
                else:
                    interval_range = 0

                while True:
                    try:
                        s.bars.index[idx]
                        break
                    except KeyError as e:
                        attempts += 1
                        if attempts > interval_range:
                            raise

                        idx = idx - self.interval_delta

            return_dict[k] = s.bars.loc[idx].copy()

        return return_dict

    def get_all(self, foward_fill: bool = True):
        # first make a superset of the indexes
        indexes = set()
        for k, s in self.symbols.items():
            s.refresh_cache()
            this_index = set(s.bars.index)
            indexes = indexes.union(this_index)

        # now now make sure that each symbol has an index for every one of those superset of records
        return_dict = {}
        for k, s in self.symbols.items():
            missing_indexes = indexes.symmetric_difference(s.bars.index)
            self.log_wp.info(
                f"Plugging {len(missing_indexes)} missing timestamp indexes for {k} using ffill"
            )

            # blank rows to insert into the symbol's bars so we can use ffill til plug the gaps
            new_rows = pd.DataFrame(
                [[NaN, NaN, NaN, NaN, NaN]], columns=s.bars.columns, index=missing_indexes
            )
            filled_bars = s.merge_bars(s.bars, new_rows)
            filled_bars = filled_bars.fillna(method="ffill")

            return_dict[k] = filled_bars

        return return_dict


class SymbolData:
    yf_symbol: str
    interval: str
    bars: pd.DataFrame
    registered_ta_functions: set
    ta_data: dict
    interval_minutes: int
    max_range: relativedelta
    interval_delta: relativedelta
    refresh_timeout: datetime

    class Decorators:
        @classmethod
        def refresh_bars(cls, decorated):
            def inner(*args, **kwargs):
                if kwargs.get("refresh"):
                    args[0].refresh_cache()
                return decorated(*args, **kwargs)

            return inner

    def __init__(self, yf_symbol: str, interval: str = "5m", log_level=logging.INFO):
        self.yf_symbol = yf_symbol
        self._configure_logging(level=log_level)

        self.interval = interval
        self.registered_ta_functions = set()
        self.ta_data = {}
        self.interval_delta, self.max_range = utils.get_interval_settings(self.interval)
        self.interval_minutes = int(interval[:-1])
        self.refresh_timeout = None

        self.refresh_cache()

        if len(self.bars) == 0:
            # if len(yf.Ticker(yf_symbol).actions) == 0:
            # invalid ticker
            error_message = f"Invalid symbol specified, bailing"
            self.log_wp.error(error_message)
            raise SymbolError(error_message)

    def _configure_logging(self, level):
        logger = logging.getLogger(self.yf_symbol)  # or pass an explicit name here, e.g. "mylogger"
        log_wp = logging.LoggerAdapter(logger)
        hdlr = logging.StreamHandler()
        fhdlr = logging.FileHandler(f"symbol_objects_{self.yf_symbol}.log")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)9s - %(levelname)s - %(funcName)20s - %(message)s"
        )
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.addHandler(fhdlr)
        logger.setLevel(level)
        self.log_wp = log_wp

    def __repr__(self):
        return self.yf_symbol

    def _validate_minute(self, minute):
        if self.interval == "1m":
            return True
        elif self.interval == "2m":
            if minute % 2 == 0:
                return True
        elif minute % 5 == 0:
            return True
        return False

    @staticmethod
    def merge_bars(bars, new_bars):
        return pd.concat([bars, new_bars[~new_bars.index.isin(bars.index)]]).sort_index()
        # pd.concat([self.bars, new_bars[~new_bars.index.isin(self.bars.index)]]).sort_index()

    def _make_now(self):
        local_tz = pytz.timezone("Australia/Melbourne")
        start_date_unaware = datetime.now()
        start_date_melbourne = local_tz.localize(start_date_unaware)
        # start_date_unaware.replace(tzinfo=pytz.timezone("Australia/Melbourne"))

        # if there's no - then assume its NYSE, else assume its crypto
        if self.yf_symbol.find("-") == -1:
            tz = "US/Eastern"
        else:
            tz = "UTC"

        start_date = start_date_melbourne.astimezone(pytz.timezone(tz))
        start_date = start_date.replace(microsecond=0)
        return start_date

    def refresh_cache(self, start: pd.Timestamp = None, end: pd.Timestamp = None):
        cache_miss = False
        initialising = False

        if not hasattr(self, "bars") or len(self.bars) == 0:
            cache_miss = True
            initialising = True
            self.log_wp.debug(f"Cache miss - bars len 0")

            rounded_end = self._make_now()
            max_duration = rounded_end - self.max_range
            rounded_start = round_time(max_duration, self.interval_minutes)

            yf_start = rounded_start

            self.bars = pd.DataFrame()

        # has a bars attribute so its safe to inspect it
        else:
            yf_start = self.bars.index[-1]
            if start == None:
                rounded_start = self.bars.index[0]
            else:
                rounded_start = round_time(start, self.interval_minutes)
                if rounded_start < self.bars.index[0]:
                    yf_start = rounded_start
                    cache_miss = True
                    self.log_wp.debug(f"Cache miss - start earlier than bars")
                elif rounded_start > self.bars.index[-1]:
                    cache_miss = True
                    self.log_wp.debug(f"Cache miss - start later than bars")

            if end == None:
                rounded_end = round_time(self._make_now(), self.interval_minutes)
            else:
                rounded_end = round_time(end, self.interval_minutes)
            if rounded_end > self.bars.index[-1]:
                cache_miss = True
                self.log_wp.debug(f"Cache miss - end later than bars")

        if cache_miss:
            if self.refresh_timeout != None and self.refresh_timeout > datetime.now():
                self.log_wp.debug(
                    f"Cache timeout {self.refresh_timeout} is still in effect, cancelling cache refresh"
                )
                return

            # self.log_wp.debug(f"Cache miss")
            self.log_wp.debug(f"  - pulling from yf from {yf_start}")
            new_bars = yf.Ticker(self.yf_symbol).history(
                start=yf_start,
                interval=self.interval,
                actions=False,
                debug=False,
            )

            if len(new_bars) == 0:
                self.log_wp.error(f"Failed to retrieve new bars")
                return

            # yfinance returns results for periods still in progress (eg. includes 9:07:00 after 9:05:00 if you query at 9:08)
            if not self._validate_minute(new_bars.index[-1].minute):
                # trim it
                self.log_wp.debug(f"  - dropped half-baked row {new_bars.index[-1]}")
                new_bars = new_bars.iloc[:-1]

            if not initialising:
                old_rows = len(self.bars)
                old_start = self.bars.index[0]
                old_finish = self.bars.index[-1]

            self.bars = self.merge_bars(self.bars, new_bars)

            if not initialising:
                self.log_wp.debug(
                    f"  - merged {old_rows:,} old bars with {len(new_bars):,} new bars, new length is {len(self.bars):,}"
                )
                if self.bars.index[0] != old_start:
                    self.log_wp.debug(f"  - new start is {self.bars.index[0]} vs old {old_start}")
                if self.bars.index[-1] != old_finish:
                    self.log_wp.debug(
                        f"  - new finish is {self.bars.index[-1]} vs old {old_finish}"
                    )

            self._reapply_btalib(start=new_bars.index[0], end=new_bars.index[-1])

            timeout_seconds = utils.get_pause(self.interval)
            timeout_window = relativedelta(seconds=timeout_seconds)
            new_timeout = datetime.now() + timeout_window
            self.refresh_timeout = new_timeout

        # hackity hack - we just changed the length/last record in the dataframe
        # if end == None:
        #    rounded_end = self.bars.index[-1]

        # return_records = self.bars.loc[(self.bars.index >= rounded_start) & (self.bars.index <= rounded_end)]
        # log_wp.debug(f"Returning {len(return_records)} out of {len(self.bars)} rows")
        # return return_records

    def apply_btalib(self, btalib_function, start=None, end=None):
        key_name = str(btalib_function)
        # new ta function
        if not str(btalib_function) in self.ta_data:
            # register this ta function - so it gets refreshed next time there is a cache miss
            self.ta_data[key_name] = btalib_function(self.bars).df
            self.registered_ta_functions.add(btalib_function)

        else:
            # existing ta function, so just refresh what's changed
            # start by grabbing the new rows, plus a buffer of 100 previous rows
            # get the index 100 rows earlier

            start_loc = self.bars.index.get_loc(start)
            padding = 100
            if start_loc < padding:
                padding_start = self.bars.index[0]
            else:
                padding_start = self.bars.index[start_loc - padding]

            # can't just use slice because get a weird error about comparing different timezones
            # ta_data_input = self.bars.loc[padding_start:end]
            ta_data_input = self.bars.loc[
                (self.bars.index >= padding_start) & (self.bars.index <= end)
            ]

            ta_data_output = btalib_function(ta_data_input).df

            # NOT NEEDED - the xor gets rid of this
            # get rid of the padding
            # ta_data_output_trimmed = ta_data_output.loc[start:end]

            dest_df = self.ta_data[key_name]

            self.ta_data[key_name] = self.merge_bars(dest_df, ta_data_output)
            # pd.concat([dest_df, ta_data_output[~ta_data_output.index.isin(dest_df.index)]]).sort_index()

    def _reapply_btalib(self, start=None, end=None):
        if not start:
            start = self.bars.index[0]
        if not end:
            end = self.bars.index[-1]

        for btalib_function in self.registered_ta_functions:
            self.apply_btalib(btalib_function, start, end)

    def get_first(self):
        return self.bars.iloc[0]

    @Decorators.refresh_bars
    def get_range(self, start: pd.Timestamp = None, end: pd.Timestamp = None):
        return self.bars.loc[start:end]

    @Decorators.refresh_bars
    def get_latest(self, refresh=False):
        return self.bars.iloc[-1]

    @Decorators.refresh_bars
    def in_bars(self, timestamp, refresh=False):
        return timestamp in self.bars


def round_time(date: pd.Timestamp, interval_minutes):
    minutes = (date.minute % interval_minutes) * 60
    seconds = date.second
    total_seconds = minutes + seconds

    interval_seconds = interval_minutes * 60
    interval_midpoint = interval_seconds / 2

    if total_seconds < interval_midpoint:
        # round down
        delta = -relativedelta(seconds=total_seconds)

    else:
        # round up
        padding = interval_seconds - total_seconds
        delta = relativedelta(seconds=padding)

    rounded_date = date + delta
    # log_wp.debug(f"Rounded {date} to {rounded_date}")
    return rounded_date


if __name__ == "__main__":
    interval_string = "5m"
    a = SymbolData("BTC-USD", interval_string)
    b = SymbolData("ADA-USD", interval_string)
    # a.bars = a.bars.loc[(a.bars.index > pd.Timestamp("2022-06-10 23:00:00-04:00"))]
    # a.get(start=pd.Timestamp("2022-06-04 12:32:39-04:00"))
    c = SymbolCollection([a, b, "AVAX-USD"])
    c.get_latest()
    import btalib

    # a.ta_data["<class 'btalib.indicators.sma.sma'>"] = a.ta_data["<class 'btalib.indicators.sma.sma'>"].iloc[:500]
    a.apply_btalib(btalib.sma)
    a.apply_btalib(btalib.macd)
    print(a.get_latest(refresh=False))
    print(a.get_range())
    print(c.get_one(pd.Timestamp("2022-06-24 11:33:00")))
    c.get_all()
    while True:
        pause = utils.get_pause(interval_string)
        print(f"Got data, sleeping for {round(pause,0)}s")
        # log_wp.debug(f"Got data, sleeping for {round(pause,0)}s")
        time.sleep(pause)
        print(a.get_latest(refresh=True).name)

    print("banana")
    # a.apply_btalib(btalib.sma, "2022-06-14 12:05:00-04:00", "2022-06-17 16:00:00-04:00")
