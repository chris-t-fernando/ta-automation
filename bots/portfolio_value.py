from numpy import NaN
from symbol_objects import SymbolCollection
import logging
import pandas as pd

log_wp = logging.getLogger("portfolio_value")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("portfolio_value.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.INFO)


class PortfolioValue(SymbolCollection):
    class Decorators:
        @classmethod
        def add_portfolio_values(cls, decorated):
            def inner(*args, **kwargs):
                returned_values = decorated(*args, **kwargs)
                return args[0]._add_holding_values(returned_values)

            return inner

    def __init__(
        self,
        portfolio: dict,
        symbols: list = None,
        interval: str = "5m",
        log_level=logging.INFO,
        sma_intervals: int = 100,
        sma_period: int = 20,
    ):
        super().__init__(symbols=symbols, interval=interval, log_level=log_level)
        self.sma_intervals = sma_intervals
        self.sma_period = sma_period
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
        portfolio_df = self.add_sma(portfolio_df)
        self.add_comparisons(portfolio_df, starting_value)
        return portfolio_df

        # this_sma = round(portfolio_analysis.sma.iloc[-1], 3)
        # this_diff_pct = round(portfolio_analysis.portfolio_diff_pct.iloc[-1], 3)

    def add_sma(self, portfolio_df):
        if len(portfolio_df) < self.sma_intervals:
            raise KeyError(
                f"Cannot calculate SMA for {self.sma_intervals} intervals, since length of dataframe is only {len(portfolio_df)}"
            )

        slice_length = self.sma_intervals + self.sma_period

        df = portfolio_df.iloc[-slice_length:].copy()
        sma = []

        for index in df.index:
            if index <= df.index[self.sma_period]:
                sma.append(NaN)
            else:
                sma_end = df.index.get_loc(index) + 1  # want it to be inclusive of current record
                sma_start = sma_end - self.sma_period
                sma.append(df.iloc[sma_start:sma_end].mean().Close_value)

        df["sma"] = sma
        df = df.loc[df.sma.isna() == False]
        portfolio_df = portfolio_df.assign(sma=df["sma"])
        log_wp.debug(f"Added {len(sma)} SMA values to portfolio_df")
        return portfolio_df

    def add_comparisons(self, portfolio_df, starting_value):
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
        portfolio_df["portfolio_diff_pct"].fillna(
            rows_to_calculate["portfolio_diff_pct"], inplace=True
        )
