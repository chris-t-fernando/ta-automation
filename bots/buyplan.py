from utils import (
    get_blue_cycle_start,
    get_red_cycle_start,
    calculate_stop_loss_date,
    calculate_stop_loss_unit_price,
    count_intervals,
    clean,
)
from math import floor
import logging
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

log_wp = logging.getLogger("buyplan")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
log_wp.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)


class BuyPlan:
    ORDER_SIZE = 2000

    def __init__(self, symbol, df, profit_target: float = 1.5, notional_units=False):
        self.symbol = symbol
        self.capital = BuyPlan.ORDER_SIZE

        self.blue_cycle_start = get_blue_cycle_start(df=df)
        self.red_cycle_start = get_red_cycle_start(
            df=df, before_date=self.blue_cycle_start
        )
        self.blue_cycle_record = df.loc[self.blue_cycle_start]

        self.blue_cycle_macd = self.blue_cycle_record.macd_macd
        self.blue_cycle_signal = self.blue_cycle_record.macd_signal
        self.blue_cycle_histogram = self.blue_cycle_record.macd_histogram
        self.macd_signal_gap = self.blue_cycle_macd - self.blue_cycle_signal

        # then get the lowest close price since the cycle began
        stop_unit = calculate_stop_loss_unit_price(
            df=df,
            start_date=self.red_cycle_start,
            end_date=self.blue_cycle_start,
        )

        stop_unit_date = calculate_stop_loss_date(
            df=df,
            start_date=self.red_cycle_start,
            end_date=self.blue_cycle_start,
        )

        # and for informational/confidence purposes, hold on to the intervals since this happened
        self.intervals_since_stop = count_intervals(df=df, start_date=stop_unit_date)

        # calculate other order variables
        self.entry_unit = df.Close.iloc[-1]
        self.stop_unit = stop_unit

        if notional_units:
            self.units = self.capital / self.entry_unit
        else:
            self.units = floor(self.capital / self.entry_unit)

        self.steps = 0
        self.risk_unit = self.entry_unit - self.stop_unit
        self.risk_value = self.units * self.risk_unit
        self.target_profit = profit_target * self.risk_unit
        self.original_risk_unit = self.risk_unit
        self.original_stop = stop_unit

        self.entry_unit = round(self.entry_unit, 2)
        self.target_price = self.entry_unit + self.target_profit

        # fmt: off
        log_wp.info(f"{self.symbol} - BUY PLAN REPORT")
        log_wp.info(f"{self.symbol} - Strength:\t\tNot sure how I want to do this yet")
        log_wp.info(f"{self.symbol} - MACD:\t\t\t{self.blue_cycle_macd}")
        log_wp.info(f"{self.symbol} - Signal:\t\t{self.blue_cycle_signal}")
        log_wp.info(f"{self.symbol} - Histogram:\t\t{self.blue_cycle_histogram}")
        log_wp.info(f"{self.symbol} - Capital:\t\t${clean(self.capital)}")
        log_wp.info(f"{self.symbol} - Units to buy:\t\t{clean(self.units)} units")
        log_wp.info(f"{self.symbol} - Entry point:\t\t${clean(self.entry_unit)}")
        log_wp.info(f"{self.symbol} - Stop loss:\t\t${clean(stop_unit)}")
        log_wp.info(f"{self.symbol} - Cycle began:\t\t{self.intervals_since_stop} intervals ago on {stop_unit_date}")
        log_wp.info(f"{self.symbol} - Unit risk:\t\t${clean(self.risk_unit)} ({round(self.risk_unit/self.entry_unit*100,1)}% of unit cost)")
        log_wp.info(f"{self.symbol} - Unit profit:\t\t${clean(self.target_profit)} ({round(self.target_profit/self.entry_unit*100,1)}% of unit cost)")
        log_wp.info(f"{self.symbol} - Target price:\t\t${clean(self.target_price)} ({round(self.target_price/self.capital*100,1)}% of capital)")
        # fmt: on
