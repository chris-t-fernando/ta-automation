# external packages
import logging
from math import floor

# my modules
from itradeapi import IOrderResult, Position
from utils import (
    get_blue_cycle_start,
    get_red_cycle_start,
    calculate_stop_loss_date,
    calculate_stop_loss_unit_price,
    count_intervals,
    clean,
)

log_wp = logging.getLogger("buyplan")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("buyplan.log")
log_wp.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)


class BuyPlan:
    def __init__(
        self,
        symbol: str,
        df,
        balance: float,
        profit_target: float = 1.5,
        notional_units: bool = False,
        precision: int = 3,
        min_trade_increment: float = 1,
        min_order_size: float = 1,
        min_price_increment: float = 0.001,
        max_play_value: float = 2000,
    ):
        self.success = False
        self.min_trade_increment = min_trade_increment
        self.min_order_size = min_order_size
        self.min_price_increment = min_price_increment
        self.precision = precision
        self.max_play_value = max_play_value

        self.symbol = symbol
        if balance < max_play_value:
            self.capital = balance
        else:
            self.capital = max_play_value

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
        stop_unit = round(
            calculate_stop_loss_unit_price(
                df=df,
                start_date=self.red_cycle_start,
                end_date=self.blue_cycle_start,
            ),
            precision,
        )

        stop_unit_date = calculate_stop_loss_date(
            df=df,
            start_date=self.red_cycle_start,
            end_date=self.blue_cycle_start,
        )

        # and for informational/confidence purposes, hold on to the intervals since this happened
        self.intervals_since_stop = count_intervals(df=df, start_date=stop_unit_date)

        # calculate other order variables
        self.entry_unit = round(df.Close.iloc[-1], precision)
        self.stop_unit = round(stop_unit, precision)
        self.last_low = df.Low.iloc[-1]
        self.last_high = df.High.iloc[-1]

        if self.stop_unit > self.last_low:
            self.error_message = "stop_unit_too_high"
            return

        if self.entry_unit * 1.25 < self.last_high:
            self.error_message = "last_high_too_low"
            return

        # if notional_units:
        #    self.units = self.capital / self.entry_unit
        # else:
        #    self.units = floor(self.capital / self.entry_unit)

        if max_play_value < self.entry_unit:
            # we're not buying any units
            self.error_message = "entry_larger_than_order_size"
            return

        units = self.capital / self.entry_unit
        if units < min_order_size:
            # too few - failed order
            self.error_message = "min_order_size"
            return

        self.units = floor(units - (units % min_trade_increment))

        if self.units == 0:
            # we're not buying any units
            self.error_message = "zero_units"
            return

        # if we don't have enough money, bail out
        if self.entry_unit * self.units > balance:
            self.error_message = "insufficient_balance"
            return

        self.steps = 0
        self.risk_unit = round(self.entry_unit - self.stop_unit, precision)
        if self.risk_unit == 0:
            print("banana")
        self.risk_value = round(self.units * self.risk_unit, precision)
        self.target_profit = round(profit_target * self.risk_unit, precision)
        self.original_risk_unit = round(self.risk_unit, precision)
        self.original_stop = stop_unit

        self.entry_unit = round(self.entry_unit, precision)
        self.target_price = round(self.entry_unit + self.target_profit, precision)

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

        self.success = True

    def take_profit(
        self,
        filled_order: IOrderResult,
        active_rule: dict,
        new_position_quantity: Position,
    ):
        filled_quantity = filled_order.filled_unit_quantity
        filled_unit_price = filled_order.filled_unit_price
        filled_value = filled_quantity * filled_unit_price

        # raise sell order
        pct_sell_down = active_rule["risk_point_sell_down_pct"]
        units = new_position_quantity

        units_to_sell = pct_sell_down * units

        units_to_sell -= units_to_sell % self.min_trade_increment

        if units_to_sell < self.min_order_size or units_to_sell == 0:
            units_to_sell = self.min_order_size

        new_steps = active_rule["steps"] + 1
        new_target_profit = active_rule["original_risk"] * new_steps
        new_target_unit_price = active_rule["current_target_price"] + new_target_profit

        # update rules
        new_sales_obj = {
            "units": filled_order.filled_unit_quantity,
            "sale_price": filled_order.filled_unit_price,
        }
        new_units_held = new_position_quantity
        new_units_sold = active_rule["units_sold"] + filled_order.filled_unit_quantity

        new_rule = active_rule.copy()
        # new_stop_loss = new_target_unit_price * new_rule["risk_point_new_stop_loss_pct"]

        # this will keep x% of the gain
        # first work out the gain
        gain = filled_unit_price - active_rule["purchase_price"]
        protected_gain = gain * active_rule["risk_point_new_stop_loss_pct"]
        new_stop_loss = active_rule["purchase_price"] + protected_gain

        # new_stop_loss = active_rule["current_target_price"] + ()

        new_rule["current_stop_loss"] = new_stop_loss
        new_rule["current_risk"] = new_target_profit
        new_rule["sales"].append(new_sales_obj)
        new_rule["units_held"] = new_units_held
        new_rule["units_sold"] = new_units_sold
        new_rule["steps"] += new_steps
        new_rule["current_target_price"] = new_target_unit_price
        new_rule["play_id"] = active_rule["play_id"]

        return {
            "new_rule": new_rule,
            "new_stop_loss": new_stop_loss,
            "new_units_to_sell": units_to_sell,
            "new_target_unit_price": new_target_unit_price,
            "new_units_held": new_units_held,
            "new_units_sold": new_units_sold,
        }
        print("banana")
