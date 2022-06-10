# external packages
from decimal import Decimal
import logging
from math import floor, log10

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


class OrderQuantitySmallerThanMinimum(Exception):...
class OrderValueSmallerThanMinimum(Exception):...
class ZeroUnitsOrdered(Exception):...
class InsufficientBalance(Exception):...
class StopPriceAlreadyMet(Exception):...
class TakeProfitAlreadyMet(Exception):...

class BuyPlan:
    def __init__(
        self,
        symbol: str,
        df,
        balance: float,
        play_id:str,
        profit_target: float = 1.5,
        notional_units: bool = False,
        precision: int = 3,
        min_quantity_increment: float = 1,
        min_quantity: float = 1,
        min_price_increment: float = 0.001,
        max_play_value: float = 500,
    ):
        if min_price_increment == 0.0025:
            print("banana")
        self.success = False
        self.min_quantity_increment = min_quantity_increment
        self.min_quantity = min_quantity
        self.min_price_increment = min_price_increment
        self.precision = precision
        self.max_play_value = max_play_value
        self.play_id = play_id

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
        entry_unit = Decimal(df.Close.iloc[-1])
        entry_unit_trim = entry_unit % Decimal(min_price_increment)
        #self.entry_unit=round(float(entry_unit-entry_unit_trim),precision)
        self.entry_unit=entry_unit-entry_unit_trim
        self.entry_unit = self.hacky_float(self.entry_unit)

        self.stop_unit = stop_unit

        self.last_low = df.Low.iloc[-1]
        self.last_high = df.High.iloc[-1]

        if self.stop_unit > self.last_low:
            raise StopPriceAlreadyMet(f"Stop unit price of {self.stop_unit} would already trigger since last low was {self.last_low}")

        if self.entry_unit * 1.25 < self.last_high:
            raise TakeProfitAlreadyMet(f"Take profit price of {self.entry_unit * 1.25} would already trigger since last high was {self.last_high}")

        units = self.capital / self.entry_unit
        units_trim = units % min_quantity_increment
        self.units = int(units - units_trim)

        if max_play_value < self.entry_unit * min_quantity:
            # we can't afford to buy any units
            raise OrderValueSmallerThanMinimum(f"Play value of {max_play_value} is lower than entry unit price of {self.entry_unit} * minimum units {min_quantity}")

        if self.units < min_quantity:
            # too few - failed order
            raise OrderQuantitySmallerThanMinimum(f"Play quantity of {self.units} is lower than minimum quantity of {min_quantity}")

        # if we don't have enough money, bail out
        if self.entry_unit * self.units > self.capital:
            raise InsufficientBalance(f"Balance of {self.capital} is insufficient to purchase {self.units} units at {self.entry_unit}")

        #if self.units == 0:
        #    # we're not buying any units
        #    raise ZeroUnitsOrdered(f"Units to purchase is 0. Maybe due to floor? Calculated units was {units}, minimum trade increment is {min_quantity_increment}")

        
        self.steps = 0
        self.risk_unit = self.entry_unit - self.stop_unit
        self.risk_value = self.units * self.risk_unit
        self.target_profit = profit_target * self.risk_unit
        self.original_risk_unit = self.risk_unit
        self.original_stop = stop_unit

        #self.entry_unit = round(self.entry_unit, precision)
        self.target_price = self.entry_unit + self.target_profit

        # fmt: off
        log_wp.info(f"{self.symbol}\t- BUY PLAN REPORT")
        log_wp.info(f"{self.symbol}\t- Strength:\t\tNot sure how I want to do this yet")
        log_wp.info(f"{self.symbol}\t- MACD:\t\t\t{self.blue_cycle_macd}")
        log_wp.info(f"{self.symbol}\t- Signal:\t\t{self.blue_cycle_signal}")
        log_wp.info(f"{self.symbol}\t- Histogram:\t\t{self.blue_cycle_histogram}")
        log_wp.info(f"{self.symbol}\t- Capital:\t\t${self.capital:,.2f}")
        log_wp.info(f"{self.symbol}\t- Units to buy:\t\t{self.units:,} units")
        log_wp.info(f"{self.symbol}\t- Entry point:\t\t${self.f_float(self.entry_unit)}")
        log_wp.info(f"{self.symbol}\t- Stop loss:\t\t${self.f_float(stop_unit)}")
        log_wp.info(f"{self.symbol}\t- Cycle began:\t\t{self.intervals_since_stop} intervals ago on {stop_unit_date}")
        log_wp.info(f"{self.symbol}\t- Unit profit:\t\t${self.f_float(self.target_profit)} ({round(self.target_profit/self.entry_unit*100,1)}% of unit cost)")
        log_wp.info(f"{self.symbol}\t- Target price:\t\t${self.f_float(self.target_price)} ({round(self.target_price/self.capital*100,1)}% of capital)")
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

        units_to_sell -= units_to_sell % self.min_quantity_increment
        units_to_sell = floor(units_to_sell)

        if units_to_sell < self.min_quantity or units_to_sell == 0:
            units_to_sell = new_position_quantity

        new_steps = active_rule["steps"] + 1
        new_target_profit = active_rule["original_risk"] * new_steps
        new_target_unit_price = active_rule["current_target_price"] + new_target_profit
        new_target_unit_price = round(new_target_unit_price)
        new_target_unit_price_trim = new_target_unit_price % self.min_price_increment
        new_target_unit_price_trim=new_target_unit_price-new_target_unit_price_trim        

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

        # TODO what in tarnation was i doing here
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

    def hacky_float(self, dec:Decimal)->float:
        string_dec = str(dec)
        dot_at = string_dec.find(".") + 1
        if dot_at == 0:
            # there isn't a . in the decimal
            return float(dec)

        zeroes_to_keep = abs(int(log10(abs(self.min_price_increment))))
        truncate_at = dot_at + zeroes_to_keep
        truncated_string = string_dec[:truncate_at]
        back_to_float = float(truncated_string)
        return back_to_float

    def f_float(self, the_float):
        zeroes_to_keep = abs(int(log10(abs(self.min_price_increment))))
        return "%0.*f" % (zeroes_to_keep, the_float)

        a=hacky_float(Decimal(124.123123177456745623), min_price_increment=0.00000001)
        b=hacky_float(Decimal(124.123123177456745623), min_price_increment=0.00001)
        c=hacky_float(Decimal(124.123123177456745623), min_price_increment=0.001)
        d=hacky_float(Decimal(124.123123177456745623), min_price_increment=1)
        e=hacky_float(Decimal(124), min_price_increment=1)
        print("banana")