# external packages
from decimal import *
import logging
from math import floor

# my modules
from itradeapi import IOrderResult, Position

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

class OrderParameters():
    def __init__(self, precision:int=5, min_quantity_increment:Decimal=1, min_quantity:Decimal=1, min_price_increment:Decimal=0.001, max_play_value:Decimal=500, take_profit_pct:Decimal=0.25):
        self.precision=precision
        self.min_unit_increment=Decimal(min_quantity_increment)
        self.min_unit_quantity=Decimal(min_quantity)
        self.min_price_increment=Decimal(min_price_increment)
        self.max_play_value=Decimal(max_play_value)
        self.take_profit_pct=take_profit_pct
        #TODO some validation

class BuyPlan:
    class BuyPlanOrder():
        #def __init__(self, unit_price:Decimal, stop_loss_price:Decimal, unit_quantity:Decimal, purchase_value:Decimal, order_constraints:OrderConstraints):
        def __init__(self, symbol, balance, entry_unit, stop_loss_unit, order_parameters:OrderParameters):
            self.symbol=symbol
            self.requested_unit_price = entry_unit
            self.requested_stop_loss_price = stop_loss_unit

            # work out how much we're going to spend
            if balance < order_parameters.max_play_value:
                self.capital = balance
            else:
                self.capital = order_parameters.max_play_value

            # work out buy price and quantity
            self.order_unit_price = calc_unit_price(requested_unit_price=entry_unit,min_price_increment=order_parameters.min_price_increment)
            requested_units = self.capital / self.order_unit_price
            self.order_unit_quantity = calc_units_quantity(requested_units = requested_units, unit_price=self.order_unit_price, min_quantity_increment=order_parameters.min_unit_increment)
            self.order_purchase_value = self.order_unit_price * self.order_unit_quantity
            self.order_stop_loss_unit_price = stop_loss_unit
            self.risk_unit_price = self.order_unit_price - self.order_stop_loss_unit_price
            self.risk_value = self.order_unit_quantity * self.risk_unit_price

            # what can go wrong here?
            #  - can't afford the minimum number of units (should preclude trying to buy 0 units)
            #  - can't afford minimum order value
            #  - can't align to trade increment
            if self.order_unit_quantity < order_parameters.min_unit_quantity:
                # too few - failed order
                raise OrderQuantitySmallerThanMinimum(f"Play quantity of {self.order_unit_quantity} "
                f"is lower than minimum quantity of {order_parameters.min_unit_quantity}")

            # if we don't have enough money, bail out
            if self.capital < self.order_purchase_value:
                raise InsufficientBalance(f"Balance of {self.capital} is insufficient for order "
                f"value {self.order_purchase_value} ({self.order_unit_quantity} units at {self.order_unit_price})")

            if self.order_unit_quantity < order_parameters.min_unit_quantity:
                # too few - failed order
                raise OrderQuantitySmallerThanMinimum(f"Play quantity of {self.order_unit_quantity} "
                f"is lower than minimum quantity of {order_parameters.min_unit_quantity}")

            # notionally this should never trigger because of the check above
            if self.order_unit_quantity <= 0:
                # we're not buying any units
                raise ZeroUnitsOrdered(f"Units to purchase is {self.order_unit_quantity}. Capital "
                f"is {self.capital}, requested unit price was {entry_unit}")

    def __init__(
        self,
        symbol: str,
        balance: Decimal,
        play_id:str,
        last_low:Decimal,
        last_high:Decimal,
        entry_unit:Decimal,
        stop_loss_unit:Decimal,
        order_parameters:OrderParameters,
        profit_target: Decimal = 1.5,
    ):
        #getcontext().prec = order_constraints.precision
        getcontext().prec = 7
        entry_unit = Decimal(entry_unit)
        stop_loss_unit = Decimal(stop_loss_unit)
        profit_target = Decimal(profit_target)

        if order_parameters.min_price_increment == 0.0025:
            print("banana")

        self.success = False
        self.order_parameters = order_parameters
        self.play_id = play_id
        self.symbol = symbol

        draft_order = self.BuyPlanOrder(symbol=symbol, balance=balance, entry_unit=entry_unit, stop_loss_unit=stop_loss_unit, order_parameters=order_parameters)

        if draft_order.order_stop_loss_unit_price > last_low:
            raise StopPriceAlreadyMet(f"Stop unit price of {draft_order.order_stop_loss_unit_price:,} "
            f"would already trigger, since last low was {last_low:,}")

        take_profit_target = draft_order.order_unit_price * Decimal(1.25)
        if take_profit_target > last_high:
            raise TakeProfitAlreadyMet(f"Take profit price of {take_profit_target:,} would already trigger since last high was {last_high:,}")

        # if we got here, we have a viable play
        # start by grabbing the parts of the draft order we care about
        self.starting_capital = draft_order.capital
        self.original_unit_quantity = draft_order.order_unit_quantity
        self.original_unit_price = draft_order.order_unit_price
        self.original_purchase_value = draft_order.order_purchase_value
        self.original_stop_loss_unit_price = draft_order.order_stop_loss_unit_price

        self.steps = 0
        self.original_risk_unit = draft_order.risk_unit_price
        self.original_stop = stop_loss_unit
        self.target_profit = profit_target * draft_order.risk_unit_price
        self.target_price = draft_order.order_unit_price + self.target_profit

        # fmt: off
        f_capital = float(self.starting_capital)
        f_quantity = float(self.original_unit_quantity)
        f_price = round(float(self.original_unit_price), order_parameters.precision)
        f_value = round(float(self.original_purchase_value), order_parameters.precision)
        f_stop = round(float(self.original_stop_loss_unit_price), order_parameters.precision)

        log_wp.info(f"{self.symbol}\t- BUY PLAN REPORT")
        log_wp.info(f"{self.symbol}\t- Capital:\t\t${f_capital:,.2f}")
        log_wp.info(f"{self.symbol}\t- Units to buy:\t\t{f_quantity:,} units")
        log_wp.info(f"{self.symbol}\t- Entry point:\t\t${f_price:,}")
        log_wp.info(f"{self.symbol}\t- Buy order value:\t${f_value:,}")
        log_wp.info(f"{self.symbol}\t- Stop loss:\t\t${f_stop:,}")
        # fmt: on

        self.success = True
        self.buy_plan_order = draft_order

    def take_profit(
        self,
        filled_order: IOrderResult,
        active_rule: dict,
        held_units: Position,
    ):
        #filled_quantity = filled_order.filled_unit_quantity
        #filled_unit_price = filled_order.filled_unit_price
        #filled_value = filled_quantity * filled_unit_price

        # raise sell order
        #pct_sell_down = active_rule["risk_point_sell_down_pct"]
        
        next_profit_unit_price = calc_unit_price(requested_unit_price=self.target_price ,min_price_increment=self.order_parameters.min_price_increment)
        requested_units = self.order_parameters.take_profit_pct * held_units
        next_profit_unit_quantity = calc_units_quantity(requested_units=requested_units, unit_price=next_profit_unit_price, min_quantity_increment=self.order_parameters.min_unit_increment)

        #units_to_sell = 
        return

        units_to_sell -= units_to_sell % self.min_trade_increment

        if units_to_sell < self.min_order_size or units_to_sell == 0:
            units_to_sell = self.min_order_size

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

def calc_unit_price(requested_unit_price:Decimal, min_price_increment:Decimal, snap_price_down:bool=True):
    price_modulo = requested_unit_price % min_price_increment.normalize()
    calculated_unit_price = requested_unit_price - price_modulo

    # if we are rounding up, snap up to the next allowed value
    if not snap_price_down:
        calculated_unit_price = requested_unit_price - price_modulo + min_price_increment
        snap_text = "+"
    else:
        snap_text = "-"

    # trim down to just the decimal length i want
    calculated_unit_price = calculated_unit_price.normalize()

    log_wp.debug(f"Unit price will be {calculated_unit_price:,} "
    f"vs requested price {requested_unit_price.normalize():,}")
    log_wp.log(9, f"API won't accept {price_modulo.normalize():,}, "
    f"snap was {snap_text}{min_price_increment.normalize():,})")

    return calculated_unit_price

def calc_units_quantity(requested_units:Decimal, unit_price:Decimal, min_quantity_increment:Decimal):
    requested_units = Decimal(requested_units)
    units_to_discard = requested_units % min_quantity_increment.normalize()
    trimmed_units = requested_units - units_to_discard
    # TODO if i ever support partial/notional/fractional, wrap this in a check
    trimmed_units_floor = floor(trimmed_units)

    log_wp.debug(f"Volume will be {trimmed_units_floor:,} units")
    log_wp.log(9, f"Requested units was {requested_units.normalize():,}, "
    f"API won't accept {units_to_discard.normalize():,}, trimmed_units was "
    f"{trimmed_units.normalize():,}, rounded down")
    
    return trimmed_units_floor


if __name__ == "__main__":
    a_parameters = OrderParameters(min_quantity=.01,  min_quantity_increment=.1, min_price_increment=.1,)
    a=BuyPlan(symbol="AAVE", balance=100000, play_id="123", last_low=100, last_high=130, entry_unit=101.3462131, stop_loss_unit=100, order_parameters=a_parameters)
    az = a.take_profit("abc", "def", 4)
    az = a.take_profit("abc", "def", 3)
    az = a.take_profit("abc", "def", 2)
    az = a.take_profit("abc", "def", 1)

    b_parameters = OrderParameters(min_quantity=1,  min_quantity_increment=1, min_price_increment=0.000025000,)
    b=BuyPlan(symbol="BAT", balance=10000, play_id=123, last_low=62.6106, last_high=65.6106, entry_unit=0.402684, stop_loss_unit=60.6106, order_parameters=b_parameters)
    
    c_parameters = OrderParameters(min_quantity=100000, min_quantity_increment=100000, min_price_increment=0.00000001 )
    c=BuyPlan(symbol="SHIBUSD", balance=2000, play_id=123, last_low=62.6106, last_high=65.6106, entry_unit=0.00001057, stop_loss_unit=60.6106, order_parameters=c_parameters)

    d_parameters = OrderParameters(min_quantity=.001, min_quantity_increment=.0001, min_price_increment=0.025 )
    d=BuyPlan(symbol="BCH", balance=2000, play_id=123, last_low=62.6106, last_high=250, entry_unit=179.24123, stop_loss_unit=60.6106, order_parameters=d_parameters)

    print("banana")
    
