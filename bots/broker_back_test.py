from itradeapi import (
    ITradeAPI,
    IOrderResult,
    Account,
    Position,
    Asset,
    NotImplementedError,
)
from pandas import Timestamp
import logging
import pytz
import utils

# import datetime
# from dateutil.relativedelta import relativedelta

# update fixtures to match new OrderResults spec
# or maybe update fixtures to use raw API results and then just remember
# that i need to pipe them through?
# or maybe make a script to update them?!

log_wp = logging.getLogger(
    "backtest_api"
)  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("backtest_api.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.INFO)


# CONSTANTS
MARKET_BUY = 1
MARKET_SELL = 2
LIMIT_BUY = 3
LIMIT_SELL = 4
STOP_LIMIT_BUY = 5
STOP_LIMIT_SELL = 6

ORDER_STATUS_SUMMARY_TO_ID = {
    "cancelled": {2, 7, 8, 9, 10},
    "open": {1, 3, 5},
    "pending": {6},
    "filled": {4},
}
ORDER_STATUS_ID_TO_SUMMARY = {
    1: "open",
    2: "cancelled",
    3: "open",
    4: "filled",
    5: "open",
    6: "pending",
    7: "cancelled",
    8: "cancelled",
    9: "cancelled",
    10: "cancelled",
}
ORDER_STATUS_TEXT = {
    1: "Open",
    2: "Insufficient balance",
    3: "Partially filled",
    4: "Filled",
    5: "Pending",
    6: "User cancelled",
    7: "Unknown error",
    8: "Cancelled by system",
    9: "Failed - below minimum trading amount",
    10: "Refunded",
}

ORDER_MAP = {
    "MARKET_BUY": MARKET_BUY,
    "MARKET_SELL": MARKET_SELL,
    "LIMIT_BUY": LIMIT_BUY,
    "LIMIT_SELL": LIMIT_SELL,
    "STOP_LIMIT_BUY": STOP_LIMIT_BUY,
    "STOP_LIMIT_SELL": STOP_LIMIT_SELL,
}
ORDER_MAP_INVERTED = {y: x for x, y in ORDER_MAP.items()}

INTERVAL_MAP = {
    "1m": "1Min",
    "5m": "5Min",
    "15m": "15Min",
    "1d": "1Day",
}


class OrderResult(IOrderResult):
    def __init__(self, response: dict):
        self._raw_response = response

        self.order_type = response["order_type"]
        self.order_type_text = ORDER_MAP_INVERTED[self.order_type]
        self.order_id = response["orderUuid"]
        self.symbol = response["symbol"]

        if self.order_type == LIMIT_BUY or self.order_type == LIMIT_SELL:
            self.ordered_unit_quantity = float(response["quantity"])
            self.ordered_unit_price = float(response["limit_price"])
            self.ordered_total_value = (
                self.ordered_unit_quantity * self.ordered_unit_price
            )

        else:
            # market orders - so there is only quantity is known, not price or total value
            self.ordered_unit_quantity = float(response["quantity"])
            self.ordered_unit_price = None
            self.ordered_total_value = None

        self.filled_unit_quantity = None
        self.filled_unit_price = None
        self.filled_total_value = None

        self.status = response["status"]
        self.status_text = ORDER_STATUS_TEXT[self.status]
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]
        self.success = (
            response["status"] in ORDER_STATUS_SUMMARY_TO_ID["open"]
            or response["status"] in ORDER_STATUS_SUMMARY_TO_ID["filled"]
        )

        self.fees = response["feeAmount"]

        self.create_time = response["created_time"]
        self.update_time = response["updated_time"]

        open_statuses = ["open", "pending"]
        if self.status_summary in open_statuses:
            self.closed = False
        else:
            self.closed = True

        self.validate()

# concrete implementation of trade api for alpaca
class BackTestAPI(ITradeAPI):
    def __init__(
        self,
        real_money_trading=False,
        back_testing: bool = False,
        back_testing_balance: float = 100000,
    ):
        # set up asset lists
        self.assets = {
            "BTC": None,
            "SOL": None,
            "ADA": None,
            "SHIB": None,
            "AAPL": None,
            "BHP": None,
        }
        self.back_testing = back_testing
        self._balance = back_testing_balance

        self.asset_list_by_symbol = self.assets
        self.supported_crypto_symbols = self._get_crypto_symbols()

        self.default_currency = "USD"

        self._assets_held = {}
        self._orders = {}
        self._inactive_orders = []
        self._bars = {}

    def get_broker_name(self):
        return "back_test"

    def _get_crypto_symbols(self):
        # crypto_symbols = ["BTC", "SOL", "ADA", "SHIB"]
        crypto_symbols = [
            "AAVE-USD",
            "AVAX-USD",
            "BAT-USD",
            "BTC-USD",
            "BCH-USD",
            "LINK-USD",
            "DAI-USD",
            "DOGE-USD",
            "ETH-USD",
            "GRT-USD",
            "LTC-USD",
            "MKR-USD",
            "MATIC-USD",
            "PAXG-USD",
            "SHIB-USD",
            "SOL-USD",
            "SUSHI-USD",
            "USDT-USD",
            "TRX-USD",
            "UNI-USD",
            "WBTC-USD",
            "YFI-USD",
        ]

        return crypto_symbols

    # not implemented
    def _structure_asset_dict_by_id(self, asset_dict):
        raise NotImplementedError(
            "Back Trade API does not order assets with a int key"
        )

    def get_account(self) -> Account:
        account = Account({"USD": self._balance})
        return account

    def get_position(self, symbol):
        for position in self.list_positions():
            if position.symbol == symbol:
                return position
        return Position(symbol=symbol, quantity=0)

    def list_positions(self):
        # {symbol: , quantity}
        positions = []

        for symbol in self._assets_held:
            quantity = 0
            for order in self._assets_held[symbol]:
                quantity += order["units"]

            positions.append(Position(symbol=symbol, quantity=quantity))

        return positions

    def get_last_close(self, symbol: str):
        raise NotImplementedError

    def get_bars(self, symbol: str, start: str, end: str, interval: str):
        raise NotImplementedError("Back Trade API does not query for bars")

    def buy_order_limit(
        self,
        symbol: str,
        units: float,
        unit_price: float,
        back_testing_date: Timestamp,
    ):
        order_id = "buy-" + utils.generate_id()
        response = {
            "order_type": LIMIT_BUY,
            "orderUuid": order_id,
            "secondary_asset": self.default_currency,
            "primary_asset": symbol,
            "symbol": symbol,
            "quantity": units,
            "quantity_asset": self.default_currency,
            "limit_price": unit_price,
            "status": 1,
            "fees": 0,
            "feeAmount": 0,
            "created_time": back_testing_date,
            "updated_time": back_testing_date,
        }

        self._save_order(response=response)

        # get the order status so it can be returned
        order_result = self.get_order(
            order_id=order_id, back_testing_date=back_testing_date
        )

        return order_result

    def buy_order_market(self, symbol: str, units: float, back_testing_date: Timestamp):
        order_id = "buy-" + utils.generate_id()
        response = {
            "order_type": MARKET_BUY,
            "orderUuid": order_id,
            "secondary_asset": self.default_currency,
            "primary_asset": symbol,
            "symbol": symbol,
            "quantity": units,
            "quantity_asset": self.default_currency,
            "status": 1,
            "fees": 0,
            "feeAmount": 0,
            "created_time": back_testing_date,
            "updated_time": back_testing_date,
        }

        self._save_order(response=response)

        # get the order status so it can be returned
        order_result = self.get_order(
            order_id=order_id, back_testing_date=back_testing_date
        )

        return order_result

    def sell_order_limit(
        self,
        symbol: str,
        units: float,
        unit_price: float,
        back_testing_date: Timestamp,
    ):
        order_id = "sell-" + utils.generate_id()
        response = {
            "order_type": LIMIT_SELL,
            "orderUuid": order_id,
            "secondary_asset": symbol,
            "primary_asset": self.default_currency,
            "symbol": symbol,
            "quantity": units,
            "quantity_asset": self.default_currency,
            "limit_price": unit_price,
            "status": 1,
            "fees": 0,
            "feeAmount": 0,
            "created_time": back_testing_date,
            "updated_time": back_testing_date,
        }

        self._save_order(response=response)

        order_result = self.get_order(
            order_id=order_id, back_testing_date=back_testing_date
        )

        return order_result

    def sell_order_market(
        self, symbol: str, units: float, back_testing_date: Timestamp
    ):

        order_id = "sell-" + utils.generate_id()
        response = {
            "order_type": MARKET_SELL,
            "orderUuid": order_id,
            "secondary_asset": symbol,
            "primary_asset": self.default_currency,
            "symbol": symbol,
            "quantity": units,
            "quantity_asset": self.default_currency,
            "status": 1,
            "fees": 0,
            "feeAmount": 0,
            "created_time": back_testing_date,
            "updated_time": back_testing_date,
        }

        self._save_order(response=response)

        order_result = self.get_order(
            order_id=order_id, back_testing_date=back_testing_date
        )

        return order_result

    def close_position(self, symbol: str, back_testing_date: Timestamp):
        held_position = self.get_position(symbol)
        return self.sell_order_market(
            symbol=symbol,
            units=held_position.quantity,
            back_testing_date=back_testing_date,
        )

    def list_orders(self, symbol: str = None, symbols: list = None, after: str = None):
        if symbol and symbols:
            raise ValueError("Can't specify both 'symbol' and 'symbols' - choose one")

        if after:
            raise NotImplementedError(
                f"Parameter 'after' is not implemented in back_test_wrapper"
            )

        return_orders = []
        for ordered_symbol in self._orders:
            return_orders.append(self._orders[ordered_symbol])

        for order in self._inactive_orders:
            return_orders.append(order)

        if symbol or symbols:
            if symbol:
                symbols = [symbol]

            new_return_orders = []
            for order in return_orders:
                if order.symbol in symbols:
                    new_return_orders.append(order)

            return_orders = new_return_orders

        return return_orders

    def get_order(self, order_id: str, back_testing_date: Timestamp):
        # refresh order status first
        self._update_order_status(back_testing_date=back_testing_date)

        all_orders = self.list_orders()
        for order in all_orders:
            if order.order_id == order_id:
                return order

        return False

    def _save_order(self, response):
        if self._orders.get(response["symbol"]):
            raise ValueError(
                f'{response["symbol"]}: Already have an order open for this symbol'
            )
        self._orders[response["symbol"]] = OrderResult(response=response)

    def cancel_order(self, order_id, back_testing_date):
        symbol_to_delete = None
        for symbol in self._orders:
            if self._orders[symbol].order_id == order_id:
                if (
                    self._orders[symbol].status
                    in ORDER_STATUS_SUMMARY_TO_ID["cancelled"]
                    or self._orders[symbol].status
                    in ORDER_STATUS_SUMMARY_TO_ID["filled"]
                ):
                    log_wp.debug(
                        f"{symbol_to_delete}: Unable to delete order_id {order_id} from "
                        f"self._orders list since its already in {self._orders[symbol].status_summary} state"
                    )
                    return False
                symbol_to_delete = symbol

        if symbol_to_delete:
            # need to update the order to cancelled
            self._orders[symbol_to_delete].status = 6
            self._orders[symbol_to_delete].status_summary = ORDER_STATUS_ID_TO_SUMMARY[
                6
            ]
            self._orders[symbol_to_delete].status_text = ORDER_STATUS_TEXT[6]
            self._orders[symbol_to_delete].success = False
            self._orders[symbol_to_delete].update_time = back_testing_date

            # need to move the order to self._inactive_orders
            self._inactive_orders.append(self._orders[symbol_to_delete])
            del self._orders[symbol_to_delete]

            log_wp.debug(
                f"{symbol_to_delete}: Moved order_id {order_id} from self._orders to"
                f"self._inactive_orders"
            )
            return self.get_order(
                order_id=order_id, back_testing_date=back_testing_date
            )
        else:
            log_wp.warning(
                f"Tried to remove order_id {order_id} from self._orders but did not "
                f"find it - is it already closed?"
            )
            return False

    def _put_bars(self, symbol, bars):
        self._bars[symbol] = bars

    def _get_held_units(self, symbol):
        unit_count = 0
        paid = 0
        if not self._assets_held.get(symbol):
            return 0, 0

        for order in self._assets_held[symbol]:
            unit_count += order["units"]
            paid += order["units"] * order["unit_price"]

        return unit_count, paid

    def _update_order_status(self, back_testing_date):
        # loop through all the orders looking for whether they've been filled
        # assumes that this gets called with back_testing_date for every index in bars, since it only checks this index/back_testing_date
        filled_symbols = []

        # hacky way of avoiding deleting orders and raising RuntimeError for dict changed size during iteration
        orders_copy = self._orders.copy()
        for symbol in orders_copy:
            this_order = self._orders[symbol]
            # if the order is cancelled or filled ie. already actioned
            if (
                this_order.status in ORDER_STATUS_SUMMARY_TO_ID["cancelled"]
                or this_order.status in ORDER_STATUS_SUMMARY_TO_ID["filled"]
            ):
                self._inactive_orders.append(this_order)
                filled_symbols.append(symbol)
                log_wp.debug(
                    f"{symbol}: Skipping this symbol in _inactive_orders since the "
                    f"status is {ORDER_STATUS_ID_TO_SUMMARY[this_order.status]}"
                )
                continue

            try:
                check_index = self._bars[symbol].loc[back_testing_date]
            except KeyError as e:
                log_wp.debug(f"{symbol}: No data for {back_testing_date}")
                continue

            # if we got here, the order is not yet actioned
            if this_order.order_type == MARKET_BUY:
                # immediate fill - its just a question of how many units they bought
                log_wp.debug(
                    f"{symbol}: Starting fill for MARKET_BUY order {this_order.order_id}"
                )

                unit_price = round(
                    self._bars[symbol].Low.loc[back_testing_date],
                    self.get_precision(yf_symbol=symbol),
                )
                units_purchased = this_order.ordered_unit_quantity
                order_value = unit_price * units_purchased

                # don't process this order if it would send balance to negative
                if order_value > self._balance:
                    log_wp.warning(
                        f"{symbol}: Unable to fill {this_order.order_id} - order value "
                        f"is {order_value} but balance is only {self._balance}"
                    )
                    self.cancel_order(order_id=this_order.order_id)
                    continue

                # mark this order as filled
                this_order.status = 4
                this_order.status_text = ORDER_STATUS_TEXT[this_order.status]
                this_order.status_summary = ORDER_STATUS_ID_TO_SUMMARY[
                    this_order.status
                ]
                this_order.filled_unit_quantity = units_purchased
                this_order.filled_unit_price = unit_price
                this_order.filled_total_value = (
                    this_order.filled_unit_quantity * this_order.filled_unit_price
                )

                # instantiate this symbol in the held array - it gets populated in a couple lines
                if self._assets_held.get(symbol) == None:
                    self._assets_held[symbol] = []

                self._assets_held[symbol].append(
                    {
                        "units": this_order.filled_unit_quantity,
                        "unit_price": this_order.filled_unit_price,
                    }
                )

                # update balance
                self._balance = round(
                    self._balance
                    - (this_order.filled_unit_price * this_order.filled_unit_quantity),
                    15,
                )

                filled_symbols.append(symbol)

                log_wp.debug(
                    f"{symbol}: market_buy filled, {this_order.filled_unit_quantity} "
                    f"units at {this_order.filled_unit_price}, balance {self._balance}"
                )

            elif this_order.order_type == MARKET_SELL:
                log_wp.debug(
                    f"{symbol}: Starting fill for MARKET_SELL order {this_order.order_id}"
                )

                # how many of this symbol do we own? is it >= than the requested amount to sell?
                held, paid = self._get_held_units(symbol)

                if held < this_order.ordered_unit_quantity:
                    log_wp.warning(
                        f"{symbol}: Failed to fill order {this_order.order_id} - trying to "
                        f"sell {this_order.ordered_unit_quantity} units but only hold {held}"
                    )
                    self.cancel_order(order_id=this_order.order_id)
                    continue
                    # raise ValueError(
                    #    f"{symbol}: Hold {held} so can't sell {this_order.ordered_unit_quantity} units"
                    # )

                unit_price = round(
                    self._bars[symbol].High.loc[back_testing_date],
                    self.get_precision(yf_symbol=symbol),
                )

                # mark this order as filled
                this_order.status = 4
                this_order.status_text = ORDER_STATUS_TEXT[this_order.status]
                this_order.status_summary = ORDER_STATUS_ID_TO_SUMMARY[
                    this_order.status
                ]
                this_order.filled_unit_quantity = this_order.ordered_unit_quantity
                this_order.filled_unit_price = unit_price
                this_order.filled_total_value = (
                    this_order.filled_unit_quantity * this_order.filled_unit_price
                )

                self._do_sell(
                    quantity_to_sell=this_order.filled_unit_quantity, symbol=symbol
                )

                # update balance
                self._balance = round(
                    self._balance + round(this_order.filled_total_value, 2), 15
                )

                filled_symbols.append(symbol)

                log_wp.info(
                    f"{symbol}: market_sell filled, {this_order.filled_unit_quantity} "
                    f"units at {this_order.filled_unit_price}, balance {self._balance}"
                )

            elif this_order.order_type == LIMIT_BUY:
                if (
                    self._bars[symbol].Low.loc[back_testing_date]
                    < this_order.ordered_unit_price
                ):
                    log_wp.debug(
                        f"{symbol}: Starting fill for LIMIT_BUY order {this_order.order_id}"
                    )

                    # don't process this order if it would send balance to negative
                    order_value = (
                        this_order.ordered_unit_quantity * this_order.ordered_unit_price
                    )
                    if order_value > self._balance:
                        log_wp.warning(
                            f"{symbol}: Unable to fill {this_order.order_id} - order "
                            f"value is {order_value} but balance is only {self._balance}"
                        )
                        self.cancel_order(
                            order_id=this_order.order_id,
                            back_testing_date=back_testing_date,
                        )
                        continue

                    # mark this order as filled
                    this_order.status = 4
                    this_order.status_text = ORDER_STATUS_TEXT[this_order.status]
                    this_order.status_summary = ORDER_STATUS_ID_TO_SUMMARY[
                        this_order.status
                    ]
                    this_order.filled_unit_quantity = this_order.ordered_unit_quantity
                    this_order.filled_unit_price = round(
                        this_order.ordered_unit_price,
                        self.get_precision(yf_symbol=symbol),
                    )
                    this_order.filled_total_value = (
                        this_order.filled_unit_quantity * this_order.filled_unit_price
                    )

                    # instantiate this symbol in the held array - it gets populated in a couple lines
                    if self._assets_held.get(symbol) == None:
                        self._assets_held[symbol] = []

                    self._assets_held[symbol].append(
                        {
                            "units": this_order.filled_unit_quantity,
                            "unit_price": this_order.filled_unit_price,
                        }
                    )

                    # update balance
                    self._balance = round(
                        self._balance
                        - (
                            this_order.filled_unit_price
                            * this_order.filled_unit_quantity
                        ),
                        15,
                    )

                    filled_symbols.append(symbol)

                    log_wp.info(
                        f"{symbol}: limit_buy filled, {this_order.filled_unit_quantity} "
                        f"units at {this_order.filled_unit_price}, balance {self._balance}"
                    )

            elif this_order.order_type == LIMIT_SELL:
                if (
                    self._bars[symbol].High.loc[back_testing_date]
                    > this_order.ordered_unit_price
                ):
                    log_wp.debug(
                        f"{symbol}: Starting fill for LIMIT_SELL order {this_order.order_id}"
                    )
                    # how many of this symbol do we own? is it >= than the requested amount to sell?
                    held, paid = self._get_held_units(symbol)

                    if held < this_order.ordered_unit_quantity:
                        log_wp.debug(
                            f"{symbol}: Failed to fill order {this_order.order_id} - "
                            f"trying to sell {this_order.ordered_unit_quantity} units "
                            f"but only hold {held}"
                        )
                        self.cancel_order(
                            order_id=this_order.order_id,
                            back_testing_date=back_testing_date,
                        )
                        continue
                        # raise ValueError(
                        #    f"{symbol}: Hold {held} so can't sell {this_order.ordered_unit_quantity} units"
                        # )

                    # mark this order as filled
                    this_order.status = 4
                    this_order.status_text = ORDER_STATUS_TEXT[this_order.status]
                    this_order.status_summary = ORDER_STATUS_ID_TO_SUMMARY[
                        this_order.status
                    ]
                    this_order.filled_unit_quantity = this_order.ordered_unit_quantity
                    this_order.filled_unit_price = round(
                        self._bars[symbol].High.loc[back_testing_date],
                        self.get_precision(yf_symbol=symbol),
                    )
                    this_order.filled_total_value = (
                        this_order.filled_unit_quantity * this_order.filled_unit_price
                    )

                    self._do_sell(
                        quantity_to_sell=this_order.filled_unit_quantity, symbol=symbol
                    )

                    # update balance
                    self._balance = round(
                        self._balance + this_order.filled_total_value, 15
                    )

                    filled_symbols.append(symbol)

                    log_wp.info(
                        f"{symbol}: limit_sell filled, {this_order.filled_unit_quantity} "
                        f"units at {this_order.filled_unit_price}, balance {self._balance}"
                    )

        for symbol in filled_symbols:
            self._inactive_orders.append(self._orders[symbol])
            del self._orders[symbol]

    def _do_sell(self, quantity_to_sell, symbol):
        # if we don't hold any, return False
        if not self._assets_held.get(symbol):
            return False

        # check that we aren't trying to sell more than we actually hold
        actual_held = 0
        for order in self._assets_held[symbol]:
            actual_held += order["units"]
        if actual_held < quantity_to_sell:
            raise ValueError(
                f"{symbol}: Unable to remove {quantity_to_sell} units from holding of "
                f'{order["units"]}, since that would be less than 0'
            )

        # now start popping units from held
        # self._assets_held[symbol] is a list of objects that represent buy orders. when we sell, we need to pop/update these buy orders to remove them
        for order in self._assets_held[symbol]:
            if order["units"] - quantity_to_sell >= 0:
                # more units in this order than we need to sell
                order["units"] -= quantity_to_sell
                quantity_to_sell = 0
            else:
                quantity_to_sell -= order["units"]
                order["units"] = 0

            # nothing left to sell, no point iterating over remaining orders
            if quantity_to_sell == 0:
                break

        return True

    def get_precision(self, yf_symbol: str) -> int:
        if yf_symbol in self.supported_crypto_symbols:
            return 15
        return 3

    def get_asset(self, symbol: str):
        hardcoded_assets = {}
        hardcoded_assets["AAVE-USD"] = {
            "min_order_size": 0.01,
            "min_quantity_increment": 0.01,
            "min_price_increment": 0.1,
        }
        hardcoded_assets["AVAX-USD"] = {
            "min_order_size": 0.1,
            "min_quantity_increment": 0.1,
            "min_price_increment": 0.0005,
        }
        hardcoded_assets["BAT-USD"] = {
            "min_order_size": 1,
            "min_quantity_increment": 1,
            "min_price_increment": 0.000025,
        }
        hardcoded_assets["BTC-USD"] = {
            "min_order_size": 0.0001,
            "min_quantity_increment": 0.0001,
            "min_price_increment": 1,
        }
        hardcoded_assets["BCH-USD"] = {
            "min_order_size": 0.001,
            "min_quantity_increment": 0.0001,
            "min_price_increment": 0.025,
        }
        hardcoded_assets["LINK-USD"] = {
            "min_order_size": 0.1,
            "min_quantity_increment": 0.1,
            "min_price_increment": 0.0005,
        }
        hardcoded_assets["DAI-USD"] = {
            "min_order_size": 0.1,
            "min_quantity_increment": 0.1,
            "min_price_increment": 0.0001,
        }
        hardcoded_assets["DOGE-USD"] = {
            "min_order_size": 1,
            "min_quantity_increment": 1,
            "min_price_increment": 0.0000005,
        }
        hardcoded_assets["ETH-USD"] = {
            "min_order_size": 0.001,
            "min_quantity_increment": 0.001,
            "min_price_increment": 0.1,
        }
        hardcoded_assets["GRT-USD"] = {
            "min_order_size": 1,
            "min_quantity_increment": 1,
            "min_price_increment": 0.00005,
        }
        hardcoded_assets["LTC-USD"] = {
            "min_order_size": 0.01,
            "min_quantity_increment": 0.01,
            "min_price_increment": 0.005,
        }
        hardcoded_assets["MKR-USD"] = {
            "min_order_size": 0.001,
            "min_quantity_increment": 0.001,
            "min_price_increment": 0.5,
        }
        hardcoded_assets["MATIC-USD"] = {
            "min_order_size": 10,
            "min_quantity_increment": 10,
            "min_price_increment": 0.000001,
        }
        hardcoded_assets["PAXG-USD"] = {
            "min_order_size": 0.0001,
            "min_quantity_increment": 0.0001,
            "min_price_increment": 0.1,
        }
        hardcoded_assets["SHIB-USD"] = {
            "min_order_size": 100000,
            "min_quantity_increment": 100000,
            "min_price_increment": 0.00000001,
        }
        hardcoded_assets["SOL-USD"] = {
            "min_order_size": 0.01,
            "min_quantity_increment": 0.01,
            "min_price_increment": 0.0025,
        }
        hardcoded_assets["SUSHI-USD"] = {
            "min_order_size": 0.5,
            "min_quantity_increment": 0.5,
            "min_price_increment": 0.0001,
        }
        hardcoded_assets["USDT-USD"] = {
            "min_order_size": 0.01,
            "min_quantity_increment": 0.01,
            "min_price_increment": 0.0001,
        }
        hardcoded_assets["TRX-USD"] = {
            "min_order_size": 1,
            "min_quantity_increment": 1,
            "min_price_increment": 0.0000025,
        }
        hardcoded_assets["UNI-USD"] = {
            "min_order_size": 0.1,
            "min_quantity_increment": 0.1,
            "min_price_increment": 0.001,
        }
        hardcoded_assets["WBTC-USD"] = {
            "min_order_size": 0.0001,
            "min_quantity_increment": 0.0001,
            "min_price_increment": 1,
        }
        hardcoded_assets["YFI-USD"] = {
            "min_order_size": 0.001,
            "min_quantity_increment": 0.001,
            "min_price_increment": 5,
        }

        # asset = self.get_asset(symbol=symbol)

        # if hasattr(asset, "min_order_size"):
        if symbol in hardcoded_assets.keys():
            min_order_size = hardcoded_assets[symbol]["min_order_size"]
            min_trade_increment = hardcoded_assets[symbol]["min_quantity_increment"]
            min_price_increment = hardcoded_assets[symbol]["min_price_increment"]
        else:
            min_order_size = 1
            min_trade_increment = 1
            min_price_increment = 0.001

        return Asset(
            symbol=symbol,
            min_quantity=min_order_size,
            min_quantity_increment=min_trade_increment,
            min_price_increment=min_price_increment,
        )

    def validate_symbol(self, symbol:str):
        # TODO: this is not right - but the whole handling of symbols is busted in back testing
        return True


if __name__ == "__main__":
    starting_balance = 10000
    api = BackTestAPI(back_testing=True, back_testing_balance=starting_balance)

    import pandas as pd

    import utils

    f_file = "bots/tests/fixtures/order_buy_active.txt"
    f = open(f_file, "r")
    order_file = f.read()
    unpickled_order = utils.unpickle(order_file)

    data = pd.read_csv(
        f"bots/tests/fixtures/symbol_chris.csv",
        index_col=0,
        parse_dates=True,
        infer_datetime_format=True,
    )

    api._put_bars("CHRIS", data)
    back_testing_date = Timestamp("2022-05-09 14:50:00").tz_localize(pytz.utc)

    api.get_account()
    api.list_positions()

    api.buy_order_market(symbol="CHRIS", units=10, back_testing_date=back_testing_date)
    api.sell_order_market(symbol="CHRIS", units=4, back_testing_date=back_testing_date)
    api.sell_order_limit(
        symbol="CHRIS", units=3, unit_price=20, back_testing_date=back_testing_date
    )
    # api.buy_order_limit(
    #    symbol="CHRIS", units=3, unit_price=150, back_testing_date=back_testing_date
    # )
    # api.buy_order_limit(
    #    symbol="CHRIS", units=4, unit_price=150, back_testing_date=back_testing_date
    # )
    # api.buy_order_limit(
    #    symbol="CHRIS", units=3, unit_price=150, back_testing_date=back_testing_date
    # )
    # api.sell_order_limit(
    #    symbol="CHRIS", units=3.5, unit_price=151, back_testing_date=back_testing_date
    # )
    # api.sell_order_limit(
    #    symbol="CHRIS", units=6.5, unit_price=151, back_testing_date=back_testing_date
    # )

    print(f"Profit: {round(api._balance - starting_balance,2)}")
    api.list_orders()
    print("banana")
