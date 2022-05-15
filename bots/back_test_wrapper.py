from itradeapi import (
    ITradeAPI,
    IOrderResult,
    IAccount,
    IPosition,
    IAsset,
    NotImplementedException,
)
from datetime import datetime
import logging

log_wp = logging.getLogger(
    "backtest_api"
)  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("macd.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.INFO)


# csv_wp = logging.getLogger(
#    "backtest_api_csv"
# )  # or pass an explicit name here, e.g. "mylogger"
# csv_hdlr = logging.StreamHandler()
# csv_fhdlr = logging.FileHandler("macd.csv")
# csv_wp.addHandler(csv_hdlr)
# csv_wp.addHandler(csv_fhdlr)
# csv_wp.setLevel(logging.DEBUG)


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


class Asset(IAsset):
    symbol: str
    balance: float

    def __init__(self, symbol, balance):
        self.symbol = symbol
        self.balance = balance


class Account(IAccount):
    assets: dict

    def __init__(self, assets: dict):
        self.assets = assets


class Position(IPosition):
    symbol: str
    quantity: float

    def __init__(self, symbol, quantity):
        self.symbol = symbol
        self.quantity = quantity


class OrderResult(IOrderResult):
    order_id: str
    sold_symbol: str
    bought_symbol: str
    quantity: float
    quantity_symbol: str
    quantity_id: int
    trigger: float
    status: int
    status_text: str
    status_summary: str
    order_type: int
    order_type_text: str
    created_time: int
    updated_time: int
    success: bool
    requested_units: float
    requested_unit_price: float
    requested_total_value: float
    _raw_response: dict
    # _raw_request

    def __init__(self, response: dict):
        self._raw_response = response

        order_type = response["order_type"]
        order_type_text = ORDER_MAP_INVERTED[order_type]

        self.order_id = response["orderUuid"]
        if "BUY" in order_type_text:
            self.bought_symbol = response["secondary_asset"]
        else:
            # sells
            self.sold_symbol = response["primary_asset"]

        # quantity is what you're paying with - only known if its a limit order
        if order_type == LIMIT_BUY or order_type == LIMIT_SELL:
            self.order_value = float(response["limit_price"]) * float(response["qty"])
            self.limit_price = float(response["limit_price"])

        else:
            self.order_value = None
            self.limit_price = None

        self.quantity = response["quantity"]
        self.quantity_symbol = response["quantity_asset"]

        self.trigger = response["trigger"]
        self.status = response["status"]
        self.status_text = ORDER_STATUS_TEXT[self.status]
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]
        self.success = (
            response["status"] in ORDER_STATUS_SUMMARY_TO_ID["open"]
            or response["status"] in ORDER_STATUS_SUMMARY_TO_ID["filled"]
        )

        self.units = response["amount"]
        self.unit_price = response["rate"]
        self.fees = response["feeAmount"]
        self.total_value = self.units * self.unit_price

        self.requested_units = response["amount"]
        self.requested_unit_price = response["rate"]
        self.requested_total_value = self.units * self.unit_price

        self.create_time = response["created_time"]
        self.update_time = response["updated_time"]


# concrete implementation of trade api for alpaca
class BackTestAPI(ITradeAPI):
    supported_crypto_symbols = []

    def __init__(self, real_money_trading=False, back_testing: bool = False):
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
        self.asset_list_by_symbol = self.assets

        self.supported_crypto_symbols = self._get_crypto_symbols()

        self.default_currency = "USD"

        self.balance = 10000
        self.starting_balance = self.balance
        self.bought = 0
        self.sold = 0
        self.holdings = 0
        self.position = 0

        self.assets_held = {}

    def get_broker_name(self):
        return "back_test"

    def _get_crypto_symbols(self):
        crypto_symbols = ["BTC", "SOL", "ADA", "SHIB"]
        return crypto_symbols

    # not implemented
    def _structure_asset_dict_by_id(self, asset_dict):
        raise NotImplementedException(
            "Back Trade API does not order assets with a int key"
        )

    def get_account(self) -> Account:
        account = Account({"USD": self.balance})
        return account

    def get_position(self, symbol):
        for position in self.list_positions():
            if position.symbol == symbol:
                return position
        return Position(symbol=symbol, quantity=0)

    def list_positions(self):
        # {symbol: , quantity}
        positions = []

        for symbol in self.assets_held:
            quantity = 0
            for order in self.assets_held[symbol]:
                quantity += order["units"]

            positions.append(Position(symbol=symbol, quantity=quantity))

        return positions

    def _translate_bars(self, bars):
        raise NotImplementedException

    def get_last_close(self, symbol: str):
        raise NotImplementedException

    def get_bars(self, symbol: str, start: str, end: str, interval: str):
        raise NotImplementedException("Back Trade API does not query for bars")

    def _translate_order_types(self, order_type):
        raise NotImplementedException

    # the backtest wrapper needs access to the data and to the date being assessed
    # so that it can determine whether a limit order has been filled or not
    # how are you going to do this??

    def buy_order_limit(self, symbol: str, units: float, unit_price: float):
        response = {
            "order_type": LIMIT_BUY,
            "orderUuid": "buy-abcdef",
            "secondary_asset": self.default_currency,
            "primary_asset": symbol,
            "quantity": units,
            "quantity_asset": symbol,
            "limit_price": unit_price,
            "qty": units,
            "amount": units,
            "rate": unit_price,
            "trigger": unit_price,
            "fees": 0,
            "status": 4,
            "feeAmount": 0,
            "created_time": datetime.fromisoformat("2022-04-04 10:00:00"),
            "updated_time": datetime.fromisoformat("2022-04-04 11:00:00"),
        }

        if self.assets_held.get(symbol) == None:
            self.assets_held[symbol] = []

        self.balance -= unit_price * units
        self.assets_held[symbol].append({"units": units, "unit_price": unit_price})
        # self._update_position()

        self.active_order = response

        return OrderResult(response=response)

    def buy_order_market():
        raise NotImplementedException

    def delete_order(self):
        raise NotImplementedException

    def list_orders(self):
        return_orders = []
        for symbol in self._orders:
            return_orders.append(OrderResult(response=self._orders[symbol]))
        return return_orders

    def get_order(self, order_id: str):
        for symbol in self._orders:
            if self._orders[symbol]["order_id"] == order_id:
                return OrderResult(response=self._orders[symbol])
        return False

    def sell_order_limit(self, symbol: str, units: float, unit_price: float):
        # how many of this symbol do we own? is it >= than the requested amount to sell?
        unit_count = 0
        paid = 0
        for order in self.assets_held[symbol]:
            unit_count += order["units"]
            paid += order["units"] * order["unit_price"]

        if unit_count < units:
            raise ValueError(
                "Back Test API is trying to sell more units than you own..."
            )

        # now start popping units from held
        # self.assets_held[symbol] is a list of objects that represent buy orders. when we sell, we need to pop/update these buy orders to remove them
        unit_counter = units
        for order in self.assets_held[symbol]:
            if unit_counter - order["units"] >= 0:
                unit_counter -= order["units"]
                order["units"] = 0
            else:
                order["units"] -= unit_counter
                unit_counter = 0

        if unit_counter == 0:
            self.active_order = None

        self.balance += unit_price * units

        response = {
            "order_type": 4,
            "orderUuid": "sell-abcdef",
            "secondary_asset": symbol,
            "primary_asset": self.default_currency,
            "quantity": units,
            "quantity_asset": self.default_currency,
            "amount": units,
            "rate": unit_price,
            "trigger": unit_price,
            "fees": 0,
            "feeAmount": 0,
            "status": 4,
            "created_time": datetime.fromisoformat("2022-04-04 11:15:00"),
            "updated_time": datetime.fromisoformat("2022-04-04 11:20:00"),
        }

        # csv_wp.debug(f"SELL,LIMIT,{symbol},{units},{unit_price}")

        self._save_order(response)

        return OrderResult(order_object=response)

    def sell_order_market(
        self, symbol: str, units: float, back_testing_unit_price: float
    ):
        held_position = self.get_position(symbol)
        if held_position.quantity < units:
            raise ValueError(
                f"{symbol}: requested to sell {units} but only hold {held_position.quantity}"
            )

        unit_counter = units

        for order in self.assets_held[symbol]:
            if unit_counter - order["units"] >= 0:
                unit_counter -= order["units"]
                order["units"] = 0
            else:
                order["units"] -= unit_counter
                unit_counter = 0

        if unit_counter == 0:
            self.active_order = None
        self.balance += back_testing_unit_price * units

        response = {
            "order_type": 4,
            "orderUuid": "sell-abcdef",
            "secondary_asset": symbol,
            "primary_asset": self.default_currency,
            "quantity": units,
            "quantity_asset": self.default_currency,
            "amount": units,
            "rate": back_testing_unit_price,
            "trigger": back_testing_unit_price,
            "fees": 0,
            "feeAmount": 0,
            "status": 4,
            "created_time": datetime.fromisoformat("2022-04-04 11:15:00"),
            "updated_time": datetime.fromisoformat("2022-04-04 11:20:00"),
        }
        log_wp.warning(
            f"{symbol}: Sold {units}  at market value of {back_testing_unit_price} (total value {back_testing_unit_price * units}) (back_testing={self.back_testing})"
        )
        # csv_wp.debug(f"SELL,MARKET,{symbol},{units},{back_testing_unit_price}")
        # self._save_order(response=response)

        del self._orders[symbol]

        return OrderResult(order_object=response)

    def close_position(self, symbol: str, back_testing_unit_price: float):
        held_position = self.get_position(symbol)
        return self.sell_order_market(
            symbol=symbol,
            units=held_position.quantity,
            back_testing_unit_price=back_testing_unit_price,
        )

    def _save_order(self, response):
        self._orders[response["secondary_asset"]] = response

    def _get_order(self, symbol):
        return self._orders[symbol]

    def _delete_order(self, symbol):
        if self._orders.get(symbol):
            del self._orders[symbol]
            log_wp.debug(f"{symbol}: Removed from self._orders list")
            return True
        else:
            log_wp.warning(
                f"{symbol}: Tried to remove from self._orders but did not find symbol"
            )
            return False


if __name__ == "__main__":
    api = BackTestAPI()
    api.get_account()
    api.list_positions()
    api.buy_order_limit("btc", 10, 500)
    api.list_positions()
    api.sell_order_limit("btc", 3.5, 600)
    api.sell_order_limit("btc", 3.5, 600)
    api.sell_order_limit("btc", 3, 600)
    print(f"Profit: {api.balance - api.starting_balance}")
    print("banana")
