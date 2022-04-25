from itradeapi import (
    ITradeAPI,
    IOrderResult,
    IAccount,
    IPosition,
    IAsset,
    NotImplementedException,
)
from datetime import datetime

# CONSTANTS
MARKET_BUY = 1
MARKET_SELL = 2
LIMIT_BUY = 3
LIMIT_SELL = 4
STOP_LIMIT_BUY = 5
STOP_LIMIT_SELL = 6

ORDER_STATUS_SUMMARY_TO_ID = {
    "cancelled": {2, 7, 8, 9, 10},
    "open": {1, 3, 4, 5},
    "pending": {6},
}
ORDER_STATUS_ID_TO_SUMMARY = {
    1: "open",
    2: "cancelled",
    3: "open",
    4: "open",
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
    _raw_response: dict
    # _raw_request

    # TODO add requested like swyftx
    def __init__(self, order_object: dict):
        self._raw_response = order_object

        order_type = order_object["order_type"]
        order_type_text = ORDER_MAP_INVERTED[order_type]

        self.order_id = order_object["orderUuid"]
        if "BUY" in order_type_text:
            self.bought_symbol = order_object["secondary_asset"]
        else:
            # sells
            self.sold_symbol = order_object["primary_asset"]

        self.quantity = order_object["quantity"]
        self.quantity_symbol = order_object["quantity_asset"]

        self.trigger = order_object["trigger"]
        self.status = order_object["status"]
        self.status_text = ORDER_STATUS_TEXT[self.status]
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]

        created_time = order_object["created_time"]
        updated_time = order_object["updated_time"]


# concrete implementation of trade api for alpaca
class BackTestAPI(ITradeAPI):
    supported_crypto_symbols = []

    def __init__(self, real_money_trading=False):
        # set up asset lists
        self.assets = {
            "btc": None,
            "sol": None,
            "ada": None,
            "shib": None,
            "aapl": None,
            "bhp": None,
        }
        self.asset_list_by_symbol = self.assets

        self.supported_crypto_symbols = self._get_crypto_symbols()

        self.default_currency = "usd"

        self.balance = 10000
        self.starting_balance = self.balance
        self.bought = 0
        self.sold = 0
        self.holdings = 0
        self.position = 0

        self.assets_held = {}

    def _get_crypto_symbols(self):
        crypto_symbols = ["btc", "sol", "ada", "shib"]
        return crypto_symbols

    # not implemented
    def _structure_asset_dict_by_id(self, asset_dict):
        raise NotImplementedException(
            "Back Trade API does not order assets with a int key"
        )

    def get_account(self) -> Account:
        account = Account({"usd": 1000})
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

            positions.append({"symbol": symbol, "quantity": quantity})

        return positions

    def _translate_bars(self, bars):
        raise NotImplementedException

    def get_last_close(self, symbol: str):
        raise NotImplementedException

    def get_bars(self, symbol: str, start: str, end: str, interval: str):
        raise NotImplementedException("Back Trade API does not query for bars")

    # todo: basically everything after this!
    def _translate_order_types(self, order_type):
        raise NotImplementedException

    def order_create_by_value(self, *args, **kwargs):
        raise NotImplementedException

    def buy_order_limit(self, symbol: str, units: float, unit_price: float):
        response = {
            "order_type": 1,
            "orderUuid": "buy-abcdef",
            "secondary_asset": self.default_currency,
            "primary_asset": symbol,
            "quantity": units,
            "quantity_asset": symbol,
            "amount": None,
            "rate": None,
            "trigger": unit_price,
            "fees": 0,
            "status": 4,
            "created_time": datetime.fromisoformat("2022-04-04 10:00:00"),
            "updated_time": datetime.fromisoformat("2022-04-04 11:00:00"),
        }

        if self.assets_held.get(symbol) == None:
            self.assets_held[symbol] = []

        self.balance -= unit_price * units
        self.assets_held[symbol].append({"units": units, "unit_price": unit_price})
        # self._update_position()

        self.active_order = response

        return OrderResult(order_object=response)

    def buy_order_market():
        raise NotImplementedException

    def delete_order(self):
        raise NotImplementedException

    def list_orders(self):
        raise NotImplementedException

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
        unit_counter = units
        for order in self.assets_held[symbol]:
            # if unit_counter > order["units"]:
            if unit_counter - order["units"] >= 0:
                unit_counter -= order["units"]
                order["units"] = 0
            else:
                order["units"] -= unit_counter
                unit_counter = 0

        self.balance += unit_price * units

        response = {
            "order_type": 4,
            "orderUuid": "sell-abcdef",
            "secondary_asset": symbol,
            "primary_asset": self.default_currency,
            "quantity": units,
            "quantity_asset": self.default_currency,
            "amount": None,
            "rate": None,
            "trigger": unit_price,
            "fees": 0,
            "status": 4,
            "created_time": datetime.fromisoformat("2022-04-04 11:15:00"),
            "updated_time": datetime.fromisoformat("2022-04-04 11:20:00"),
        }

        self.active_order = None

        return OrderResult(order_object=response)

    def sell_order_market(self):
        raise NotImplementedException

    def order_create_by_units(self):
        raise NotImplementedException

    def order_delete(self):
        raise NotImplementedException

    def order_list(self):
        raise NotImplementedException

    def close_position(self, *args, **kwargs):
        raise NotImplementedException


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
