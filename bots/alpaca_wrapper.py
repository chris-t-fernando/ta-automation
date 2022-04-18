from itradeapi import (
    ITradeAPI,
    IOrderResult,
    IAccount,
    IPosition,
    IAsset,
    NotImplementedException,
)
import yfinance as yf
from datetime import datetime
from alpaca_trade_api import REST
import pandas as pd

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

    def __init__(self, response: dict, orders_create_object):
        self._raw_response = response
        self._raw_request = orders_create_object

        order_type = response["order"]["order_type"]
        order_type_text = ORDER_MAP_INVERTED[order_type]

        self.order_id = response["orderUuid"]
        if "BUY" in order_type_text:
            self.bought_symbol = orders_create_object.data["secondary"]
            self.bought_id = response["order"]["secondary_asset"]
        else:
            # sells
            self.sold_symbol = orders_create_object.data["primary"]
            self.sold_id = response["order"]["primary_asset"]

        self.quantity = response["order"]["quantity"]
        self.quantity_symbol = orders_create_object.data["assetQuantity"]
        self.quantity_id = response["order"]["quantity_asset"]

        self.trigger = response["order"]["trigger"]
        self.status = response["order"]["status"]
        self.status_text = ORDER_STATUS_TEXT[self.status]
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]

        created_time = response["order"]["created_time"]
        updated_time = response["order"]["updated_time"]


# concrete implementation of trade api for alpaca
class AlpacaAPI(ITradeAPI):
    supported_crypto_symbols = []

    def __init__(
        self, alpaca_key_id: str, alpaca_secret_key: str, real_money_trading=False
    ):
        # self.order_types = ORDER_TYPES
        if real_money_trading:
            base_url = "https://api.alpaca.markets"
        else:
            base_url = "https://paper-api.alpaca.markets"

        self.api = REST(
            key_id=alpaca_key_id,
            secret_key=alpaca_secret_key,
            base_url=base_url,
        )

        # set up asset lists
        self.assets = self.api.list_assets()
        # self.asset_list_by_id = self._structure_asset_dict_by_id(assets)
        self.asset_list_by_symbol = self._structure_asset_dict_by_symbol(self.assets)

        self.supported_crypto_symbols = self._get_crypto_symbols()

        self.default_currency = "usd"

    def _get_crypto_symbols(self):
        crypto_symbols = []
        for asset in self.assets:
            if asset._raw["class"] == "crypto":
                crypto_symbols.append(asset._raw["symbol"])

        return crypto_symbols

    # not implemented
    def _structure_asset_dict_by_id(self, asset_dict):
        raise NotImplementedException("Alpaca does not order assets with a int key")

    def _structure_asset_dict_by_symbol(self, asset_dict):
        return_dict = {}
        for asset in asset_dict:
            # code
            # name
            return_dict[asset.symbol] = asset

    def get_account(self) -> Account:
        request = self.api.get_account()
        currency = request.currency
        currency = currency.lower()
        account = Account({currency: float(request.cash)})
        # account.USD = account.cash
        return account

    def get_position(self, symbol):
        for position in self.list_positions():
            if position.symbol == symbol:
                return position
        return Position(symbol=symbol, quantity=0)

    def list_positions(self):
        # symbol, quantity
        positions = []
        for position in self.api.list_positions():
            positions.append(Position(symbol=position.symbol, quantity=position.qty))
        return positions

    def _translate_bars(self, bars):
        ...

    def get_last_close(self, symbol: str):
        history = yf.Ticker(symbol).history(interval="1m", actions=False)
        return history["Close"].iloc[-1]

    def get_bars(self, symbol: str, start: str, end: str, interval: str):
        return yf.Ticker(symbol).history(
            start=start, end=end, interval=interval, actions=False
        )

    # todo: basically everything after this!
    def _translate_order_types(self, order_type):
        if order_type == "MARKET_BUY":
            return "buy"
        elif order_type == "MARKET_SELL":
            return "sell"
        else:
            raise NotImplementedException

    def order_create_by_value(self, *args, **kwargs):
        # todo - normalise this!
        if kwargs.get("order_type") != None:
            side = kwargs.get("order_type")
            del kwargs["order_type"]
            kwargs["side"] = side
        else:
            for arg in args:
                if arg == "buy" or arg == "sell":
                    arg = self._translate_order_types(arg)

            # side = args[3]

        return self.api.submit_order(*args, **kwargs)

    def buy_order_limit(self):
        ...

    def buy_order_market(self):
        ...

    def delete_order(self):
        ...

    def list_orders(self):
        ...

    def sell_order_limit(self):
        ...

    def sell_order_market(self):
        ...

    def order_create_by_units(self):
        ...

    def order_delete(self):
        ...

    def order_list(self):
        ...

    def close_position(self, *args, **kwargs):
        return self.api.close_position(self, *args, **kwargs)


if __name__ == "__main__":
    import boto3

    ssm = boto3.client("ssm")
    api_key = (
        ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )
    secret_key = (
        ssm.get_parameter(Name="/tabot/alpaca/security_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    api = AlpacaAPI(alpaca_key_id=api_key, alpaca_secret_key=secret_key)
    api.get_account()
    api.list_positions()
    print("banana")
