import pyswyft
from pyswyft.endpoints import accounts, history, markets, orders
from itradeapi import (
    ITradeAPI,
    IOrderResult,
    IAccount,
    IPosition,
    NotImplementedException,
)

# from pandas import DataFrame as df


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


# return objects
class Account(IAccount):
    positions: list

    def __init__(self, positions):
        self.positions = positions


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
    _raw_request: orders.OrdersCreate

    def __init__(self, response: dict, orders_create_object: orders.OrdersCreate):
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


# concrete class
class Swyftx(ITradeAPI):
    def __init__(self, api_key: str, environment: str = "demo"):
        # this is munted. there's no Markets endpoint in demo?!
        self.api = pyswyft.API(access_token=api_key, environment="live")
        # set up asset lists
        self.asset_list_by_id = self._structure_asset_dict_by_id(
            self.api.request(markets.MarketsAssets())
        )
        self.asset_list_by_symbol = self._structure_asset_dict_by_symbol(
            self.api.request(markets.MarketsAssets())
        )

        self.api = pyswyft.API(access_token=api_key, environment=environment)

        # set up data structures
        self.default_currency = "AUD"

    def _structure_asset_dict_by_id(self, asset_dict):
        return_dict = {}
        for asset in asset_dict:
            return_dict[asset["id"]] = asset
        return return_dict

    def _structure_asset_dict_by_symbol(self, asset_dict):
        return_dict = {}
        for asset in asset_dict:
            return_dict[asset["code"]] = asset
        return return_dict

    def order_id_to_text(self, id):
        return ORDER_MAP[id]

    def order_text_to_id(self, text):
        return ORDER_MAP_INVERTED[text]

    def symbol_id_to_text(self, id):
        return self.asset_list_by_id[id]["code"]

    def symbol_text_to_id(self, symbol):
        return self.asset_list_by_symbol[symbol]["id"]

    # todo: there is more stuff in an account objects. i don't think i'll ever use it but maybe one day ill add it in
    def get_account(self) -> Account:
        """Retrieves data about the trading account

        Returns:
            Account: User's trading account information
        """
        return Account(positions=self.list_positions())

    def get_position(self, symbol: str) -> Position:
        """Returns position of a requested symbol

        Args:
            symbol (str): The symbol to search for

        Returns:
            Position: Position object representing the requested symbol
        """
        for position in self.list_positions():
            if position.symbol == symbol:
                return position
        return Position(symbol=symbol, quantity=0)

    def list_positions(self) -> list:
        """Lists all positions

        Returns:
            list: List of Position objects representing all positions
        """
        raw_positions = self.api.request(accounts.AccountBalance())
        return_positions = []

        for position in raw_positions:
            symbol = self.symbol_id_to_text(id=position["assetId"])
            return_positions.append(
                Position(symbol=symbol, quantity=position["availableBalance"])
            )

        return return_positions

    def get_bars(self):
        raise NotImplementedException
        # the owner of the pyswyftx library has not implemented Charts?????
        # raw_bars = self.api.request(charts.)
        return "swiftyx get bars"

    def order_create_by_value(
        self, symbol: str, notional: float, type: int, trigger: bool = None
    ) -> OrderResult:
        """Submits an order (either buy or sell) based on value

        Args:
            symbol (str): the symbol to be bought/sold
            notional (float): the total value to be bought/sold
            type (int): see the ORDER_MAP constant for mapping of ints to strings
            trigger (bool, optional): Trigger amount for the order. Defaults to None.

        Returns:
            OrderResult: output from the API endpoint
        """
        order_type = type
        quantity = notional
        asset_quantity = self.default_currency

        if "BUY" in ORDER_MAP_INVERTED[type]:
            primary = self.default_currency
            secondary = symbol
        else:
            # sells
            primary = symbol
            secondary = self.default_currency

        orders_create_object = orders.OrdersCreate(
            primary=primary,
            secondary=secondary,
            quantity=quantity,
            assetQuantity=asset_quantity,
            orderType=order_type,
            trigger=trigger,
        )
        request = self.api.request(orders_create_object)

        return OrderResult(response=request, orders_create_object=orders_create_object)

    def order_create_by_units(
        self, symbol: str, units: int, type: int, trigger: bool = None
    ) -> OrderResult:
        """Submits an order (either buy or sell) based on units

        Args:
            symbol (str): the symbol to be bought/sold
            units (int): number of units to be bought/sold
            type (int): see the ORDER_MAP constant for mapping of ints to strings
            trigger (bool, optional): Trigger amount for the order. Defaults to None.

        Returns:
            OrderResult: output from the API endpoint
        """
        orderType = type
        quantity = units
        assetQuantity = symbol

        if "BUY" in ORDER_MAP_INVERTED[type]:
            primary = self.default_currency
            secondary = symbol
        else:
            # sells
            primary = symbol
            secondary = self.default_currency

        orders_create_object = orders.OrdersCreate(
            primary=primary,
            secondary=secondary,
            quantity=quantity,
            assetQuantity=assetQuantity,
            orderType=orderType,
            trigger=trigger,
        )
        request = self.api.request(orders_create_object)

        return OrderResult(response=request, orders_create_object=orders_create_object)

    def order_delete(self, order_id: int) -> dict:
        request = self.api.request(orders.OrdersCancel(orderID=order_id))
        return request

    def order_list(self) -> dict:
        request = self.api.request(orders.OrdersListAll())
        return request["orders"]

    def close_position(self, symbol: str) -> OrderResult:
        """Function to sell all units of a given symbol

        Args:
            symbol (str): the symbol to sell

        Returns:
            OrderResult: output from the API endpoint
        """
        position = self.get_position(symbol)
        request = self.order_create_by_units(
            symbol=symbol, units=position.quantity, type=MARKET_SELL
        )
        return request


if __name__ == "__main__":
    import boto3

    ssm = boto3.client("ssm")
    api_key = (
        ssm.get_parameter(Name="/tabot/swyftx/access_token", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    api = Swyftx(api_key=api_key)
    account = api.get_account()
    api.list_positions()
    api.order_create_by_value("XRP", 500, MARKET_BUY)
    api.order_create_by_units("XRP", 200, MARKET_BUY)
    api.order_list()
    api.close_position("XRP")
    print("a")
