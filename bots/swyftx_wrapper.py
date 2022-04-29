import pyswyft
from pyswyft.endpoints import accounts, history, markets, orders
from itradeapi import (
    ITradeAPI,
    IAsset,
    IOrderResult,
    IAccount,
    IPosition,
    NotImplementedException,
)
from math import floor
import yfinance as yf
from datetime import datetime, timedelta

# from order_result import OrderResult


class OrderRequiresPriceOrUnitsException(Exception):
    ...


# these are repeated in order_result.py because i am just taking these statuses 1:1
# the alpaca wrapper will have to normalise to these statuses
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


class OrderResult(IOrderResult):
    order_id: str
    sold_symbol: str
    bought_symbol: str
    quantity: float
    quantity_symbol: str
    quantity_id: int
    unit_price: int
    trigger: float
    status: int
    status_text: str
    status_summary: str
    order_type: int
    order_type_text: str
    created_time: int
    updated_time: int
    success: bool
    _raw_response: dict
    _raw_request = None

    def __init__(self, order_object, asset_list_by_id: dict):
        self._raw_response = order_object

        order_type = order_object["order_type"]
        order_type_text = ORDER_MAP_INVERTED[order_type]

        self.order_id = order_object["orderUuid"]
        if "BUY" in order_type_text:
            self.bought_id = order_object["secondary_asset"]
            self.bought_symbol = asset_list_by_id[self.bought_id]["symbol"]
        else:
            # sells
            self.sold_id = order_object["primary_asset"]
            self.sold_symbol = asset_list_by_id[self.sold_id]["symbol"]

        self.quantity = order_object["quantity"]
        self.quantity_id = order_object["quantity_asset"]
        self.quantity_symbol = asset_list_by_id[self.quantity_id]["symbol"]

        if order_type == MARKET_BUY or order_type == MARKET_SELL:
            # immediate sell, so we can fill in all
            self.units = order_object["amount"]
            self.unit_price = order_object["rate"]
            self.fees = order_object["feeAmount"]
            self.total_value = self.units * self.unit_price

            # self.requested_units = order_object["amount"]
            # self.requested_unit_price = order_object["rate"]
            # self.requested_total_value = self.units * self.unit_price

        elif order_type == LIMIT_BUY or order_type == LIMIT_SELL:
            self.requested_units = order_object["quantity"]
            self.requested_unit_price = order_object["trigger"]
            self.fees = 0
            self.requested_total_value = (
                self.requested_units * self.requested_unit_price
            )

        if order_object["status"] == 3 or order_object["status"] == 4:
            self.units = order_object["amount"]
            self.unit_price = order_object["rate"]
            self.fees = order_object["feeAmount"]
            self.total_value = self.units * self.unit_price

        self.trigger = order_object["trigger"]
        self.status = order_object["status"]
        self.status_text = ORDER_STATUS_TEXT[self.status]
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]
        self.success = (
            order_object["status"] in ORDER_STATUS_SUMMARY_TO_ID["open"]
            or order_object["status"] in ORDER_STATUS_SUMMARY_TO_ID["filled"]
        )

        created_time = order_object["created_time"]
        updated_time = order_object["updated_time"]


# return objects
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

    def __str__(self):
        print("banana")


class Position(IPosition):
    symbol: str
    quantity: float

    def __init__(self, symbol, quantity):
        self.symbol = symbol
        self.quantity = quantity


# concrete class
class SwyftxAPI(ITradeAPI):
    def __init__(self, api_key: str, real_money_trading: bool = False):
        self.api_key = api_key
        self.assets_initialised = False

        if real_money_trading != True:
            # now use the environment that was actually requested. i hate this.
            self.api = pyswyft.API(access_token=api_key, environment="demo")
        else:
            self.api = pyswyft.API(access_token=api_key, environment="live")

        # set up data structures
        self.default_currency = "aud"

    def get_assets(self):
        if self.assets_initialised != True:
            self._build_asset_list()
        return self._asset_list_by_symbol

    def get_asset_by_id(self, id):
        if self.assets_initialised != True:
            self._build_asset_list()
        return self._asset_list_by_symbol

    def get_asset_by_name(self, name: str):
        if self.assets_initialised != True:
            self._build_asset_list()
        return self._asset_list_by_symbol

    def _build_asset_list(self):
        # this is munted. there's no Markets endpoint in demo?!
        temp_api = pyswyft.API(access_token=self.api_key, environment="live")
        # set up asset lists
        self._asset_list_by_id = self._structure_asset_dict_by_id(
            temp_api.request(markets.MarketsAssets())
        )
        self._asset_list_by_symbol = self._structure_asset_dict_by_symbol(
            temp_api.request(markets.MarketsAssets())
        )

        return True

    def _structure_asset_dict_by_id(self, asset_dict):
        return_dict = {}
        for asset in asset_dict:
            asset["symbol"] = asset["code"]
            return_dict[asset["id"]] = asset
        return return_dict

    def _structure_asset_dict_by_symbol(self, asset_dict):
        return_dict = {}
        for asset in asset_dict:
            asset["symbol"] = str(asset["code"]).lower()
            return_dict[asset["code"]] = asset
        return return_dict

    def order_id_to_text(self, id):
        return ORDER_MAP[id]

    def order_text_to_id(self, text):
        return ORDER_MAP_INVERTED[text]

    def symbol_id_to_text(self, id):
        assets = self.get_assets()
        return [b.lower() for b in assets if assets[b]["id"] == id][0]
        return assets[id]["code"]

    def symbol_text_to_id(self, symbol):
        assets = self.get_assets()
        return assets[symbol]["id"]

    def get_account(self) -> Account:
        """Retrieves data about the trading account

        Returns:
            Account: User's trading account information
        """
        # AccountBalance
        assets = {}
        request = self.api.request(accounts.AccountBalance())

        for asset in request:
            symbol = self.symbol_id_to_text(asset["assetId"])
            symbol = symbol.lower()

            ##########
            ## I did this when I thought I could get swyftx to buy stuff in USD, but I can't work out how to do that
            #            # intercept aud and convert it to usd
            #            if symbol == "aud":
            #                # convert it to usd
            #                rate = self.api.request(
            #                    orders.OrdersExchangeRate(
            #                        buy="USD",
            #                        sell="AUD",
            #                        amount=asset["availableBalance"],
            #                        limit="AUD",
            #                    )
            #                )
            #                symbol = "usd"
            #                asset["availableBalance"] = rate["amount"]
            #########

            assets[symbol] = float(asset["availableBalance"])

        return Account(assets=assets)

    def get_position(self, symbol: str) -> Position:
        """Returns position of a requested symbol

        Args:
            symbol (str): The symbol to search for

        Returns:
            Position: Position object representing the requested symbol
        """
        for position in self.list_positions():
            if position.symbol.lower() == symbol.lower():
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
            # dumb api lets you have incredibly small units
            if float(position["availableBalance"]) > 100:
                symbol = self.symbol_id_to_text(id=position["assetId"])
                return_positions.append(
                    Position(
                        symbol=symbol.lower(), quantity=position["availableBalance"]
                    )
                )

        return return_positions

    def get_last_close(self, symbol: str):
        if symbol.lower() == self.default_currency:
            return 1
        else:
            close = self.api.request(
                orders.OrdersExchangeRate(buy=symbol, sell=self.default_currency)
            )
            return float(close["price"])

    def get_bars(self, symbol: str, start: str, end: str = None, interval: str = "1d"):
        intervals = [
            "1m",
            "2m",
            "5m",
            "15m",
            "30m",
            "60m",
            "90m",
            "1h",
            "1d",
            "5d",
            "1wk",
            "1mo",
            "3mo",
        ]
        if interval not in intervals:
            raise ValueError(f"Interval must be one of {str(intervals)}")

        if end == None:
            end = datetime.now()

        if type(start) == str:
            start = datetime.fromisoformat(start)

        symbol = symbol + "-USD"

        return yf.Ticker(symbol).history(
            start=start, end=end, interval=interval, actions=False
        )

        # the owner of the pyswyftx library has not implemented Charts????? or swyftx don't offer it??
        # raw_bars = self.api.request(charts.)

    def buy_order_market(
        self, symbol: str, order_value: float = None, units: float = None
    ):
        if order_value == None and units == None:
            raise OrderRequiresPriceOrUnitsException

        if order_value != None:
            # buying by total order value
            # first get a quote for the symbol
            exchange_rate = self.api.request(
                orders.OrdersExchangeRate(buy=symbol, sell=self.default_currency)
            )
            units = floor(order_value / float(exchange_rate["price"]))

        # no need for an else, units was already specified in the call

        return self._submit_order(
            symbol=symbol, units=units, type=MARKET_BUY, trigger=None
        )

    def buy_order_limit(self, symbol: str, units: float, unit_price: float):
        # buying by total order value
        return self._submit_order(
            symbol=symbol,
            units=units,
            type=LIMIT_BUY,
            trigger=unit_price,
        )

    def sell_order_market(
        self, symbol: str, order_value: float = None, units: float = None
    ):
        if order_value == None and units == None:
            raise OrderRequiresPriceOrUnitsException

        if order_value != None:
            # selling by total order value
            # first get a quote for the symbol
            exchange_rate = self.api.request(
                orders.OrdersExchangeRate(
                    buy=self.default_currency.upper(), sell=symbol.upper()
                )
            )
            units = floor(order_value / float(exchange_rate["price"]))

        # no need for an else, units was already specified in the call

        return self._submit_order(
            symbol=symbol, units=units, type=MARKET_SELL, trigger=None
        )

    def sell_order_limit(self, symbol: str, units: float, unit_price: float):
        trigger = 1 / unit_price
        return self._submit_order(
            symbol=symbol, units=units, type=LIMIT_SELL, trigger=trigger
        )

    def _submit_order(
        self, symbol: str, units: int, type: int, trigger: bool = None
    ) -> OrderResult:
        """Submits an order (either buy or sell) based on value.  Note that this should not be called directly

        Args:
            symbol (str): the symbol to be bought/sold
            units (int): the total number of units to be bought/sold
            type (int): see the ORDER_MAP constant for mapping of ints to strings
            trigger (bool, optional): Trigger amount for the order. Defaults to None.  Trigger is the price per one

        Returns:
            OrderResult: output from the API endpoint
        """
        if type > 4:
            raise NotImplementedException(
                f"STOPLIMITBUY and STOPLIMITSELL is not implemented yet"
            )
        order_type = type
        quantity = units

        # swyftx api expects symbols in upper case....
        if "BUY" in ORDER_MAP_INVERTED[type]:
            primary = self.default_currency.upper()
            secondary = symbol.upper()
            asset_quantity = symbol.upper()
        else:
            # sells
            primary = self.default_currency.upper()
            secondary = symbol.upper()
            asset_quantity = symbol.upper()

        orders_create_object = orders.OrdersCreate(
            primary=primary,
            secondary=secondary,
            quantity=quantity,
            assetQuantity=asset_quantity,
            orderType=order_type,
            trigger=trigger,
        )

        response = self.api.request(orders_create_object)
        # this annoys me, but LIMIT_BUY and LIMIT_SELL don't return any detail about the order on lodgement
        # whereas MARKET does
        # sleep(1)
        return self.get_order(order_id=response["orderUuid"])

        if not response.get("order"):
            # i dunno why, by LIMIT_BUY and LIMIT_SELL don't return any detail about the order when you lodge it
            # whereas
            if order_type == LIMIT_BUY:
                # i've only see this when submitting a buy order with insufficient cash
                # so we're going to be dodgey and fudge a response
                response["order"] = {
                    "order_type": order_type,
                    "secondary_asset": secondary,
                    "primary_asset": primary,
                    "quantity": quantity,
                    "quantity_asset": asset_quantity,
                    "trigger": trigger,
                    "status": 2,  # order cancelled
                    "created_time": None,  # never got created
                    "updated_time": None,  # never got modified
                }
            else:
                # but maybe it could happen in other cases too?
                raise Exception("API did not return any data!")

        return OrderResult(response=response, orders_create_object=orders_create_object)

    def get_order(self, order_id: str):
        response = self.api.request(orders.OrdersGetOrder(orderID=order_id))
        # orders_create_object: orders.OrdersCreate):
        return OrderResult(
            order_object=response, asset_list_by_id=self._asset_list_by_id
        )

    def delete_order(self, order_id: str) -> dict:
        request = self.api.request(orders.OrdersCancel(orderID=order_id))
        return request

    def list_orders(
        self,
        filled: bool = False,
        cancelled: bool = False,
        still_open: bool = False,
    ) -> list:
        order_list = []
        # handle pagination
        page = 0
        page_size = 50
        while True:
            request = self.api.request(orders.OrdersListAll(limit=page_size, page=page))

            for order in request["orders"]:
                result = OrderResult(
                    order_object=order,
                    asset_list_by_id=self._asset_list_by_id,
                )
                # if no filters are applied
                if not filled and not cancelled and not still_open:
                    order_list.append(result)
                else:
                    # at least one filter has been applied
                    if result.status in ORDER_STATUS_SUMMARY_TO_ID["filled"] and filled:
                        order_list.append(result)
                    elif (
                        result.status in ORDER_STATUS_SUMMARY_TO_ID["cancelled"]
                        and cancelled
                    ):
                        order_list.append(result)
                    elif (
                        result.status in ORDER_STATUS_SUMMARY_TO_ID["open"]
                        and still_open
                    ):
                        order_list.append(result)
            page += 1

            # we've finished processing the last page
            if len(request["orders"]) < page_size:
                break

        return order_list

    def close_position(self, symbol: str) -> OrderResult:
        """Function to sell all units of a given symbol

        Args:
            symbol (str): the symbol to sell

        Returns:
            OrderResult: output from the API endpoint
        """
        position = self.get_position(symbol)
        request = self.sell_order_market(symbol=symbol, units=position.quantity)
        return request


if __name__ == "__main__":
    import boto3

    ssm = boto3.client("ssm")
    api_key = (
        ssm.get_parameter(Name="/tabot/swyftx/access_token", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    api = SwyftxAPI(api_key=api_key)
    api.get_bars("SOL-USD", start="2022-04-01T00:00:00+10:00")

    api.get_account()
    buy_market_value = api.buy_order_market(symbol="XRP", order_value=100)
    buy_market_units = api.buy_order_market(symbol="XRP", units=75)
    buy_limit = api.buy_order_limit(symbol="XRP", units=52, unit_price=195)
    sell_market_value = api.sell_order_market(symbol="XRP", order_value=100)
    sell_market_units = api.sell_order_market(symbol="XRP", units=10)
    sell_limit = api.sell_order_limit(symbol="XRP", units=52, unit_price=0.95)
    api.list_positions()
    api.get_position(symbol="XRP")
    api.list_orders()
    api.list_orders(filled=True)
    api.list_orders(cancelled=True)
    api.list_orders(still_open=True)
    api.close_position("XRP")

    print("a")
