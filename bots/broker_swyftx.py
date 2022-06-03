from datetime import datetime
from math import floor
import logging
from socket import create_server
import pyswyft
from pyswyft.endpoints import accounts, history, markets, orders
import pytz
import time
import yfinance as yf


from itradeapi import (
    ITradeAPI,
    Asset,
    IOrderResult,
    Account,
    Position,
    NotImplementedException,
    BrokerAPIError
)

log_wp = logging.getLogger("swyftx")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("swyftx.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)


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
    "cancelled": {2, 6, 7, 8, 9, 10},
    "open": {1, 3},
    "pending": {5},
    "filled": {4},
}
ORDER_STATUS_ID_TO_SUMMARY = {
    1: "open",
    2: "cancelled",
    3: "open",
    4: "filled",
    5: "pending",
    6: "cancelled",
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
    10: "Refunded/rolled back",
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

YF_SYMBOL_MAP = {
    "SHIB-USD": "SHIB",
    "ETH-USD":"ETH",
    "DOGE-USD":"DOGE",
    "MATIC-USD":"MATIC",
    "WBTC-USD":"WBTC",
    "TRX-USD":"TRX",
    "BAT-USD":"BAT",
    "PAXG-USD":"PAXG",
    "AAVE-USD":"AAVE",
    "AVAX-USD":"AVAX",
    "BCH-USD":"BCH",
    "LINK-USD":"LINK",
    "DAI-USD":"DAI",
    "LTC-USD":"LTC",
    "MKR-USD":"MKR",
    "SUSHI-USD":"SUSHI",
    "YFI-USD":"YFI",
    "XRP-USD":"XRP",
    "SOL-USD":"SOL",
    "ADA-USD":"ADA"

}
SWYFTX_SYMBOL_MAP = {y: x for x, y in YF_SYMBOL_MAP.items()}

class OrderResult(IOrderResult):
    # TODO delete this cruft
    order_id: str
    sold_symbol: str
    bought_symbol: str
    quantity: float
    quantity_symbol: str
    quantity_id: int
    unit_price: int
    status: int
    status_text: str
    status_summary: str
    order_type: int
    order_type_text: str
    create_time: int
    update_time: int
    total_value: float
    success: bool
    _raw_response: dict
    _raw_request = None

    def __init__(self, order_object, asset_list_by_id: dict):
        self._raw_response = order_object

        self.status = order_object["status"]
        self.status_text = ORDER_STATUS_TEXT[self.status]
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]
        self.success = (
            order_object["status"] in ORDER_STATUS_SUMMARY_TO_ID["open"]
            or order_object["status"] in ORDER_STATUS_SUMMARY_TO_ID["filled"]
        )


        self.order_type = order_object["order_type"]
        self.order_type_text = ORDER_MAP_INVERTED[self.order_type]

        self.order_id = order_object["orderUuid"]

        bought_id = order_object["secondary_asset"]
        self.symbol = asset_list_by_id[bought_id]["symbol"]


        if "limit" in ORDER_MAP_INVERTED[order_object["order_type"]]:
        #if order_object.type == "limit":
            # TODO this is wrong
            #self.ordered_unit_quantity = float(response.qty)
            #self.ordered_unit_price = float(response.limit_price)
            #self.ordered_total_value = (
            #    self.ordered_unit_quantity * self.ordered_unit_price
            #)
            self.ordered_unit_quantity = order_object["amount"]
            self.ordered_unit_price = order_object["trigger"]
            self.ordered_total_value = (
                self.ordered_unit_quantity * self.ordered_unit_price
            )
            
        else:
            # market orders - so there is only quantity is known, not price or total value
            self.ordered_unit_quantity = order_object["amount"]
            self.ordered_unit_price = None
            self.ordered_total_value = None

        if self.status_summary == "filled":
            self.filled_unit_quantity = order_object["amount"]
            self.filled_unit_price = order_object["rate"]
            self.filled_total_value = order_object["amount"] * order_object["rate"]
        else:
            self.filled_unit_quantity = 0
            self.filled_unit_price = None
            self.filled_total_value = None


        self.fees = order_object["feeAudValue"]


        if order_object["status"] == 3 or order_object["status"] == 4:
            self.fees = order_object["feeAmount"]
        
        timezone = pytz.timezone('UTC')
        create_s, create_ms = divmod(order_object["created_time"], 1000)
        self.create_time = timezone.localize(datetime.fromisoformat('%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(create_s)), create_ms)))
        
        mod_s, mod_ms = divmod(order_object["updated_time"], 1000)
        self.update_time = timezone.localize(datetime.fromisoformat('%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(mod_s)), mod_ms)))


# return objects

# concrete class
class SwyftxAPI(ITradeAPI):
    def __init__(
        self, access_token: str, back_testing: bool = False, back_testing_balance:float=None, real_money_trading:bool=False
    ):
        self.access_token = access_token
        self.assets_initialised = False
        self.back_testing = back_testing

        if real_money_trading != True:
            # now use the environment that was actually requested. i hate this.
            self.api = pyswyft.API(access_token=access_token, environment="demo")
        else:
            self.api = pyswyft.API(access_token=access_token, environment="live")

        self._build_asset_list()

        # set up data structures
        self.default_currency = "USD"

    def get_precision(self, yf_symbol:str)->int:
        return 5

    def get_broker_name(self)->str:
        return "swyftx"

    def get_assets(self)->dict:
        if not self.assets_initialised:
            self._build_asset_list()
        return self._asset_list_by_symbol

    def get_asset(self, symbol:str)->Asset:
        sw_symbol = YF_SYMBOL_MAP[symbol]
        if not self.assets_initialised:
            self._build_asset_list()
        return self._asset_list_by_symbol[sw_symbol]

    def get_asset_by_id(self, id)->Asset:
        if not self.assets_initialised:
            self._build_asset_list()
        return self._asset_list_by_id[id]


    def _build_asset_list(self)->bool:
        # this is munted. there's no Markets endpoint in demo?!
        temp_api = pyswyft.API(access_token=self.access_token, environment="live")
        swyftx_assets = temp_api.request(markets.MarketsAssets())

        # set up asset lists
        self._asset_list_by_id = self._structure_asset_dict_by_id(swyftx_assets)
        
        self._asset_list_by_symbol = self._structure_asset_dict_by_symbol(swyftx_assets)

        self.assets_initialised = True

        return True

    def _structure_asset_dict_by_id(self, asset_dict)->dict:
        return_dict = {}
        for asset in asset_dict:
            asset["symbol"] = asset["code"]
            return_dict[asset["id"]] = asset
        return return_dict

    def _structure_asset_dict_by_symbol(self, asset_dict)->dict:
        return_dict = {}
        for asset in asset_dict:
            asset["symbol"] = str(asset["code"])
            return_dict[asset["code"]] = asset
        return return_dict

    def order_id_to_text(self, id)->str:
        return ORDER_MAP[id]

    def order_text_to_id(self, text)->int:
        return ORDER_MAP_INVERTED[text]

    def symbol_id_to_text(self, id)->str:
        if not self.assets_initialised:
            self._build_asset_list()

        asset = self.get_asset_by_id(id=id)
        #return [b for b in assets if assets[b]["id"] == id][0]
        return asset["code"]

    def symbol_text_to_id(self, symbol)->int:
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
            # dumb api lets you have incredibly small units
            if float(position["availableBalance"]) > 100:
                symbol = self.symbol_id_to_text(id=position["assetId"])
                return_positions.append(
                    Position(symbol=symbol, quantity=position["availableBalance"])
                )

        return return_positions

    def get_last_close(self, symbol: str):
        raise NotImplementedError
        if symbol == self.default_currency:
            return 1
        else:
            close = self.api.request(
                orders.OrdersExchangeRate(buy=symbol, sell=self.default_currency)
            )
            return float(close["price"])

    def get_bars(self, symbol: str, start: str, end: str = None, interval: str = "1d"):
        raise NotImplementedError
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
        #self, symbol: str, order_value: float = None, units: float = None
        self, symbol:str, units:int, back_testing_date=None
    )->OrderResult:
        sw_symbol = YF_SYMBOL_MAP[symbol]
        return self._submit_order(
            sw_symbol=sw_symbol, units=units, order_type=MARKET_BUY
        )

        # TODO integrate this back in to the alpaca api. i kind of like it
        #if order_value == None and units == None:
        #    raise OrderRequiresPriceOrUnitsException(f"Need to specify either order_value or units")

        #if order_value != None:
        #    # buying by total order value
        #    # first get a quote for the symbol
        #    exchange_rate = self.api.request(
        #        orders.OrdersExchangeRate(buy=sw_symbol, sell=self.default_currency)
        #    )
        #    units = floor(order_value / float(exchange_rate["price"]))

        # no need for an else, units was already specified in the call


    def buy_order_limit(self, symbol: str, units: float, unit_price: float, back_testing_date=None)->OrderResult:
        # buying by total order value
        sw_symbol = YF_SYMBOL_MAP[symbol]
        return self._submit_order(
            sw_symbol=sw_symbol,
            units=units,
            order_type=LIMIT_BUY,
            limit_unit_price=unit_price
        )

    def sell_order_market(
        #self, symbol: str, order_value: float = None, units: float = None
        self, symbol: str, units: float = None, back_testing_date=None
    )->OrderResult:
        sw_symbol = YF_SYMBOL_MAP[symbol]
        return self._submit_order(
            sw_symbol=sw_symbol, units=units, order_type=MARKET_SELL
        )

    def sell_order_limit(self, symbol: str, units: float, unit_price: float, back_testing_date=None)->OrderResult:
        sw_symbol = YF_SYMBOL_MAP[symbol]
        return self._submit_order(
            sw_symbol=sw_symbol, units=units, order_type=LIMIT_SELL, limit_unit_price=unit_price
        )

    def _submit_order(
        self, sw_symbol: str, units: int, order_type: int, limit_unit_price: float = None, sell_stop_price: float = None,
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
        if order_type > 4:
            raise NotImplementedException(
                f"STOPLIMITBUY and STOPLIMITSELL is not implemented yet"
            )
        
        if order_type == LIMIT_BUY:
            # sell 50 XRP units at $4 per unit. primary is USD, secondary is XRP
            # limit is 0.25, or 1 USD divided by unit price
            #trigger = 1 / limit_unit_price
            asset_quantity = sw_symbol.upper()
        elif order_type == LIMIT_SELL:
            # buy is the opposite
            # primary is USD, secondary is XRP
            # secondary per primary
            # so if unit price is $4 for 1 unit
            #trigger = limit_unit_price
            asset_quantity = self.default_currency.upper()
            asset_quantity = sw_symbol.upper()
            limit_unit_price = 1 / limit_unit_price
        elif order_type == MARKET_BUY:
            asset_quantity = sw_symbol.upper()
        elif order_type == MARKET_SELL:
            asset_quantity = sw_symbol.upper()

        # swyftx api expects symbols in upper case....
        primary = self.default_currency.upper()
        secondary = sw_symbol.upper()

        orders_create_object = orders.OrdersCreate(
            primary=primary,
            secondary=secondary,
            quantity=units,
            assetQuantity=asset_quantity,
            orderType=order_type,
            trigger=limit_unit_price,
        )

        response = self.api.request(orders_create_object)
        # this annoys me, but LIMIT_BUY and LIMIT_SELL don't return any detail about the order on lodgement
        # whereas MARKET does
        # sleep(1)
        return self.get_order(order_id=response["orderUuid"], back_testing_date=None)

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
                    "create_time": None,  # never got created
                    "update_time": None,  # never got modified
                }
            else:
                # but maybe it could happen in other cases too?
                raise Exception("API did not return any data!")

        return OrderResult(response=response, orders_create_object=orders_create_object)

    def get_order(self, order_id: str, back_testing_date=None)->OrderResult:
        response = self.api.request(orders.OrdersGetOrder(orderID=order_id))
        # orders_create_object: orders.OrdersCreate):
        return OrderResult(
            order_object=response, asset_list_by_id=self._asset_list_by_id
        )

    def cancel_order(self, order_id: str, back_testing_date=None) ->OrderResult:
        cancel_request = self.api.request(orders.OrdersCancel(orderID=order_id))
        #if request["status"]
        order_result =  self.get_order(order_id=order_id, back_testing_date=back_testing_date)
        if order_result.status_summary == "cancelled":
            return order_result
        else:
            raise BrokerAPIError(f"Cancel order {order_id} has failed. Current status of the order is {cancel_request['status']} instead of cancelled")
        #return request

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

    def close_position(self, symbol: str, back_testing_date=None) -> OrderResult:
        """Function to sell all units of a given symbol

        Args:
            symbol (str): the symbol to sell

        Returns:
            OrderResult: output from the API endpoint
        """
        sw_symbol = YF_SYMBOL_MAP[symbol]
        position = self.get_position(sw_symbol)
        request = self.sell_order_market(symbol=symbol, units=position.quantity)
        return request


if __name__ == "__main__":
    import boto3

    ssm = boto3.client("ssm")
    access_token = (
        ssm.get_parameter(Name="/tabot/paper/swyftx/access_token", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    api = SwyftxAPI(access_token=access_token)
    #api.get_bars("SOL-USD", start="2022-04-01T00:00:00+10:00")

    api.get_account()
    
    buy_limit = api.buy_order_limit(symbol="XRP-USD", units=52, unit_price=0.1)
    buy_market_units = api.buy_order_market(symbol="XRP-USD", units=75)
    sell_market_value = api.sell_order_market(symbol="XRP-USD", units=10)
    sell_limit = api.sell_order_limit(symbol="XRP-USD", units=52, unit_price=2)
    api.list_positions()
    api.get_position(symbol="XRP-USD")
    api.list_orders()
    api.list_orders(filled=True)
    api.list_orders(cancelled=True)
    api.list_orders(still_open=True)
    api.close_position("XRP-USD")

    print("a")
