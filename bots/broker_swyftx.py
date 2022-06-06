from datetime import datetime
#from math import floor
import json
import logging
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
    BrokerAPIError,
    UnknownSymbol,
    DelistedAsset,
    UntradeableAsset,
    ZeroUnitsOrdered,
    ApiRateLimit
)

import utils

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

class OrderResult(IOrderResult):
    def __init__(self, order_object, asset_list_by_id: dict):
        self._raw_response = order_object
        self.status = order_object["status"]
        self.status_text = ORDER_STATUS_TEXT[self.status]
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]
        self.success = (
            order_object["status"] in ORDER_STATUS_SUMMARY_TO_ID["open"]
            or order_object["status"] in ORDER_STATUS_SUMMARY_TO_ID["filled"]
            or order_object["status"] in ORDER_STATUS_SUMMARY_TO_ID["pending"]
        )

        self.order_type = order_object["order_type"]
        self.order_type_text = ORDER_MAP_INVERTED[self.order_type]

        self.order_id = order_object["orderUuid"]

        bought_id = order_object["secondary_asset"]
        self.symbol = asset_list_by_id[bought_id].symbol

        if "LIMIT" in ORDER_MAP_INVERTED[order_object["order_type"]]:
            self.ordered_unit_quantity = order_object["quantity"]
            # if selling, 1 / trigger = unit price. This API is goddamn stupid
            if "SELL" in ORDER_MAP_INVERTED[order_object["order_type"]]:
                self.ordered_unit_price = 1/ order_object["trigger"]
            else:
                self.ordered_unit_price = order_object["trigger"]
            self.ordered_total_value = (
                self.ordered_unit_quantity * self.ordered_unit_price
            )
            
        else:
            # market orders - so there is only quantity is known, not price or total value
            self.ordered_unit_quantity = order_object["quantity"]
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

        open_statuses = ["open", "pending"]
        if self.status_summary in open_statuses:
            self.closed = False
        else:
            self.closed = True

        self.validate()

# concrete class
class SwyftxAPI(ITradeAPI):
    def __init__(
        self, access_token: str, back_testing: bool = False, back_testing_balance:float=None, real_money_trading:bool=False
    ):
        self.access_token = access_token
        self.back_testing = back_testing

        if real_money_trading != True:
            # now use the environment that was actually requested. i hate this.
            self.api = pyswyft.API(access_token=access_token, environment="demo")
        else:
            self.api = pyswyft.API(access_token=access_token, environment="live")

        # set up data structures
        self._build_asset_list()

        self.default_currency = "USD"

        self.rejected_orders = {}

    def get_precision(self, yf_symbol:str)->int:
        return 5

    def get_broker_name(self)->str:
        return "swyftx"

    def get_assets(self)->dict:
        return self._asset_list_by_yf_symbol

    def get_asset(self, symbol:str)->Asset:
        return self._asset_list_by_yf_symbol[symbol]

    def get_asset_by_id(self, id)->Asset:
        return self._asset_list_by_id[id]

    def validate_symbol(self, symbol:str)->bool:
        # if its valid, just return True
        if symbol in self._asset_list_by_yf_symbol:
            return True
        
        #  if its also not in dict of invalid assets, so its just totally unknown
        if symbol not in self._invalid_assets:
            raise UnknownSymbol(f"{symbol} is not known to {self.get_broker_name()}")
        
        return False
        # so its invalid but the broker does know about it - delisted/not tradeable
        if self._invalid_assets[symbol]["delisting"] == 1:
            raise DelistedAsset(f"{symbol} has been delisted on {self.get_broker_name()}")
        
        if self._invalid_assets[symbol]["tradable"] == 0:
            raise UntradeableAsset(f"{symbol} is not currently tradeable on {self.get_broker_name()}")
        
        # logically we shouldn't get here
        raise RuntimeError("We shouldn't have gotten here.")

    def _sw_to_yf(self, sw_symbol:str)->str:
        skip_symbols = ["AUD", "USD"]
        if sw_symbol in skip_symbols:
            return sw_symbol
        return sw_symbol + "-USD"
    
    def _yf_to_sw(self, yf_symbol:str)->str:
        location = yf_symbol.rfind("USD") - 1
        return yf_symbol[:location]

    def _build_asset_list(self)->bool:
        # this is munted. there's no Markets endpoint in demo?!
        temp_api = pyswyft.API(access_token=self.access_token, environment="live")
        raw_assets = temp_api.request(markets.MarketsAssets())
        valid_assets = []
        self._invalid_assets = {}

        # convert them to Asset objects
        for this_asset in raw_assets:
            yf_symbol = self._sw_to_yf(this_asset["code"])
            minimum_order = float(this_asset["minimum_order_increment"])
            asset_obj = Asset(symbol=yf_symbol, min_order_size=1, min_trade_increment=minimum_order, min_price_increment=0.00001)
            asset_obj.id = this_asset["id"]
            if self._is_invalid_asset(this_asset):
                self._invalid_assets[yf_symbol] = asset_obj
            else:
                valid_assets.append(asset_obj)

        # set up asset lists
        self._asset_list_by_yf_symbol = self._structure_asset_dict_by_yf_symbol(valid_assets)
        self._asset_list_by_id = self._structure_asset_dict_by_id(valid_assets)

        return True

    def _is_invalid_asset(self, asset_dict:dict)->dict:
        if asset_dict["tradable"] == 0 or asset_dict["buyDisabled"] == 1 or asset_dict["delisting"] == 1:
            return True
        return False

    def _structure_asset_dict_by_id(self, asset_dict:dict)->dict:
        return_dict = {}
        for asset in asset_dict:
            #asset["symbol"] = asset.symbol
            return_dict[asset.id] = asset
        return return_dict

    def _structure_asset_dict_by_yf_symbol(self, asset_dict:dict)->dict:
        return_dict = {}
        for asset in asset_dict:
            #asset["symbol"] = asset.symbol
            return_dict[asset.symbol] = asset
        return return_dict

    def order_id_to_text(self, id)->str:
        return ORDER_MAP[id]

    def order_text_to_id(self, text)->int:
        return ORDER_MAP_INVERTED[text]



    def get_account(self) -> Account:
        """Retrieves data about the trading account

        Returns:
            Account: User's trading account information
        """
        # AccountBalance
        assets = {}
        request = self.api.request(accounts.AccountBalance())

        for asset in request:
            symbol = self._asset_list_by_id[asset["assetId"]]

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
            
            # need to account for transaction fees. probably better to do this at buyplan time but whatever trevor
            if symbol == self.default_currency:
                balance = float(asset["availableBalance"])
                if balance < 10:
                    assets[symbol] = 0
                else:    
                    assets[symbol] = float(asset["availableBalance"]) - 10
            else:
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
            #if float(position["availableBalance"]) > 100:
            symbol = self._asset_list_by_id[position["assetId"]].symbol
            f_balance = float(position["availableBalance"])

            # i don't treat usd and aud as positions
            if symbol in ["USD", "AUD"]:
                continue

            # ignore symbols where I don't actually hold a position    
            if f_balance == 0:
                continue

            #yf_symbol = self._sw_to_yf(symbol)
            return_positions.append(
                Position(symbol=symbol, quantity=f_balance)
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
        sw_symbol = self._yf_to_sw(symbol)
        asset_quantity = sw_symbol.upper()

        try:
            return self._submit_order(
                sw_symbol=sw_symbol, units=units, order_type=MARKET_BUY, asset_quantity=asset_quantity
            )
        except ZeroUnitsOrdered as e:
            return self._make_rejected_order_result(sw_symbol=sw_symbol, units=units, order_type=LIMIT_BUY, sw_asset_quantity=asset_quantity)
        except ApiRateLimit as e:
            return self._make_rejected_order_result(sw_symbol=sw_symbol, units=units, order_type=LIMIT_SELL, sw_asset_quantity=asset_quantity)
        except:
            raise


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
        sw_symbol = self._yf_to_sw(symbol)
        asset_quantity = sw_symbol.upper()

        try:
            return self._submit_order(
                sw_symbol=sw_symbol,
                units=units,
                order_type=LIMIT_BUY,
                limit_unit_price=unit_price, asset_quantity=asset_quantity
            )
        except ZeroUnitsOrdered as e:
            return self._make_rejected_order_result(sw_symbol=sw_symbol, units=units, order_type=LIMIT_BUY, sw_asset_quantity=asset_quantity, unit_price=unit_price)
        except ApiRateLimit as e:
            return self._make_rejected_order_result(sw_symbol=sw_symbol, units=units, order_type=LIMIT_SELL, sw_asset_quantity=asset_quantity, unit_price=unit_price)
        except:
            raise

    def sell_order_market(
        #self, symbol: str, order_value: float = None, units: float = None
        self, symbol: str, units: float = None, back_testing_date=None
    )->OrderResult:
        sw_symbol = self._yf_to_sw(symbol)
        asset_quantity = sw_symbol.upper()

        try:
            return self._submit_order(
                sw_symbol=sw_symbol, units=units, order_type=MARKET_SELL, asset_quantity=asset_quantity
            )
        except ZeroUnitsOrdered as e:
            return self._make_rejected_order_result(sw_symbol=sw_symbol, units=units, order_type=MARKET_SELL, sw_asset_quantity=asset_quantity)
        except ApiRateLimit as e:
            return self._make_rejected_order_result(sw_symbol=sw_symbol, units=units, order_type=LIMIT_SELL, sw_asset_quantity=asset_quantity)
        except:
            raise


    def sell_order_limit(self, symbol: str, units: float, unit_price: float, back_testing_date=None)->OrderResult:
        sw_symbol = self._yf_to_sw(symbol)
        precision = self.get_precision(symbol)
        asset_quantity = sw_symbol.upper()
        limit_unit_price = round(1 / unit_price, precision)

        try:
            return self._submit_order(
                sw_symbol=sw_symbol, units=units, order_type=LIMIT_SELL, limit_unit_price=limit_unit_price, asset_quantity=asset_quantity
            )
        except ZeroUnitsOrdered as e:
            return self._make_rejected_order_result(sw_symbol=symbol, units=units, order_type=LIMIT_SELL, sw_asset_quantity=asset_quantity, unit_price=limit_unit_price)
        except ApiRateLimit as e:
            return self._make_rejected_order_result(sw_symbol=symbol, units=units, order_type=LIMIT_SELL, sw_asset_quantity=asset_quantity, unit_price=limit_unit_price)
        except:
            raise


    def _submit_order(
        self, sw_symbol: str, units: int, order_type: int, asset_quantity:str, limit_unit_price: float = None
    ) -> OrderResult:
        """Submits an order (either buy or sell) based on value.  Note that this should not be called from outside of this class

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
        
        # this is the most frustrating API ever
        precision = self.get_precision(yf_symbol = self._sw_to_yf(sw_symbol))
        if order_type == LIMIT_SELL:
            limit_unit_price = round(1 / limit_unit_price, precision)

        # swyftx api expects symbols in upper case....
        primary = self.default_currency.upper()
        secondary = sw_symbol.upper()
        quantity = round(units, precision)

        orders_create_object = orders.OrdersCreate(
            primary=primary,
            secondary=secondary,
            quantity=quantity,
            assetQuantity=asset_quantity,
            orderType=order_type,
            trigger=limit_unit_price,
        )

        try:
            response = self.api.request(orders_create_object)
        except pyswyft.exceptions.PySwyftError as e:
            this_exception = self.get_exception(exception=e)
            if this_exception["error"] == "ArgsError":
                # usually happens when you request a non-sensical order like 0 quantity of units
                if quantity == 0:
                    log_wp.error(f"Can't buy/sell zero units: {str(orders_create_object)}")
                    raise ZeroUnitsOrdered(f"Failed to sell/buy 0 units")
            
            if this_exception["error"] == "RateLimit":
                # try again
                log_wp.error(f"API rate limit triggered: {str(orders_create_object)}")
                raise ApiRateLimit(this_exception["message"])
                
            raise
        except:
            raise

        # this annoys me, but LIMIT orders don't return any detail about the
        # order on lodgement - whereas MARKET does
        return self.get_order(order_id=response["orderUuid"], back_testing_date=None)

    def get_exception(self, exception:pyswyft.exceptions.PySwyftError):
        inflated =  json.loads(exception.args[0])
        return inflated["error"]

    def get_order(self, order_id: str, back_testing_date=None)->OrderResult:
        if order_id[:9] == "REJECTED-":
            # this is one of my shonky orders
            return OrderResult(order_object=self.rejected_orders[order_id], asset_list_by_id=self._asset_list_by_id)

        response = self.api.request(orders.OrdersGetOrder(orderID=order_id))
        # orders_create_object: orders.OrdersCreate):
        return OrderResult(
            order_object=response, asset_list_by_id=self._asset_list_by_id
        )

    def cancel_order(self, order_id: str, back_testing_date=None) ->OrderResult:
        try:
            cancel_request = self.api.request(orders.OrdersCancel(orderID=order_id))
        except pyswyft.exceptions.PySwyftError as e:
            # while i'm trying to catch swyftx errors
            print("banana")
        except:
            # if we get here, I've created a specific exception for this situation so I'm okay to emit it upstream
            raise

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
        
        # TODO if we don't hold any of this, do something smart
        position = self.get_position(symbol)

        try:
            request = self.sell_order_market(symbol=symbol, units=position.quantity)
        except pyswyft.exceptions.PySwyftError as e:
            print("banana")
        return request

    def _make_rejected_order_result(self, sw_symbol:str, units:float, order_type:int, sw_asset_quantity:str, unit_price:float=None):
        order_id = "REJECTED-" + utils.generate_id()
        primary_asset_yf = self.default_currency
        secondary_asset_yf = self._sw_to_yf(sw_symbol)
        asset_quantity_yf = self._sw_to_yf(sw_asset_quantity)

        primary_asset = self._asset_list_by_yf_symbol[primary_asset_yf].id
        secondary_asset = self._asset_list_by_yf_symbol[secondary_asset_yf].id
        #self._asset_list_by_symbol[sw_symbol]["id"]
        #asset_quantity = self._asset_list_by_symbol[asset_quantity]["id"]
        asset_quantity = self._asset_list_by_yf_symbol[asset_quantity_yf].id
        epoch = datetime.utcfromtimestamp(0)
        now = int((datetime.now() - epoch).total_seconds() * 1000.0)

        order = {
            'orderUuid':order_id,
            'order_type':order_type,
            'primary_asset':primary_asset,
            'secondary_asset':secondary_asset,
            'quantity_asset':asset_quantity,
            'quantity':units,
            'trigger':unit_price,
            'status':8, # rejected by system
            'created_time':now,
            'updated_time':now,
            'amount':None,
            'total':None,
            'rate':None,
            'audValue':None,
            'userCountryValue':None,
            'feeAmount':None,
            'feeAsset':None,
            'feeAudValue':None,
            'feeUserCountryValue':None,
        }

        self.rejected_orders[order_id] = order
        return order

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
    a = api.close_position("XRP-USD")
    b = api.buy_order_limit(symbol="XRP-USD", units=52, unit_price=0.1)
    c = api.buy_order_market(symbol="XRP-USD", units=75)
    d = api.sell_order_market(symbol="XRP-USD", units=10)
    e = api.sell_order_limit(symbol="XRP-USD", units=10, unit_price=2)
    f= api.sell_order_limit(symbol="XRP-USD", units=11, unit_price=3)
    g= api.sell_order_limit(symbol="XRP-USD", units=12, unit_price=8)
    h=api.list_positions()
    i=api.get_position(symbol="XRP-USD")
    j=api.list_orders()
    k=api.list_orders(filled=True)
    l=api.list_orders(cancelled=True)
    m=api.list_orders(still_open=True)
    n=api.close_position("XRP-USD")


    print("a")
