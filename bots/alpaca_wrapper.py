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
from alpaca_trade_api import REST, entity
import pandas as pd
import boto3
import logging
import json
import math
from dateutil.relativedelta import relativedelta

log_wp = logging.getLogger("alpaca")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("macd.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)

# CONSTANTS
MARKET_BUY = 1
MARKET_SELL = 2
LIMIT_BUY = 3
LIMIT_SELL = 4
STOP_LIMIT_BUY = 5
STOP_LIMIT_SELL = 6

ORDER_STATUS_SUMMARY_TO_ID = {
    "cancelled": {6, 11, 8, 12, 13, 18, 19},
    "open": {1, 3, 5, 14, 15, 16},
    "pending": {},
    "filled": {4},
}

ORDER_STATUS_ID_TO_SUMMARY = {
    1: "open",
    3: "open",
    4: "filled",
    5: "open",
    6: "cancelled",
    11: "cancelled",
    8: "cancelled",
    12: "cancelled",
    13: "cancelled",
    14: "open",
    15: "open",
    16: "open",
    17: "filled",
    18: "cancelled",
    19: "cancelled",
    20: "filled",
}

ORDER_STATUS_TEXT = {
    1: "new",
    3: "partially_filled",
    4: "filled",
    5: "done_for_day",
    6: "canceled",
    11: "expired",
    8: "replaced",
    12: "pending_cancel",
    13: "pending_replace",
    14: "accepted",
    15: "pending_new",
    16: "accepted_for_bidding",
    17: "stopped",
    18: "rejected",
    19: "suspended",
    20: "calculated",
}
ORDER_STATUS_TEXT_INVERTED = {y: x for x, y in ORDER_STATUS_TEXT.items()}

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
        self.quantity = float(quantity)


class OrderResult(IOrderResult):
    order_id: str
    sold_symbol: str
    bought_symbol: str
    order_value: float
    order_currency: str
    order_currency_id: int
    limit_price: float
    status: int
    status_text: str
    status_summary: str
    order_type: int
    order_type_text: str
    create_time: int
    update_time: int
    total_value: float
    requested_units: float
    requested_unit_price: float
    requested_total_value: float
    success: bool
    _raw_response: dict
    # _raw_request

    # TODO add requested values too, like swyftx
    def __init__(self, response: entity.Order):
        self._raw_response = response

        # convert side and type combination into one of my static
        self.order_type = self._convert_order_type_to_constant(
            order_side=response.side, order_type=response.type
        )
        self.order_type_text = ORDER_MAP_INVERTED[self.order_type]

        self.order_id = response.id

        if response.side == "buy":
            self.bought_symbol = response.symbol
            self.bought_id = response.asset_id
        else:
            self.sold_symbol = response.symbol
            self.sold_id = response.asset_id

        self.unit_quantity = float(response.qty)

        # quantity is what you're paying with - only known if its a limit order
        if response.type == "limit":
            self.order_value = float(response.limit_price) * float(response.qty)
            self.limit_price = float(response.limit_price)

        else:
            self.order_value = None
            self.limit_price = None

        self.order_currency = "USD"
        self.order_currency_id = None

        self.status = ORDER_STATUS_TEXT_INVERTED[response.status]
        self.status_text = response.status
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]

        self.success = (
            self.status in ORDER_STATUS_SUMMARY_TO_ID["open"]
            or self.status in ORDER_STATUS_SUMMARY_TO_ID["filled"]
        )

        self.create_time = response.submitted_at
        self.update_time = response.updated_at

    def _convert_order_type_to_constant(self, order_side, order_type):
        if order_side == "buy":
            if order_type == "limit":
                return LIMIT_BUY
            elif order_type == "market":
                return MARKET_BUY
            else:
                raise ValueError(f"Unknown order type: {order_type}")
        elif order_side == "sell":
            if order_type == "limit":
                return LIMIT_SELL
            elif order_type == "market":
                return MARKET_SELL
            else:
                raise ValueError(f"Unknown order type: {order_type}")
        else:
            raise ValueError(f"Unknown market side: {order_side}")


# concrete implementation of trade api for alpaca
class AlpacaAPI(ITradeAPI):
    supported_crypto_symbols = []

    def __init__(
        self,
        alpaca_key_id: str,
        alpaca_secret_key: str,
        real_money_trading=False,
        back_testing: bool = False,
    ):
        # self.order_types = ORDER_TYPES
        if real_money_trading:
            base_url = "https://api.alpaca.markets"
        else:
            base_url = "https://paper-api.alpaca.markets"

        self.back_testing = back_testing

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

    def get_broker_name(self):
        return "alpaca"

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
            return_dict[str(asset.symbol)] = asset
        return return_dict

    def get_account(self) -> Account:
        request = self.api.get_account()
        currency = request.currency
        currency = currency
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

    def _submit_order(
        self, symbol: str, units: int, order_type: int, trigger: bool = None
    ) -> OrderResult:
        if order_type > 4:
            raise NotImplementedException(
                f"STOPLIMITBUY and STOPLIMITSELL is not implemented yet"
            )

        if "BUY" in ORDER_MAP_INVERTED[order_type]:
            side = "buy"
        else:
            side = "sell"

        if "MARKET" in ORDER_MAP_INVERTED[order_type]:
            alpaca_type = "market"
            limit_price = None
        else:
            alpaca_type = "limit"
            limit_price = round(trigger, 2)

        # do the order
        response = self.api.submit_order(
            symbol=symbol,
            qty=math.floor(units),
            side=side,
            type=alpaca_type,
            limit_price=limit_price,
            time_in_force="day",
        )

        # get the order so we have all the info about it
        return self.get_order(order_id=response.id)

    def get_order(self, order_id: str):
        all_orders = self.list_orders()
        for o in all_orders:
            if o.order_id == order_id:
                return o

        # return OrderResult(
        #    order_object=response, asset_list_by_id=self._asset_list_by_id
        # )

    # todo: basically everything after this!
    def _translate_order_types(self, order_type):
        if order_type == "MARKET_BUY":
            return "buy"
        elif order_type == "MARKET_SELL":
            return "sell"
        else:
            raise NotImplementedException

    def order_create_by_value(self, *args, **kwargs):
        # TODO - normalise this!
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

    def sell_order_limit(self, symbol: str, units: float, unit_price: float):
        return self._submit_order(
            symbol=symbol, units=units, order_type=LIMIT_SELL, trigger=unit_price
        )

        return self.api.submit_order(
            symbol=symbol,
            qty=units,
            side="sell",
            order_type="limit",
            limit_price=str(unit_price),
            time_in_force="day",
        )

    def buy_order_limit(self, symbol: str, units: float, unit_price: float):
        return self._submit_order(
            symbol=symbol, units=units, order_type=LIMIT_BUY, trigger=unit_price
        )
        return self.api.submit_order(
            symbol=symbol,
            qty=math.floor(units),
            side="buy",
            order_type="limit",
            limit_price=str(round(unit_price, 2)),
            time_in_force="day",
        )

    def buy_order_market(self, symbol, units):
        return self._submit_order(symbol=symbol, units=units, order_type=MARKET_BUY)

    def delete_order(self, order_id):
        return self.api.cancel_order(order_id=order_id)

    # TODO signature needs to match swyftx
    # also need the return to match swyftx - currently returns empty list if no active orders
    def list_orders(self, symbol: str = None, symbols: list = None, after: str = None):
        if symbol and symbols:
            raise ValueError("Can't specify both 'symbol' and 'symbols' - choose one")

        # API expects symbols, rather than just symbol, so wrap the symbol in a list
        if symbol:
            symbols = [symbol]

        # after defaults to last 7 days
        if after:
            try:
                pd.Timestamp(after)
            except:
                raise (f"Invalid timestamp provided for 'after' parameter: {after}")
        else:
            after = datetime.now() - relativedelta(days=7)

        # all done validating, now do the thing
        orders = []
        all_orders = self.api.list_orders(status="all", symbols=symbol, after=after)
        for o in all_orders:
            orders.append(OrderResult(response=o))
        return orders

    def sell_order_limit(self):
        ...

    def sell_order_market(
        self, symbol: str, units: float, back_testing_unit_price: None
    ):
        if back_testing_unit_price != None:
            raise NotImplementedError(
                "back_testing_unit_price is not available in real implementation of ITradeAPI"
            )

        return self._submit_order(symbol=symbol, units=units, order_type=MARKET_SELL)

        return self.api.submit_order(
            symbol=symbol,
            qty=units,
            side="sell",
            order_type="market",
            time_in_force="day",
        )

    # TODO i don't think this actually returns an orderresult
    def close_position(self, symbol: str) -> OrderResult:
        return self.api.close_position(symbol=symbol)


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
    api.buy_order_limit(symbol="AAPL", units=5, unit_price=201)
    api.buy_order_market(symbol="AAPL", units=10)
    print("banana")
