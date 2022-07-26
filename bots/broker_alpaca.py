from itradeapi import (
    ITradeAPI,
    IOrderResult,
    Account,
    Position,
    Asset,
    NotImplementedError,
    UnknownSymbolError,
    DelistedAssetError,
    UntradeableAssetError,
    BrokerAPIError,
)
import yfinance as yf
from datetime import datetime
from alpaca_trade_api import REST, entity
from alpaca_trade_api.rest import APIError
import pandas as pd
import boto3
import logging
import math
from dateutil.relativedelta import relativedelta

log_wp = logging.getLogger("alpaca")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("alpaca.log")
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
DUST_SELL = 8

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
    "DUST_SELL": DUST_SELL,
}
ORDER_MAP_INVERTED = {y: x for x, y in ORDER_MAP.items()}

INTERVAL_MAP = {
    "1m": "1Min",
    "5m": "5Min",
    "15m": "15Min",
    "1d": "1Day",
}


class OrderResult(IOrderResult):
    def __init__(self, response: entity.Order, alpaca_to_yf_symbol_map: dict):
        self._raw_response = response

        # convert side and type combination into one of my static
        self.order_type = self._convert_order_type_to_constant(
            order_side=response.side, order_type=response.type
        )
        self.order_type_text = ORDER_MAP_INVERTED[self.order_type]

        self.order_id = response.id
        self.symbol = self._to_yf(response.symbol, alpaca_to_yf_symbol_map)

        if response.type == "limit":
            self.ordered_unit_quantity = float(response.qty)
            self.ordered_unit_price = float(response.limit_price)
            self.ordered_total_value = self.ordered_unit_quantity * self.ordered_unit_price

        else:
            # market orders - so there is only quantity is known, not price or total value
            self.ordered_unit_quantity = float(response.qty)
            self.ordered_unit_price = None
            self.ordered_total_value = None

        self.filled_unit_quantity = float(response.filled_qty)

        if response.filled_avg_price:
            self.filled_unit_price = float(response.filled_avg_price)
            self.filled_total_value = self.filled_unit_quantity * self.filled_unit_price
        else:
            self.filled_unit_price = None
            self.filled_total_value = None

        self.status = ORDER_STATUS_TEXT_INVERTED[response.status]
        self.status_text = response.status
        self.status_summary = ORDER_STATUS_ID_TO_SUMMARY[self.status]

        self.success = (
            self.status in ORDER_STATUS_SUMMARY_TO_ID["open"]
            or self.status in ORDER_STATUS_SUMMARY_TO_ID["filled"]
            or self.status in ORDER_STATUS_SUMMARY_TO_ID["pending"]
        )

        self.fees = 0

        self.create_time = response.submitted_at
        self.update_time = response.updated_at

        open_statuses = ["open", "pending"]
        if self.status_summary in open_statuses:
            self.closed = False
        else:
            self.closed = True

        self.validate()

    def _to_yf(self, alpaca_symbol, alpaca_to_yf_symbol_map) -> str:
        if alpaca_symbol in alpaca_to_yf_symbol_map:
            return alpaca_to_yf_symbol_map[alpaca_symbol]

        # not a crypto symbol
        return alpaca_symbol

    def _convert_order_type_to_constant(self, order_side, order_type) -> int:
        if order_side == "buy":
            if order_type == "limit":
                return LIMIT_BUY
            elif order_type == "market":
                return MARKET_BUY
            elif order_type == "stop_limit":
                return STOP_LIMIT_BUY
            else:
                raise ValueError(f"Unknown order type: {order_type}")
        elif order_side == "sell":
            if order_type == "limit":
                return LIMIT_SELL
            elif order_type == "market":
                return MARKET_SELL
            elif order_type == "stop_limit":
                return STOP_LIMIT_SELL
            else:
                raise ValueError(f"Unknown order type: {order_type}")
        else:
            raise ValueError(f"Unknown market side: {order_side}")


# concrete implementation of trade api for alpaca
class AlpacaAPI(ITradeAPI):
    supported_crypto_symbols_alp = []

    def __init__(
        self,
        alpaca_key_id: str,
        alpaca_secret_key: str,
        real_money_trading=False,
        back_testing: bool = False,
        back_testing_balance: float = None,
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
        self._build_asset_list()

        # self.asset_list_by_id = self._structure_asset_dict_by_id(assets)
        self.asset_list_by_symbol = self._structure_asset_dict_by_symbol(self.assets)

        self.supported_crypto_symbols_alp = self._get_crypto_symbols()
        self._create_yf_to_alpaca_symbol_mapping(self.supported_crypto_symbols_alp)
        self.supported_crypto_symbols_yf = self._get_crypto_symbols_yf()

        self.default_currency = "USD"

    def _build_asset_list(self):
        alpaca_assets = self.api.list_assets()

        self._invalid_assets = {}
        valid_assets = []
        for asset in alpaca_assets:
            if asset.status == "inactive" or not asset.tradable:
                self._invalid_assets[asset.symbol] = asset
            else:
                valid_assets.append(asset)

        self.assets = valid_assets

    def validate_symbol(self, symbol: str):
        al_symbol = self._to_alpaca(symbol)
        # if its valid, just return True
        if al_symbol in self.asset_list_by_symbol.keys():
            return True

        # also check crypto symbols TODO: at some point just merge crypto and normal
        if al_symbol in self.supported_crypto_symbols_alp:
            return True

        #  if its also not in dict of invalid assets, so its just totally unknown
        if al_symbol not in self._invalid_assets:
            raise UnknownSymbolError(f"{symbol} is not known to {self.get_broker_name()}")

        return False

        # so its invalid but the broker does know about it - delisted/not tradeable
        if self._invalid_assets[al_symbol].status == "inactive":
            raise DelistedAssetError(f"{symbol} has been delisted on {self.get_broker_name()}")

        if self._invalid_assets[al_symbol].tradable == False:
            raise UntradeableAssetError(
                f"{symbol} is not currently tradeable on {self.get_broker_name()}"
            )

    def _get_crypto_symbols_yf(self) -> list:
        yf_symbols = []
        for alp_symbol in self.supported_crypto_symbols_alp:
            yf_symbols.append(self._to_yf(alp_symbol))

        return yf_symbols

    def _create_yf_to_alpaca_symbol_mapping(self, crypto_symbols):
        self._yf_to_alpaca_symbol_map = {}
        self._alpaca_to_yf_symbol_map = {}
        for symbol in crypto_symbols:
            if symbol[-4:] == "/USD":
                yf_symbol = symbol[:-4] + "-USD"
                self._yf_to_alpaca_symbol_map[yf_symbol] = symbol
                self._alpaca_to_yf_symbol_map[symbol] = yf_symbol
            elif symbol.find("/") > 0:
                # ignore the other binary pairs, USDT and BTC
                ...
            else:
                # its just a normal nyse stock
                print("banana")

            # location = symbol.rfind("USD")
            # yf_symbol = symbol[:location] + "-USD"
            # self._yf_to_alpaca_symbol_map[yf_symbol] = symbol
            # self._alpaca_to_yf_symbol_map[symbol] = yf_symbol

    def get_broker_name(self) -> str:
        return "alpaca"

    def _get_crypto_symbols(self) -> list:
        # convert this to yf symbols
        crypto_symbols = []
        for asset in self.assets:
            if asset._raw["class"] == "crypto":
                crypto_symbols.append(asset._raw["symbol"])

        return crypto_symbols

    # not implemented
    def _structure_asset_dict_by_id(self, asset_dict):
        raise NotImplementedError("Alpaca does not order assets with a int key")

    def _structure_asset_dict_by_symbol(self, asset_dict) -> dict:
        return_dict = {}
        for asset in asset_dict:
            # code
            # name
            return_dict[str(asset.symbol)] = asset
        return return_dict

    def get_account(self) -> Account:
        request = self.api.get_account()
        currency = request.currency
        account = Account({currency: float(request.cash)})
        return account

    def get_position(self, symbol) -> Position:
        for position in self.list_positions():
            if position.symbol == symbol:
                return position
        return Position(symbol=symbol, quantity=0)

    def list_positions(self) -> list:
        # symbol, quantity
        positions = []
        try:
            for position in self.api.list_positions():
                yf_symbol = self._to_yf(position.symbol)
                positions.append(Position(symbol=yf_symbol, quantity=position.qty))
        except APIError as e:
            raise BrokerAPIError(e)
        return positions

    def get_last_close(self, symbol: str):
        raise NotImplementedError
        history = yf.Ticker(symbol).history(interval="1m", actions=False)
        return history["Close"].iloc[-1]

    def get_bars(self, symbol: str, start: str, end: str, interval: str):
        raise NotImplementedError
        return yf.Ticker(symbol).history(start=start, end=end, interval=interval, actions=False)

    def _to_yf(self, alpaca_symbol) -> str:
        if alpaca_symbol in self._alpaca_to_yf_symbol_map:
            return self._alpaca_to_yf_symbol_map[alpaca_symbol]

        # not a crypto symbol
        return alpaca_symbol

    def _to_alpaca(self, yf_symbol) -> str:
        if yf_symbol in self._yf_to_alpaca_symbol_map:
            return self._yf_to_alpaca_symbol_map[yf_symbol]

        # not a crypto symbol
        return yf_symbol

    def _submit_order(
        self,
        symbol: str,
        units: int,
        order_type: int,
        limit_unit_price: float = None,
        sell_stop_price: float = None,
    ) -> OrderResult:
        if order_type == 5:
            raise NotImplementedError(f"STOPLIMITBUY is not implemented yet")

        alpaca_symbol = self._to_alpaca(symbol)

        order_type_text = ORDER_MAP_INVERTED[order_type]
        if "BUY" in order_type_text:
            side = "buy"
        else:
            side = "sell"

        if "MARKET" in order_type_text:
            # if its a market order
            alpaca_type = "market"
            limit_price = None
            sell_stop_dict = None
        elif "STOP_LIMIT_SELL" == order_type_text:
            # if its a sell limit order
            alpaca_type = "stop_limit"
            limit_price = limit_unit_price
            # precision = self.get_precision(yf_symbol=symbol)
            # limit_price = round(limit_unit_price, precision)
            # sell_stop_price_rounded = round(sell_stop_price, precision)
            # sell_stop_dict = {
            #    "stop_price": sell_stop_price_rounded,
            #    "limit_price": 0.000005,
            # }
            # sell_stop_dict = {"stop_price": "%.10f" % sell_stop_price_rounded}
            # sell_stop_dict = {
            #    "stop_price": "{:f}".format(float(sell_stop_price_rounded))
            # }

        else:
            # just a plain old sell limit order
            alpaca_type = "limit"
            limit_price = limit_unit_price
            # if its a crypto symbol, we can go to ridiculous degrees of precision
            # but if its a normal symbol, it needs to be clipped at a precision of thousandth's (0.000)
            # precision = self.get_precision(yf_symbol=symbol)
            # limit_price = round(limit_unit_price, precision)
            # sell_stop_dict = None
            # sell_stop_price_rounded = round(sell_stop_price, precision)
            # sell_stop_dict = {
            #    "stop_price": sell_stop_price_rounded,
            #    "limit_price": 0.000005,
            # }
        # hack hack hackity hack
        # if limit_price:
        #    limit_price_string = str(limit_price)
        #    dot_at = limit_price_string.find(".")
        #    truncate_at = dot_at + 6
        #    limit_price_truncated = limit_price_string[:truncate_at]
        # else:
        #    limit_price_truncated = limit_price

        # do the order
        try:
            response = self.api.submit_order(
                symbol=alpaca_symbol,
                qty=math.floor(units),
                side=side,
                type=alpaca_type,
                limit_price=limit_price,
                time_in_force="gtc",
                # stop_loss=sell_stop_dict,
            )
        except APIError as e:
            if e == "qty must be >= 10 with trade increment 10":
                updated_units = math.floor(units / 10)
                response = self.api.submit_order(
                    symbol=alpaca_symbol,
                    qty=updated_units,
                    side=side,
                    type=alpaca_type,
                    limit_price=limit_price,
                    time_in_force="day",
                    stop_loss=sell_stop_dict,
                )
            else:
                raise

        # get the order so we have all the info about it
        order = self.get_order(order_id=response.id)
        if order == None:
            print("wuty")
        return order

    def get_order(self, order_id: str, back_testing_date=None) -> OrderResult:
        all_orders = self.list_orders()
        for o in all_orders:
            if o.order_id == order_id:
                return o

    def _translate_order_types(self, order_type) -> str:
        if order_type == "MARKET_BUY":
            return "buy"
        elif order_type == "MARKET_SELL":
            return "sell"
        else:
            raise NotImplementedError

    def sell_order_limit(
        self, symbol: str, units: float, unit_price: float, back_testing_date=None
    ) -> OrderResult:
        return self._submit_order(
            symbol=symbol,
            units=units,
            order_type=LIMIT_SELL,
            limit_unit_price=unit_price,
        )

        return self.api.submit_order(
            symbol=symbol,
            qty=units,
            side="sell",
            order_type="limit",
            limit_price=str(unit_price),
            time_in_force="day",
        )

    def buy_order_limit(
        self, symbol: str, units: float, unit_price: float, back_testing_date=None
    ) -> OrderResult:
        return self._submit_order(
            symbol=symbol,
            units=units,
            order_type=LIMIT_BUY,
            limit_unit_price=unit_price,
        )
        return self.api.submit_order(
            symbol=symbol,
            qty=math.floor(units),
            side="buy",
            order_type="limit",
            limit_price=str(round(unit_price, 2)),
            time_in_force="day",
        )

    def buy_order_market(self, symbol: str, units: int, back_testing_date=None) -> IOrderResult:
        return self._submit_order(symbol=symbol, units=units, order_type=MARKET_BUY)

    def cancel_order(self, order_id: str, back_testing_date=None) -> IOrderResult:
        self.api.cancel_order(order_id=order_id)
        return self.get_order(order_id=order_id, back_testing_date=back_testing_date)

    def list_orders(self, symbol: str = None, symbols: list = None, after: str = None) -> list:
        if symbol and symbols:
            raise ValueError("Can't specify both 'symbol' and 'symbols' - choose one")

        # API expects symbols, rather than just symbol, so wrap the symbol in a list
        if symbol:
            symbols = [self._to_alpaca(symbol)]
        elif symbols:
            alpaca_symbols = []
            for s in symbols:
                alpaca_symbols.append(self._to_alpaca(s))

            symbols = alpaca_symbols

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
        all_orders = self.api.list_orders(status="all", symbols=symbols, after=after)
        for o in all_orders:
            orders.append(
                OrderResult(response=o, alpaca_to_yf_symbol_map=self._alpaca_to_yf_symbol_map)
            )
        return orders

    # def sell_order_limit(
    #    self, symbol: str, units: int, order_type: int, back_testing_date=None
    # ):
    #    return self._submit_order(symbol=symbol, units=units, order_type=MARKET_SELL)

    def sell_order_market(self, symbol: str, units: float, back_testing_date=None) -> IOrderResult:
        return self._submit_order(symbol=symbol, units=units, order_type=MARKET_SELL)

        return self.api.submit_order(
            symbol=symbol,
            qty=units,
            side="sell",
            order_type="market",
            time_in_force="day",
        )

    # TODO i don't think this actually returns an orderresult
    def close_position(self, symbol: str, back_testing_date=None) -> OrderResult:
        alpaca_symbol = self._to_alpaca(yf_symbol=symbol)
        return self.api.close_position(symbol=alpaca_symbol)

    def get_precision(self, yf_symbol: str) -> int:
        if yf_symbol not in self.supported_crypto_symbols_yf:
            return 3
        else:
            return 15

    # def get_asset(self, symbol):
    #    return self.api.get_asset(symbol=self._to_alpaca(symbol))

    def get_asset(self, symbol: str) -> Asset:
        asset = self.api.get_asset(symbol=self._to_alpaca(symbol))
        if hasattr(asset, "min_order_size"):
            min_order_size = float(asset.min_order_size)
            min_trade_increment = float(asset.min_trade_increment)
            min_price_increment = float(asset.price_increment)
        else:
            min_order_size = 1
            min_trade_increment = 1
            min_price_increment = 0.01

        return Asset(
            symbol=symbol,
            min_quantity=min_order_size,
            min_quantity_increment=min_trade_increment,
            min_price_increment=min_price_increment,
        )

    def get_symbol_minimums(self, symbol):
        asset = self.get_asset(symbol=symbol)
        if hasattr(asset, "min_order_size"):
            self.min_order_size = float(asset.min_quantity)
            self.min_trade_increment = float(asset.min_quantity_increment)
            self.min_price_increment = float(asset.min_price_increment)
        else:
            self.min_order_size = 1
            self.min_trade_increment = 1
            self.min_price_increment = 0.001


if __name__ == "__main__":
    import boto3

    ssm = boto3.client("ssm")
    api_key = (
        ssm.get_parameter(Name="/tabot/paper/alpaca/api_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )
    secret_key = (
        ssm.get_parameter(Name="/tabot/paper/alpaca/security_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    api = AlpacaAPI(alpaca_key_id=api_key, alpaca_secret_key=secret_key)
    api.get_account()
    api.list_positions()
    a = api.buy_order_limit(symbol="SOL-USD", units=5, unit_price=201)
    b = api.buy_order_market(symbol="SOL-USD", units=10)
    c = api.sell_order_limit(symbol="SOL-USD", units=5, unit_price=201)
    d = api.sell_order_market(symbol="SOL-USD", units=10)
    print("banana")
