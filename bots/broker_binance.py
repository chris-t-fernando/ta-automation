from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import boto3

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
    BrokerAPIError
)

from datetime import datetime
import pandas as pd
import boto3
import logging
import math
from dateutil.relativedelta import relativedelta


log_wp = logging.getLogger("binance")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("binance.log")
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


class OrderResult(IOrderResult):
    def __init__(self, response, binance_to_yf_symbol_map: dict):
        self._raw_response = response

        # convert side and type combination into one of my static
        self.order_type = self._convert_order_type_to_constant(
            order_side=response.side, order_type=response.type
        )
        self.order_type_text = ORDER_MAP_INVERTED[self.order_type]

        self.order_id = response.id
        self.symbol = self._to_yf(response.symbol, binance_to_yf_symbol_map)

        if response.type == "limit":
            self.ordered_unit_quantity = float(response.qty)
            self.ordered_unit_price = float(response.limit_price)
            self.ordered_total_value = (
                self.ordered_unit_quantity * self.ordered_unit_price
            )

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

# concrete implementation of trade api for binance
#class BinanceAPI(ITradeAPI):
class BinanceAPI():
    supported_crypto_symbols_bin = []

    def __init__(
        self,
        binance_api_key: str,
        binance_secret_key: str,
        real_money_trading=False,
        back_testing: bool = False,
        back_testing_balance: float = None,
    ):

        
        self.back_testing = back_testing

        if real_money_trading:
            self.api = Client(binance_api_key, binance_secret_key)
        else:
            self.api = Client(binance_api_key, binance_secret_key, testnet=True)

        self.default_currency = "BUSD"

        # set up asset lists
        self._build_asset_list()
        
        # self.asset_list_by_id = self._structure_asset_dict_by_id(assets)
        #self.asset_list_by_symbol = self._structure_asset_dict_by_symbol(self.assets)

        #self.supported_crypto_symbols_bin = self._get_crypto_symbols()
        #self._create_yf_to_alpaca_symbol_mapping(self.supported_crypto_symbols_bin)
        #self.supported_crypto_symbols_yf = self._get_crypto_symbols_yf()

    def _build_asset_list(self):
        prices = self.api.get_all_tickers()





if __name__ == "__main__":
    ssm = boto3.client("ssm")
    api_key = (
        ssm.get_parameter(Name="/tabot/paper/binance/api_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    secret_key = (
        ssm.get_parameter(Name="/tabot/paper/binance/secret_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    api = BinanceAPI(binance_api_key=api_key, binance_secret_key=secret_key)
