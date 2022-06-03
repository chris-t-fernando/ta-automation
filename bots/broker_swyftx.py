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
"BTC-USD":"BTC",
"TRX-USD":"TRX",
"ETH-USD":"ETH",
"XRP-USD":"XRP",
"BCH-USD":"BCH",
"EOS-USD":"EOS",
"XVG-USD":"XVG",
"NEO-USD":"NEO",
"LTC-USD":"LTC",
"ADA-USD":"ADA",
"BNB-USD":"BNB",
"IOTA-USD":"IOTA",
"QTUM-USD":"QTUM",
"ETC-USD":"ETC",
"WTC-USD":"WTC",
"ZRX-USD":"ZRX",
"SUB-USD":"SUB",
"OMG-USD":"OMG",
"XMR-USD":"XMR",
"ZEC-USD":"ZEC",
"BAT-USD":"BAT",
"LSK-USD":"LSK",
"SALT-USD":"SALT",
"FUN-USD":"FUN",
"MCO-USD":"MCO",
"POWR-USD":"POWR",
"VGX-USD":"VGX",
"WAVES-USD":"WAVES",
"ADX-USD":"ADX",
"KMD-USD":"KMD",
"GBP-USD":"GBP",
"JPY-USD":"JPY",
"EUR-USD":"EUR",
"USD-USD":"USD",
"BTT-USD":"BTT",
"DASH-USD":"DASH",
"DENT-USD":"DENT",
"HOT-USD":"HOT",
"LINK-USD":"LINK",
"MTL-USD":"MTL",
"NANO-USD":"NANO",
"NPXS-USD":"NPXS",
"XLM-USD":"XLM",
"ZIL-USD":"ZIL",
"SYS-USD":"SYS",
"PPT-USD":"PPT",
"VET-USD":"VET",
"ONT-USD":"ONT",
"XEM-USD":"XEM",
"BTG-USD":"BTG",
"USDC-USD":"USDC",
"DCR-USD":"DCR",
"TUSD-USD":"TUSD",
"REP-USD":"REP",
"BCD-USD":"BCD",
"RVN-USD":"RVN",
"BTS-USD":"BTS",
"ICX-USD":"ICX",
"PAX-USD":"PAX",
"AE-USD":"AE",
"SC-USD":"SC",
"ATOM-USD":"ATOM",
"STEEM-USD":"STEEM",
"ENJ-USD":"ENJ",
"THETA-USD":"THETA",
"STRAT-USD":"STRAT",
"SNT-USD":"SNT",
"GNT-USD":"GNT",
"ELF-USD":"ELF",
"ARDR-USD":"ARDR",
"DOGE-USD":"DOGE",
"NXS-USD":"NXS",
"IOST-USD":"IOST",
"ZEN-USD":"ZEN",
"MANA-USD":"MANA",
"XTZ-USD":"XTZ",
"RLC-USD":"RLC",
"HBAR-USD":"HBAR",
"GAS-USD":"GAS",
"ONG-USD":"ONG",
"STX-USD":"STX",
"LEND-USD":"LEND",
"ALGO-USD":"ALGO",
"ENG-USD":"ENG",
"AGI-USD":"AGI",
"KNC-USD":"KNC",
"TNT-USD":"TNT",
"AION-USD":"AION",
"REN-USD":"REN",
"WRX-USD":"WRX",
"HC-USD":"HC",
"BUSD-USD":"BUSD",
"XZC-USD":"XZC",
"FTT-USD":"FTT",
"LRC-USD":"LRC",
"CHZ-USD":"CHZ",
"WIN-USD":"WIN",
"BRD-USD":"BRD",
"FET-USD":"FET",
"LTO-USD":"LTO",
"WABI-USD":"WABI",
"NKN-USD":"NKN",
"PERL-USD":"PERL",
"RCN-USD":"RCN",
"DATA-USD":"DATA",
"KAVA-USD":"KAVA",
"GRS-USD":"GRS",
"OGN-USD":"OGN",
"COTI-USD":"COTI",
"ARK-USD":"ARK",
"SNX-USD":"SNX",
"ERD-USD":"ERD",
"COMP-USD":"COMP",
"BAND-USD":"BAND",
"DGB-USD":"DGB",
"BNT-USD":"BNT",
"DOT-USD":"DOT",
"FTM-USD":"FTM",
"IOTX-USD":"IOTX",
"MATIC-USD":"MATIC",
"MFT-USD":"MFT",
"NULS-USD":"NULS",
"OCEAN-USD":"OCEAN",
"ONE-USD":"ONE",
"REQ-USD":"REQ",
"SOL-USD":"SOL",
"SRM-USD":"SRM",
"SXP-USD":"SXP",
"TFUEL-USD":"TFUEL",
"EGLD-USD":"EGLD",
"KSM-USD":"KSM",
"YFI-USD":"YFI",
"TRB-USD":"TRB",
"MKR-USD":"MKR",
"RSR-USD":"RSR",
"PAXG-USD":"PAXG",
"UMA-USD":"UMA",
"YFII-USD":"YFII",
"HIVE-USD":"HIVE",
"STORJ-USD":"STORJ",
"JST-USD":"JST",
"IRIS-USD":"IRIS",
"DIA-USD":"DIA",
"TOMO-USD":"TOMO",
"WAN-USD":"WAN",
"BZRX-USD":"BZRX",
"ANKR-USD":"ANKR",
"BLZ-USD":"BLZ",
"NMR-USD":"NMR",
"SAND-USD":"SAND",
"ARPA-USD":"ARPA",
"CELR-USD":"CELR",
"VTHO-USD":"VTHO",
"LOOM-USD":"LOOM",
"CVC-USD":"CVC",
"AST-USD":"AST",
"CHR-USD":"CHR",
"NAS-USD":"NAS",
"DUSK-USD":"DUSK",
"BAL-USD":"BAL",
"STPT-USD":"STPT",
"PNT-USD":"PNT",
"COCOSOLD-USD":"COCOSOLD",
"FIO-USD":"FIO",
"KEY-USD":"KEY",
"DREPOLD-USD":"DREPOLD",
"CTSI-USD":"CTSI",
"VITE-USD":"VITE",
"NAV-USD":"NAV",
"NEBL-USD":"NEBL",
"ANT-USD":"ANT",
"MDT-USD":"MDT",
"TCT-USD":"TCT",
"TROY-USD":"TROY",
"MBL-USD":"MBL",
"OXT-USD":"OXT",
"AVAX-USD":"AVAX",
"SUNOLD-USD":"SUNOLD",
"SUSHI-USD":"SUSHI",
"LUNC-USD":"LUNC",
"WNXM-USD":"WNXM",
"RUNE-USD":"RUNE",
"CRV-USD":"CRV",
"HNT-USD":"HNT",
"NZD-USD":"NZD",
"SCRT-USD":"SCRT",
"ORN-USD":"ORN",
"UTK-USD":"UTK",
"XVS-USD":"XVS",
"AAVE-USD":"AAVE",
"FIL-USD":"FIL",
"INJ-USD":"INJ",
"FLM-USD":"FLM",
"WING-USD":"WING",
"ALPHA-USD":"ALPHA",
"BEL-USD":"BEL",
"POLY-USD":"POLY",
"VIDT-USD":"VIDT",
"BOT-USD":"BOT",
"NEAR-USD":"NEAR",
"DNT-USD":"DNT",
"AKRO-USD":"AKRO",
"STRAX-USD":"STRAX",
"GLM-USD":"GLM",
"AUDIO-USD":"AUDIO",
"GVT-USD":"GVT",
"QSP-USD":"QSP",
"CND-USD":"CND",
"VIBE-USD":"VIBE",
"WPR-USD":"WPR",
"QLC-USD":"QLC",
"MITH-USD":"MITH",
"COS-USD":"COS",
"STMX-USD":"STMX",
"AVA-USD":"AVA",
"WBTC-USD":"WBTC",
"MDA-USD":"MDA",
"AERGO-USD":"AERGO",
"HARD-USD":"HARD",
"FOR-USD":"FOR",
"SKL-USD":"SKL",
"DLT-USD":"DLT",
"OST-USD":"OST",
"PSG-USD":"PSG",
"JUV-USD":"JUV",
"MTH-USD":"MTH",
"OAX-USD":"OAX",
"EVX-USD":"EVX",
"VIB-USD":"VIB",
"RDN-USD":"RDN",
"BCPT-USD":"BCPT",
"CDT-USD":"CDT",
"AMB-USD":"AMB",
"CMT-USD":"CMT",
"GO-USD":"GO",
"CTXC-USD":"CTXC",
"POA-USD":"POA",
"ROSE-USD":"ROSE",
"VIA-USD":"VIA",
"SKY-USD":"SKY",
"QKC-USD":"QKC",
"CTK-USD":"CTK",
"YOYO-USD":"YOYO",
"1INCH-USD":"1INCH",
"CELO-USD":"CELO",
"COCOS-USD":"COCOS",
"FIRO-USD":"FIRO",
"TWT-USD":"TWT",
"TRU-USD":"TRU",
"REEF-USD":"REEF",
"AXS-USD":"AXS",
"BTCSTOLD-USD":"BTCSTOLD",
"SNMOLD-USD":"SNMOLD",
"APPC-USD":"APPC",
"IDEX-USD":"IDEX",
"UNFI-USD":"UNFI",
"DODO-USD":"DODO",
"CAKE-USD":"CAKE",
"RIF-USD":"RIF",
"NBS-USD":"NBS",
"FRONT-USD":"FRONT",
"ACM-USD":"ACM",
"GXS-USD":"GXS",
"AUCTION-USD":"AUCTION",
"BADGER-USD":"BADGER",
"OM-USD":"OM",
"LINA-USD":"LINA",
"BTCST-USD":"BTCST",
"DEGO-USD":"DEGO",
"RAMP-USD":"RAMP",
"PERP-USD":"PERP",
"LIT-USD":"LIT",
"TVK-USD":"TVK",
"FIS-USD":"FIS",
"PHA-USD":"PHA",
"ALICE-USD":"ALICE",
"DREP-USD":"DREP",
"PUNDIX-USD":"PUNDIX",
"EPS-USD":"EPS",
"SUPER-USD":"SUPER",
"AUTO-USD":"AUTO",
"ASR-USD":"ASR",
"GTO-USD":"GTO",
"CFX-USD":"CFX",
"SNM-USD":"SNM",
"SHIB-USD":"SHIB",
"AGIX-USD":"AGIX",
"SUN-USD":"SUN",
"TORN-USD":"TORN",
"GTC-USD":"GTC",
"MDX-USD":"MDX",
"MASK-USD":"MASK",
"BAR-USD":"BAR",
"MIR-USD":"MIR",
"TLM-USD":"TLM",
"KEEP-USD":"KEEP",
"ERN-USD":"ERN",
"LPT-USD":"LPT",
"QUICK-USD":"QUICK",
"NU-USD":"NU",
"POLS-USD":"POLS",
"FORTH-USD":"FORTH",
"ICP-USD":"ICP",
"XYM-USD":"XYM",
"QNT-USD":"QNT",
"FLOW-USD":"FLOW",
"CKB-USD":"CKB",
"USDP-USD":"USDP",
"AR-USD":"AR",
"SSV-USD":"SSV",
"ILV-USD":"ILV",
"RAY-USD":"RAY",
"DYDX-USD":"DYDX",
"CLV-USD":"CLV",
"TRIBE-USD":"TRIBE",
"FARM-USD":"FARM",
"BOND-USD":"BOND",
"BURGER-USD":"BURGER",
"DEXE-USD":"DEXE",
"MBOX-USD":"MBOX",
"SFP-USD":"SFP",
"SLP-USD":"SLP",
"C98-USD":"C98",
"YGG-USD":"YGG",
"MLN-USD":"MLN",
"GALA-USD":"GALA",
"GNO-USD":"GNO",
"BAKE-USD":"BAKE",
"AGLD-USD":"AGLD",
"TKO-USD":"TKO",
"ALPACA-USD":"ALPACA",
"MINA-USD":"MINA",
"MOVR-USD":"MOVR",
"GHST-USD":"GHST",
"AMP-USD":"AMP",
"OG-USD":"OG",
"PLA-USD":"PLA",
"PROM-USD":"PROM",
"RAD-USD":"RAD",
"RARE-USD":"RARE",
"PYR-USD":"PYR",
"ATM-USD":"ATM",
"QI-USD":"QI",
"FIDA-USD":"FIDA",
"ENS-USD":"ENS",
"FXS-USD":"FXS",
"JASMY-USD":"JASMY",
"LAZIO-USD":"LAZIO",
"CITY-USD":"CITY",
"OOKI-USD":"OOKI",
"SGB-USD":"SGB",
"BTTC-USD":"BTTC",
"XNO-USD":"XNO",
"T-USD":"T",
"USTC-USD":"USTC",
"RNDR-USD":"RNDR",
"IMX-USD":"IMX",
"APE-USD":"APE",
"JOE-USD":"JOE",
"FLUX-USD":"FLUX",
"SPELL-USD":"SPELL",
"ALCX-USD":"ALCX",
"KDA-USD":"KDA",
"KLAY-USD":"KLAY",
"DOCK-USD":"DOCK",
"XEC-USD":"XEC",
"GMT-USD":"GMT",
"PEOPLE-USD":"PEOPLE",
"TAUD-USD":"TAUD",
"DFI-USD":"DFI",
"WAXP-USD":"WAXP",
"REI-USD":"REI",
"EPX-USD":"EPX",
"LUNA-USD":"LUNA",
"LDO-USD":"LDO",
"NEXO-USD":"NEXO",
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

        self._build_asset_list()

        # set up data structures
        self.default_currency = "USD"

    def get_precision(self, yf_symbol:str)->int:
        return 5

    def get_broker_name(self)->str:
        return "swyftx"

    def get_assets(self)->dict:
        return self._asset_list_by_symbol

    def get_asset(self, symbol:str)->Asset:
        sw_symbol = YF_SYMBOL_MAP[symbol]
        return self._asset_list_by_symbol[sw_symbol]

    def get_asset_by_id(self, id)->Asset:
        return self._asset_list_by_id[id]


    def _build_asset_list(self)->bool:
        # this is munted. there's no Markets endpoint in demo?!
        temp_api = pyswyft.API(access_token=self.access_token, environment="live")
        swyftx_assets = temp_api.request(markets.MarketsAssets())

        # set up asset lists
        self._asset_list_by_id = self._structure_asset_dict_by_id(swyftx_assets)
        self._asset_list_by_symbol = self._structure_asset_dict_by_symbol(swyftx_assets)

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
        
        # this is the most frustrating API ever
        if order_type == LIMIT_BUY:
            asset_quantity = sw_symbol.upper()
        elif order_type == LIMIT_SELL:
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
        # this annoys me, but LIMIT orders don't return any detail about the
        # order on lodgement - whereas MARKET does
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
