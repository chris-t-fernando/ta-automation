from abc import ABC, abstractmethod
from pandas import DataFrame
import datetime

MARKET_BUY = 1
MARKET_SELL = 2
LIMIT_BUY = 3
LIMIT_SELL = 4
STOP_LIMIT_BUY = 5
STOP_LIMIT_SELL = 6


class NotImplementedException(Exception):
    ...

class BrokerAPIError(Exception):...

class UnknownSymbol(Exception):...
class DelistedAsset(Exception):...
class UntradeableAsset(Exception):...  
class MalformedOrderResult(Exception):...  
class ZeroUnitsOrdered(Exception):...
class ApiRateLimit(Exception):...
class MinimumOrderError(Exception):...

class Account:
    assets: dict

    def __init__(self, assets: dict):
        self.assets = assets


class Position:
    symbol: str
    quantity: float

    def __init__(self, symbol, quantity):
        self.symbol = symbol
        self.quantity = float(quantity)


class Asset:
    def __init__(
        self, symbol, min_order_size, min_trade_increment, min_price_increment
    ):
        self.symbol = symbol
        self.min_order_size = min_order_size
        self.min_trade_increment = min_trade_increment
        self.min_price_increment = min_price_increment


class IOrderResult(ABC):
    _raw_response:dict
    order_type:int
    order_type_text:str
    order_id:str
    symbol:str
    ordered_unit_quantity:float
    ordered_unit_price:float
    ordered_total_value:float
    filled_unit_quantity:float
    filled_unit_price:float
    filled_total_value:float
    status:int
    status_text:str
    status_summary:str
    success:bool
    fees:float
    create_time:datetime
    update_time:datetime
    closed:bool
        
    @abstractmethod
    def __init__(self, response: dict, orders_create_object):
        ...

    def validate(self):
        failed = False
        required_attributes = ["_raw_response",
    "status",
    "status_text",
    "status_summary",
    "success",
    "order_type",
    "order_type_text",
    "order_id",
    "symbol",
    "ordered_unit_quantity",
    "ordered_unit_price",
    "ordered_total_value",
    "filled_unit_quantity",
    "filled_unit_price",
    "filled_total_value",
    "fees",
    "create_time",
    "update_time",
    "closed"]
        
        for attribute in required_attributes:
            if not hasattr(self, attribute):
                raise MalformedOrderResult(f"OrderResult is missing {attribute}")
        return True

    def as_dict(self):
        return {
            "symbol": self.symbol,
            "order_id": self.order_id,
            "order_type": self.order_type,
            "order_type_text": self.order_type_text,
            "play_id": self.play_id,
            "status": self.status,
            "status_summary": self.status_summary,
            "status_text": self.status_text,
            "ordered_unit_quantity": self.ordered_unit_quantity,
            "ordered_unit_price": self.ordered_unit_price,
            "ordered_total_value": self.ordered_total_value,
            "filled_unit_quantity": self.filled_unit_quantity,
            "filled_unit_price": self.filled_unit_price,
            "filled_total_value": self.filled_total_value,
            "fees": self.fees,
            "success": self.success,
            "create_time": self.create_time,
            "update_time": self.update_time,
        }


# interface for api
class ITradeAPI(ABC):
    @abstractmethod
    def __init__(self, api_key: str, environment: str = "paper"):
        ...

    @abstractmethod
    def get_broker_name(self) -> str:
        ...

    @abstractmethod
    def get_account(self) -> Account:
        ...

    @abstractmethod
    def list_positions(self) -> Position:
        ...

    @abstractmethod
    def get_bars(self,symbol: str, start: str, end: str = None, interval: str = "1d") -> DataFrame:
        ...

    @abstractmethod
    def buy_order_market(self, symbol:str, units:int, back_testing_date=None) -> IOrderResult:
        ...

    @abstractmethod
    def buy_order_limit(self, symbol: str, units: float, unit_price: float, back_testing_date=None) -> IOrderResult:
        ...

    @abstractmethod
    def sell_order_market(
        self, symbol: str, units: float = None, back_testing_date=None
    ) -> IOrderResult:
        ...

    @abstractmethod
    def sell_order_limit(
        self, symbol: str, units: float, unit_price: float, back_testing_date=None
    ) -> IOrderResult:
        ...

    @abstractmethod
    def cancel_order(self, order_id: str, back_testing_date=None) -> IOrderResult:
        ...

    @abstractmethod
    def list_orders(self) -> list:
        ...

    @abstractmethod
    def get_order(self, order_id, back_testing_date) -> IOrderResult:
        ...

    @abstractmethod
    def close_position(self, symbol: str) -> IOrderResult:
        ...

    @abstractmethod
    def get_position(self, symbol: str) -> Position:
        ...

    @abstractmethod
    def get_precision(self, symbol: str) -> int:
        ...

    @abstractmethod
    def get_asset(self, symbol: str) -> Asset:
        ...

    @abstractmethod
    def validate_symbol(self, symbol:str)->bool:...