from abc import ABC, abstractmethod

MARKET_BUY = 1
MARKET_SELL = 2
LIMIT_BUY = 3
LIMIT_SELL = 4
STOP_LIMIT_BUY = 5
STOP_LIMIT_SELL = 6


class NotImplementedException(Exception):
    ...


class IAsset(ABC):
    symbol: str
    balance: float

    @abstractmethod
    def __init__(self, symbol, balance):
        ...


class IAccount(ABC):
    assets: list

    @abstractmethod
    def __init__(self, assets: list):
        ...


class IPosition(ABC):
    symbol: str
    quantity: float

    @abstractmethod
    def __init__(self, symbol, quantity):
        ...


class IOrderResult(ABC):
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

    @abstractmethod
    def __init__(self, response: dict, orders_create_object):
        ...


# interface for api
class ITradeAPI(ABC):
    @abstractmethod
    def __init__(self, api_key: str, environment: str = "paper"):
        ...

    @abstractmethod
    def get_broker_name(self):
        ...

    @abstractmethod
    def get_account(self):
        ...

    @abstractmethod
    def list_positions(self):
        ...

    @abstractmethod
    def get_bars(self):
        ...

    @abstractmethod
    def buy_order_market(self):
        ...

    @abstractmethod
    def buy_order_limit(self):
        ...

    @abstractmethod
    def sell_order_market(
        self, symbol: str, units: float, back_testing_unit_price: None
    ):
        ...

    @abstractmethod
    def sell_order_limit(self, symbol: str, units: float, unit_price: float):
        ...

    @abstractmethod
    def cancel_order(self, order_id: str):
        ...

    @abstractmethod
    def list_orders(self):
        ...

    @abstractmethod
    def get_order(self):
        ...

    @abstractmethod
    def close_position(self, symbol: str):
        ...

    @abstractmethod
    def get_position(self, symbol):
        ...
