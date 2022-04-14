from abc import ABC, abstractmethod


class NotImplementedException(Exception):
    ...


class IAccount(ABC):
    positions: list

    @abstractmethod
    def __init__(self, positions):
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
    def get_account(self):
        ...

    @abstractmethod
    def list_positions(self):
        ...

    @abstractmethod
    def get_bars(self):
        ...

    @abstractmethod
    def order_create_by_value(self):
        ...

    @abstractmethod
    def order_create_by_units(self):
        ...

    @abstractmethod
    def order_delete(self):
        ...

    @abstractmethod
    def order_list(self):
        ...

    @abstractmethod
    def close_position(self):
        ...
