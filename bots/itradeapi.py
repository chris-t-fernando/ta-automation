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
    @abstractmethod
    def __init__(self, response: dict, orders_create_object):
        ...

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
    def cancel_order(self, order_id: str, back_testing_date):
        ...

    @abstractmethod
    def list_orders(self):
        ...

    @abstractmethod
    def get_order(self, order_id, back_testing_date):
        ...

    @abstractmethod
    def close_position(self, symbol: str):
        ...

    @abstractmethod
    def get_position(self, symbol):
        ...
