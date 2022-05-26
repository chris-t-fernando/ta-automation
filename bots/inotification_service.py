from abc import ABC, abstractmethod
from iparameter_store import IParameterStore


class INotificationService(ABC):
    @abstractmethod
    def __init__(
        self,
        store: IParameterStore,
        back_testing: bool,
        real_money_trading: bool = False,
    ):
        ...

    @abstractmethod
    def send(self, message: str, subject: str = None) -> bool:
        ...
