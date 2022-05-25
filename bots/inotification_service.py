from abc import ABC, abstractmethod
from iparameter_store import IParameterStore


class INotificationService(ABC):
    @abstractmethod
    def __init__(self, store: IParameterStore, back_testing: bool):
        ...

    @abstractmethod
    def send(self, message: str, subject: str = None) -> bool:
        ...
