from abc import ABC, abstractmethod


class IParameterStore(ABC):
    @abstractmethod
    def put_parameter(
        Name: str, Value: str, Type: str = "String", Overwrite: bool = True
    ) -> dict:
        ...

    def get_parameter(Name: str, WithDecryption: bool = False) -> dict:
        ...
