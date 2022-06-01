from abc import ABC, abstractmethod


class IParameterStore(ABC):
    @abstractmethod
    def put(
        Name: str,
        Value: str,
        Type: str = "String",
        Overwrite: bool = True,
    ) -> dict:
        ...

    def get(Name: str, WithDecryption: bool = True) -> dict:
        ...
