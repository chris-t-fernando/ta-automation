# interface for a parameter store
class IParameterStore(ABC):
    @abstractmethod
    def __init__(self):
        ...

    @abstractmethod
    def get(self, path: str, decrypt: bool):
        ...

    @abstractmethod
    def put(self, path: str, value: str, encrypt: bool):
        ...
