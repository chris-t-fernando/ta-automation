from parameter_store import IParameterStore

class Ssm(IParameterStore):
    def __init__(self, store):
        self.store = store

    def get(self, path: str, decrypt: bool = False):
        return (
            self.store.get_parameter(Name=path, WithDecryption=decrypt)
            .get("Parameter")
            .get("Value")
        )

    def put(self, path: str, value: str, encrypt: bool = False):
        ...
