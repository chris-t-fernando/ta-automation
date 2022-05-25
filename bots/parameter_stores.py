from iparameter_store import IParameterStore
import boto3


class ssm(IParameterStore):
    def __init__(self):
        self.store = boto3.client("ssm")

    def put_parameter(self, *args, **kwargs) -> dict:
        return self.store.put_parameter(*args, **kwargs)

    def get_parameter(self, *args, **kwargs) -> dict:
        return self.store.get_parameter(*args, **kwargs)


class back_test_store(IParameterStore):
    class exceptions:
        class ParameterNotFound(Exception):
            ...

    def __init__(self, store=None):
        self.store = {}

    def put_parameter(
        self, Name: str, Value: str, Type: str = "String", Overwrite: bool = True
    ) -> dict:
        if Name not in self.store:
            self.store[Name] = {"Parameter": {"Value": Value}}
            return
        if Name in self.store and Overwrite == True:
            self.store[Name] = {"Parameter": {"Value": Value}}
            return
        if Name in self.store and Overwrite == False:
            raise ValueError

    def get_parameter(self, Name: str, WithDecryption: bool = False) -> dict:
        if Name not in self.store:
            raise self.exceptions.ParameterNotFound(
                f"{Name} not found in Parameter Store"
            )

        return self.store[Name]
