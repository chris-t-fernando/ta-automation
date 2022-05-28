from iparameter_store import IParameterStore
import boto3
import sys
import json


class ssm(IParameterStore):
    def __init__(self):
        self.store = boto3.client("ssm")

    def put_parameter(self, *args, **kwargs) -> dict:
        try:
            return self.store.put_parameter(*args, **kwargs)
        except Exception as e:
            raise

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
        new_path = Name not in self.store
        overwrite_path = Name in self.store and Overwrite == True
        if new_path or overwrite_path:
            self.store[Name] = {"Parameter": {"Value": Value}}
            if len(json.dumps(self.store[Name])) > 4096:
                raise Exception("Length of dict exceeds 4096 characters")
            return
        if Name in self.store and Overwrite == False:
            raise ValueError

    def get_parameter(self, Name: str, WithDecryption: bool = False) -> dict:
        if Name not in self.store:
            raise self.exceptions.ParameterNotFound(
                f"{Name} not found in Parameter Store"
            )

        return self.store[Name]
