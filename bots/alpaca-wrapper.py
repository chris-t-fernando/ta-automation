from itradeapi import (
    ITradeAPI,
    IOrderResult,
    IAccount,
    IPosition,
    NotImplementedException,
)
from alpaca_trade_api import REST

# concrete implementation of trade api for alpaca
class AlpacaAPI(ITradeAPI):
    supported_crypto_symbols = {
        "SOLUSD",
        "SUSHIUSD",
        "DAIUSD",
        "SHIBUSD",
        "DOGEUSD",
        "MKRUSD",
        "MATICUSD",
    }

    def __init__(self, alpaca_key_id: str, alpaca_secret_key: str, environment="test"):
        # self.order_types = ORDER_TYPES
        self.default_currency = "USD"

        self.api = REST(
            key_id=alpaca_key_id,
            secret_key=alpaca_secret_key,
            base_url="https://paper-api.alpaca.markets",
        )

    def get_account(self):
        account = self.api.get_account()
        account.USD = account.cash
        del account.cash
        return account

    def list_positions(self):
        return self.api.list_positions()

    def get_bars(self, *args, **kwargs):
        # i think there's probably a better way of doing this
        if kwargs.get("symbol") != None:
            symbol = kwargs.get("symbol")
        else:
            symbol = args[0]

        if symbol in self.supported_crypto_symbols:
            return self.api.get_crypto_bars(*args, **kwargs)
        else:
            return self.api.get_bars(*args, **kwargs)

    def _translate_order_types(self, order_type):
        if order_type == "MARKET_BUY":
            return "buy"
        elif order_type == "MARKET_SELL":
            return "sell"
        else:
            raise NotImplementedException

    def order_create_by_value(self, *args, **kwargs):
        # todo - normalise this!
        if kwargs.get("order_type") != None:
            side = kwargs.get("order_type")
            del kwargs["order_type"]
            kwargs["side"] = side
        else:
            for arg in args:
                if arg == "buy" or arg == "sell":
                    arg = self._translate_order_types(arg)

            # side = args[3]

        return self.api.submit_order(*args, **kwargs)

    def order_create_by_units(self):
        ...

    def order_delete(self):
        ...

    def order_list(self):
        ...

    def close_position(self, *args, **kwargs):
        return self.api.close_position(self, *args, **kwargs)


if __name__ == "__main__":
    import boto3

    ssm = boto3.client("ssm")
    access_token = (
        ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )
    signing_token = (
        ssm.get_parameter(Name="/tabot/alpaca/security_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )
