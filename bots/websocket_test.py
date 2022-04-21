import logging

log = logging.getLogger(__name__)
from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL
import boto3

ssm = boto3.client("ssm")
api_key = (
    ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=True)
    .get("Parameter")
    .get("Value")
)
api_secret = (
    ssm.get_parameter(Name="/tabot/alpaca/security_key", WithDecryption=True)
    .get("Parameter")
    .get("Value")
)
# base_url = "https://paper-api.alpaca.markets"
base_url = "https://data.sandbox.alpaca.markets/v2"
# data_url = "wss://data.alpaca.markets"
data_url = "wss://stream.data.sandbox.alpaca.markets/v2/iex"


async def print_quote(q):
    print("quote", q)


async def print_crypto_trade(t):
    print("crypto trade", t)


def main():
    logging.basicConfig(level=logging.INFO)
    feed = "IEX"
    stream = Stream(api_key, api_secret, data_feed=feed, raw_data=True)
    # stream.subscribe_trade_updates(print_trade_update)
    # stream.subscribe_trades(print_trade, "AAPL")
    stream.subscribe_quotes(print_quote, "IBM")
    stream.subscribe_crypto_trades(print_crypto_trade, "BTCUSD")

    @stream.on_bar("MSFT")
    async def _(bar):
        print("bar", bar)

    @stream.on_status("*")
    async def _(status):
        print("status", status)

    @stream.on_luld("AAPL", "MSFT")
    async def _(luld):
        print("LULD", luld)

    stream.run()


if __name__ == "__main__":
    main()
