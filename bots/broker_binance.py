from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import boto3

if __name__ == "__main__":
    ssm = boto3.client("ssm")
    api_key = (
        ssm.get_parameter(Name="/tabot/paper/binance/api_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    secret_key = (
        ssm.get_parameter(Name="/tabot/paper/binance/secret_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    client = Client(api_key, secret_key)

    # get market depth
    depth = client.get_order_book(symbol='BNBBTC')

    # get all symbol prices
    prices = client.get_all_tickers()
    print("banana")