#git clone https://github.com/resy/coinbase_python3.git
import coinbase

if __name__ == "__main__":
    import boto3

    ssm = boto3.client("ssm")
    api_key = (
        ssm.get_parameter(Name="/tabot/paper/coinbase/api_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    api_secret = (
        ssm.get_parameter(Name="/tabot/paper/coinbase/api_secret", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    api = coinbase.Coinbase.with_api_key(api_key, api_secret)
    balance = api.get_balance()
    print('Balance is ' + balance + ' BTC')
