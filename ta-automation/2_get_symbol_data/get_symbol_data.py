import yfinance as yf
from datetime import datetime
import json

# todo: probably need Pydantic or something to validate parameters

# Function to add symbol data for the given symbol being queried
def lambda_handler(event, context):
    # get the history for the symbol
    symbol = event["Payload"]["symbol"]
    start = datetime.fromisoformat(event["Payload"]["date_from"])
    end = datetime.fromisoformat(event["Payload"]["date_to"])
    interval = event["Payload"]["resolution"]
    ticker = yf.Ticker(symbol)
    history = ticker.history(start=start, end=end, interval=interval)

    return json.loads(history.to_json())


payload = {
    "Payload": {
        "date_from": "2022-01-01T04:16:13+10:00",
        "date_to": "2022-01-04T04:16:13+10:00",
        "resolution": "1d",
        "notify_method": "pushover",
        "notify_recipient": "some-pushover-app-1",
        "target_ta_confidence": 7.5,
        "symbol": "bhp",
        "ta_algo": "awesome-oscillator",
    }
}

if __name__ == "__main__":
    lambda_handler(payload, None)
