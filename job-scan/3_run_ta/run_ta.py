import pandas as pd
import json
from ta.momentum import AwesomeOscillatorIndicator
from ta.momentum import StochasticOscillator
from ta.volume import AccDistIndexIndicator


def lambda_handler(event, context):
    # don't ask me why i need to stringify the json for read_json to be able to read it. whatever trevor.
    df_json = json.dumps(event["Payload"]["symbol_data"])
    df = pd.read_json(df_json)

    selected_algo = event["Payload"]["ta_algo"]
    if selected_algo == "awesome-oscillator":
        # i wish the documentation for this library was even semi existent. it would make life better.
        df["awesome_oscillator"] = AwesomeOscillatorIndicator(
            high=df["High"], low=df["Low"], window1=5, window2=34, fillna=True
        ).awesome_oscillator()

    elif selected_algo == "stoch":
        df["stoch"] = StochasticOscillator(
            close=df["Close"],
            high=df["High"],
            low=df["Low"],
            fillna=True,
        ).stoch()
        df["stoch_signal"] = StochasticOscillator(
            close=df["Close"],
            high=df["High"],
            low=df["Low"],
            fillna=True,
        ).stoch_signal()

    elif selected_algo == "accumulation-distribution":
        df["accumulation-distribution"] = AccDistIndexIndicator(
            close=df["Close"],
            high=df["High"],
            low=df["Low"],
            volume=df["Volume"],
            fillna=True,
        ).acc_dist_index()

    # not implemented in this library. i'll calculate it later maybe
    # elif selected_algo == "accelerator-oscillator":

    return {"ta_confidence": 6.0}


payload = {
    "Payload": {
        "date_from": "2022-01-01T04:16:13+10:00",
        "date_to": "2022-02-14T04:16:13+10:00",
        "resolution": "1d",
        "notify_method": "pushover",
        "notify_recipient": "some-pushover-app-1",
        "target_ta_confidence": 7.5,
        "symbol": "bhp",
        "ta_algo": "accumulation-distribution",
        "symbol_data": {
            "Open": {
                "1641168000000": 57.9464594475,
                "1641254400000": 57.8890743756,
            },
            "High": {
                "1641168000000": 58.1281684373,
                "1641254400000": 58.9506456491,
            },
            "Low": {
                "1641168000000": 57.5160922104,
                "1641254400000": 57.8030009305,
            },
            "Close": {
                "1641168000000": 57.7073669434,
                "1641254400000": 58.6446075439,
            },
            "Volume": {
                "1641168000000": 1584400,
                "1641254400000": 3321300,
            },
            "Dividends": {
                "1641168000000": 0,
                "1641254400000": 0,
            },
            "Stock Splits": {
                "1641168000000": 0,
                "1641254400000": 0,
            },
        },
    }
}

if __name__ == "__main__":
    lambda_handler(payload, None)
