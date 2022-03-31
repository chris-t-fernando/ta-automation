# import json
import plotly.figure_factory

# fig = plotly.figure_factory.create_candlestick(df["Open"], df["High"], df["Low"], df["Close"], dates=df.axes[0])
# fig.show()


def lambda_handler(event, context):

    return {"ta_confidence": 6.0}


payload = {"Payload": None}

if __name__ == "__main__":
    lambda_handler(payload, None)
