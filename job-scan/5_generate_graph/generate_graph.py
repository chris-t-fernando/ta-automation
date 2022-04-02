# import json
import plotly.figure_factory

# fig = plotly.figure_factory.create_candlestick(df["Open"], df["High"], df["Low"], df["Close"], dates=df.axes[0])
# fig.show()


def lambda_handler(event, context):

    return {"ta_confidence": 6.0}


def do_graph(df, event):
    import matplotlib.pyplot as plt

    ax1 = plt.subplot2grid((10, 1), (0, 0), rowspan=5, colspan=1)
    ax2 = plt.subplot2grid((10, 1), (6, 0), rowspan=4, colspan=1)
    ax1.plot(df["Close"], label=event["Payload"]["symbol"], color="skyblue")
    ax1.plot(
        df.index,
        buy_price,
        marker="^",
        markersize=12,
        color="#26a69a",
        linewidth=0,
        label="BUY SIGNAL",
    )
    ax1.plot(
        df.index,
        sell_price,
        marker="v",
        markersize=12,
        color="#f44336",
        linewidth=0,
        label="SELL SIGNAL",
    )
    ax1.legend()
    ax1.set_title(f'{event["Payload"]["symbol"]} CLOSING PRICE')
    for i in range(len(df)):
        if df["awesome-oscillator"][i - 1] > df["awesome-oscillator"][i]:
            ax2.bar(df.index[i], df["awesome-oscillator"][i], color="#f44336")
        else:
            ax2.bar(df.index[i], df["awesome-oscillator"][i], color="#26a69a")
    ax2.set_title(f'{event["Payload"]["symbol"]} AWESOME OSCILLATOR 5,34')
    plt.show()


payload = {"Payload": None}

if __name__ == "__main__":
    lambda_handler(payload, None)
