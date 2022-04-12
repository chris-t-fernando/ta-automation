from datetime import datetime, timedelta
import math
import time
import boto3
from alpaca_trade_api.rest import REST, TimeFrame, APIError
from numpy import isnan, nan


class Bot:
    last_sma_fast = 1
    last_sma_slow = 1
    last_sma_pct = 1

    def __init__(self, symbol, sma_fast, sma_slow, qty_per_trade):
        self.symbol = symbol
        self.window_sma_fast = sma_fast
        self.window_sma_slow = sma_slow
        self.qty_per_trade = qty_per_trade

        self.sells = []
        self.buys = []

        # set up alpaca
        ssm = boto3.client("ssm")
        alpaca_key_id = (
            ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=False)
            .get("Parameter")
            .get("Value")
        )
        alpaca_secret_key = (
            ssm.get_parameter(Name="/tabot/alpaca/security_key", WithDecryption=False)
            .get("Parameter")
            .get("Value")
        )

        self.api = REST(
            key_id=alpaca_key_id,
            secret_key=alpaca_secret_key,
            base_url="https://paper-api.alpaca.markets",
        )

    def get_position(self):
        positions = self.api.list_positions()
        for p in positions:
            if p.symbol == self.symbol:
                return float(p.qty)
        return 0

    # Returns a series with the moving average
    def get_sma(self, series, periods):
        return series.rolling(periods).mean()

    # Checks whether we should buy (fast ma > slow ma)
    def get_signal(self, fast, slow):
        print(
            f"{self.symbol}: Fast {fast[-1]}, Slow: {slow[-1]}, Pct: {round((fast[-1]/slow[-1])*100,0)}%"
        )
        return fast[-1] > slow[-1]

    def get_bars(self, symbol):
        bars = self.api.get_crypto_bars(symbol, TimeFrame.Minute).df
        # for some reason sometimes there is no exchange header on these? no idea why
        if "exchange" in bars.columns:
            bars = bars[bars.exchange == bars.exchange.iloc[0]]
        else:
            # if this happens, just fall back to using whatever is returned i guess
            print(f"{self.symbol}: No Exchange info. Weird. {str(bars.columns)}")

        bars[f"sma_fast"] = self.get_sma(bars.close, self.window_sma_fast)
        bars[f"sma_slow"] = self.get_sma(bars.close, self.window_sma_slow)

        return bars

    def do_analysis(self):
        try:
            self.bars = self.get_bars(symbol=self.symbol)

            # sometimes the API bugs out. skip these runs
            if len(self.bars) > 0:
                # CHECK POSITIONS
                # position = self.get_position(symbol=self.symbol)
                # should_buy = self.get_signal(self.bars.sma_fast, self.bars.sma_slow)
                self.last_sma_fast = self.bars.sma_fast[-1]
                self.last_sma_slow = self.bars.sma_slow[-1]
                self.last_sma_pct = self.last_sma_fast / self.last_sma_slow

                # 1 means the handler will just hold
                if isnan(self.last_sma_pct):
                    self.last_sma_pct = 1

                return {
                    "sma_fast": self.last_sma_fast,
                    "sma_slow": self.last_sma_slow,
                    "sma_pct": self.last_sma_pct,
                }

            else:
                print(
                    f"{self.symbol}: API bugged o and returned zero rows.  Skipping this interval"
                )
                self.last_sma_fast = nan
                self.last_sma_slow = nan
                self.last_sma_pct = 1

        except Exception as e:
            print(
                f"{self.symbol}: Exception occurred, skipping this interval. Exception was {str(e)}"
            )
            self.last_sma_fast = nan
            self.last_sma_slow = nan
            self.last_sma_pct = 1

    def do_buy(self):
        try:
            self.api.submit_order(self.symbol, notional=self.qty_per_trade, side="buy")
            print(f"{self.symbol}: BUY / Quantity: {self.qty_per_trade}")
            # self.buys.append(self.bars.close.iloc[-1] * self.qty_per_trade)
            # for notionals we're specifying a dollar value instead of a unit volume
            self.buys.append(self.qty_per_trade)
        except APIError as e:
            if str(e) != "insufficient non-marginable buying power":
                print(f"{self.symbol}: BUY FAILED due to exception: {str(e)}")
        except Exception as e:
            print(f"{self.symbol}: BUY FAILED due to exception: {str(e)}")

    def do_sell(self):
        if self.get_position() > self.qty_per_trade:
            try:
                self.api.submit_order(
                    self.symbol, notional=self.qty_per_trade, side="sell"
                )
                print(f"Symbol: {self.symbol} SELL / Quantity: {self.qty_per_trade}")
                # self.sells.append(self.bars.close.iloc[-1] * self.qty_per_trade)
                # for notionals we're specifying a dollar value instead of a unit volume
                self.sells.append(self.qty_per_trade)
            except Exception as e:
                print(f"{self.symbol}: SELL FAILED due to exception: {str(e)}")
