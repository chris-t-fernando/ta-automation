import logging
import boto3
import pandas as pd

from itradeapi import (
    MARKET_BUY,
    MARKET_SELL,
    LIMIT_BUY,
    LIMIT_SELL,
    STOP_LIMIT_BUY,
    STOP_LIMIT_SELL,
)

log_wp = logging.getLogger(
    "bot_telemetry"
)  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("bot_telemetry.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)


class BotTelemetry:
    columns = [
        "symbol",
        "order_id",
        "play_id",
        "status",
        "status_summary",
        "status_text",
        "ordered_unit_quantity",
        "ordered_unit_price",
        "ordered_total_value",
        "filled_unit_quantity",
        "filled_unit_price",
        "filled_total_value",
        "fees",
        "success",
    ]

    cycle_columns = [
        "symbol",
        "Open",
        "High",
        "Low",
        "Close",
        "macd_macd",
        "macd_signal",
        "macd_histogram",
        "macd_crossover",
        "macd_signal_crossover",
        "macd_above_signal",
        "macd_cycle",
        "sma_200",
        "recent_average_sma",
        "outcome",
        "outcome_reason",
    ]

    def __init__(self, back_testing: bool):
        self.back_testing = back_testing
        self.orders = []
        self.win_count = 0
        self.loss_count = 0
        self.breakeven_count = 0
        self.win_streak = 0
        self.lose_streak = 0
        self.breakeven_streak = 0
        self.current_win_streak = 0
        self.current_loss_streak = 0
        self.current_breakeven = 0
        self.peak_orders = 0
        self.peak_capital_balance = 0
        self.concurrent_orders = 0

    def add_order(self, order_result, play_id):
        # TODO - this is a dumb error specific to back testing that I don't care enough about to fix
        # sometimes orders fail and return a bool. not interested in these guys
        if type(order_result) == bool:
            return

        order_result.play_id = play_id
        self.orders.append(order_result)
        self._update_counters()
        self._update_streaks()
        self._update_peaks()

        # if order_result.order_type == MARKET_SELL:

    def generate_df(self):
        self.orders_df = pd.DataFrame([x.as_dict() for x in self.orders])
        if len(self.orders_df) == 0:
            return

        plays = self.orders_df.play_id.unique()
        columns = [
            "play_id",
            "symbol",
            "buy_value",
            "sell_value",
            "profit",
            "outcome",
            "take_profit_count",
            "start",
            "end",
            "duration",
        ]
        # set up the destination dataframe
        plays_df = pd.DataFrame(columns=columns)

        # the broker api may fill an order automatically or it may queue it (market closed, price condition not met etc)
        # the state machine submits, and then gets the order details automatically so it might come back as filled immediately
        # then the state machine goes to the next step which also queries - so it can look like there are duplicate orders in here
        # for the purposes of generating our report, we can ignore duplicates
        unique_orders = self.orders_df.drop_duplicates(subset=["order_id"], keep="last")
        for play in plays:
            # check if the buy got filled - if not, the play never really started and we can ignore it
            buy_order_status = unique_orders.loc[
                ((unique_orders.order_type == 3) | (unique_orders.order_type == 1)) & (unique_orders.play_id == play)
            ].status_summary.item()
            if buy_order_status != "filled":
                # hacky way to skip without indents
                continue

            buy_value = unique_orders.loc[
                ((unique_orders.order_type == 3) | (unique_orders.order_type == 1)) & (unique_orders.play_id == play)
            ].filled_total_value.item()
            sell_value = unique_orders.loc[
                (unique_orders.order_type != 3) & (unique_orders.order_type != 1) & (unique_orders.play_id == play)
            ].filled_total_value.sum()

            profit = sell_value - buy_value

            if profit < 0:
                outcome = "loss"
            else:
                outcome = "win"

            symbol = unique_orders.loc[unique_orders.play_id == play].symbol.iloc[0]

            start = unique_orders.loc[
                ((unique_orders.order_type == 3) | ((unique_orders.order_type == 1))) & (unique_orders.play_id == play)
            ].create_time.min()
            end = unique_orders.loc[
                (unique_orders.order_type != 3) & (unique_orders.order_type != 1) & (unique_orders.play_id == play)
            ].update_time.max()

            take_profit_count = len(
                unique_orders.loc[
                    (unique_orders.order_type == 4)
                    & (unique_orders.play_id == play)
                    & (unique_orders.status_summary == "filled")
                ]
            )

            duration = end - start

            new_row = pd.DataFrame(
                {
                    "play_id": play,
                    "symbol": symbol,
                    "buy_value": buy_value,
                    "sell_value": sell_value,
                    "profit": profit,
                    "outcome": outcome,
                    "take_profit_count": take_profit_count,
                    "start": start,
                    "end": end,
                    "duration": duration,
                },
                columns=columns,
                index=[0],
            )
            plays_df = pd.concat([plays_df, new_row], ignore_index=True)

            # print(f"Play ID {play} made {profit} profit")

        # add concurrent play count
        ends = plays_df.start.values < plays_df.end.values[:, None]
        starts = plays_df.start.values > plays_df.start.values[:, None]
        plays_df["concurrent_plays"] = (ends & starts).sum(0)

        self.plays_df = plays_df

        self.symbols_df = pd.DataFrame()
        self.symbols_df["profit"] = self.plays_df.groupby(["symbol"]).profit.sum()
        self.symbols_df["plays"] = self.plays_df.groupby(["symbol"]).profit.count()

    # add in timestamps and use it for order by
    def _update_counters(self):
        ...

    def _update_streaks(self):
        ...

    def _update_peaks(self):
        ...

    def _convert_orders_to_df(self):
        ...

    def next_cycle(self, timestamp):
        # set up the destination dataframe
        self.cycle_df = pd.DataFrame(columns=BotTelemetry.cycle_columns)
        self.cycle_timestamp = timestamp

    def add_cycle_data(self, row):
        # don't bother writing this stuff if we're backtesting
        if self.back_testing:
            return

        new_row = pd.DataFrame(
            {
                "symbol": row["symbol"],
                "Open": row["Open"],
                "High": row["High"],
                "Low": row["Low"],
                "Close": row["Close"],
                "macd_macd": row["macd_macd"],
                "macd_signal": row["macd_signal"],
                "macd_histogram": row["macd_histogram"],
                "macd_crossover": row["macd_crossover"],
                "macd_signal_crossover": row["macd_signal_crossover"],
                "macd_above_signal": row["macd_above_signal"],
                "macd_cycle": row["macd_cycle"],
                "sma_200": row["sma_200"],
                "recent_average_sma": row["recent_average_sma"],
                "outcome": row["outcome"],
                "outcome_reason": row["outcome_reason"],
            },
            columns=BotTelemetry.cycle_columns,
            index=[0],
        )
        self.cycle_df = pd.concat([self.cycle_df, new_row], ignore_index=True)

    def save_cycle(self):
        if len(self.cycle_df) > 0:
            dyn = boto3.resource("dynamodb")
            table = dyn.Table("bot_telemetry_macd_cycles")
            cycle_json = self.cycle_df.to_json()
            # formatted_cycle_json = json.dumps(cycle_json, indent=4, sort_keys=True)
            table.put_item(
                Item={
                    "cycle_date": str(self.cycle_timestamp),
                    "signal_data": cycle_json,
                }
            )
        return True
