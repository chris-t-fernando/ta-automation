import logging
import boto3
import parameter_stores
from alpaca_wrapper import AlpacaAPI
from swyftx_wrapper import SwyftxAPI
from back_test_wrapper import BackTestAPI
import sample_symbols
import yfinance as yf
import pandas as pd
import time
import json
from stock_symbol import (
    Symbol,
    NO_POSITION_TAKEN,
    BUY_LIMIT_ORDER_ACTIVE,
    BUY_PRICE_MET,
    POSITION_TAKEN,
    TAKING_PROFIT,
    STOP_LOSS_ACTIVE,
)
from utils import get_pause, get_interval_settings
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

# do reporting
# put reporting into S3 or something
# notify to Slack
# 300 df merge update bring down to just changes - faster faster
# better stop loss pct figures
# command line parameters

global_back_testing = True
global_override_broker = True

bot_telemetry = None

log_wp = logging.getLogger("macd")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("macd.log")
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

    def __init__(self):
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
        play_df = pd.DataFrame(columns=columns)

        # the broker api may fill an order automatically or it may queue it (market closed, price condition not met etc)
        # the state machine submits, and then gets the order details automatically so it might come back as filled immediately
        # then the state machine goes to the next step which also queries - so it can look like there are duplicate orders in here
        # for the purposes of generating our report, we can ignore duplicates
        unique_orders = self.orders_df.drop_duplicates(subset=["order_id"], keep="last")
        for play in plays:
            buy_value = unique_orders.loc[
                (unique_orders.order_type == 3) & (unique_orders.play_id == play)
            ].filled_total_value.item()
            sell_value = unique_orders.loc[
                (unique_orders.order_type != 3) & (unique_orders.play_id == play)
            ].filled_total_value.sum()
            profit = sell_value - buy_value

            if profit < 0:
                outcome = "loss"
            else:
                outcome = "win"

            symbol = unique_orders.loc[unique_orders.play_id == play].symbol.iloc[0]

            start = unique_orders.loc[
                (unique_orders.order_type == 3) & (unique_orders.play_id == play)
            ].create_time.min()
            end = unique_orders.loc[
                (unique_orders.order_type != 3) & (unique_orders.play_id == play)
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
            play_df = pd.concat([play_df, new_row], ignore_index=True)

            print(f"Play ID {play} made {profit} profit")

        self.play_df = play_df

        print("banana")

    # add in timestamps and use it for order by
    def _update_counters(self):
        ...

    def _update_streaks(self):
        ...

    def _update_peaks(self):
        ...

    def _convert_orders_to_df(self):
        ...


class MacdBot:
    jobs = None

    def __init__(
        self,
        ssm,
        market_data_source,
        bot_telemetry: BotTelemetry,
        interval="5m",
        back_testing=False,
        back_testing_balance: float = None,
    ):
        self.interval = interval
        self.interval_delta, max_range = get_interval_settings(self.interval)
        self.real_money_trading = False
        self.ssm = ssm
        self.market_data_source = market_data_source
        self.back_testing = back_testing
        self.back_testing_balance = back_testing_balance
        self.bot_telemetry = bot_telemetry

        # TODO take this as parameter input
        symbols = sample_symbols.everything

        if back_testing:
            # override broker to back_test
            if global_override_broker:
                for s in symbols:
                    s["api"] = "back_test"

        # get brokers and then set them up
        self.api_list = []
        for api in symbols:
            self.api_list.append(api["api"])
            log_wp.debug(f"Found broker {api}")

        # configure brokers
        self.api_list = list(set(self.api_list))
        self.api_dict = self.setup_brokers(
            api_list=self.api_list, ssm=ssm, back_testing=back_testing
        )

        # set up individual symbols
        self.symbols = {}
        for s in symbols:
            start_time = time.time()
            new_symbol = Symbol(
                symbol=s["symbol"],
                interval=self.interval,
                real_money_trading=self.real_money_trading,
                api=self.api_dict[s["api"]],
                bot_telemetry=self.bot_telemetry,
                store=ssm,
                market_data_source=market_data_source,
                back_testing=back_testing,
            )
            if new_symbol._init_complete:
                self.symbols[s["symbol"]] = new_symbol
                log_wp.info(
                    f'{s["symbol"]}: Set up complete in {round(time.time() - start_time,1)}s'
                )
            else:
                log_wp.error(
                    f'{s["symbol"]}: Failed to set up symbol - check spelling? YF returned {len(new_symbol.bars)} bars {round(time.time() - start_time,1)}s'
                )

    def setup_brokers(self, api_list, ssm, back_testing: bool = False):
        api_set = set(api_list)
        api_dict = {}

        for api in api_set:
            start_time = time.time()
            if api == "back_test":
                api_dict[api] = BackTestAPI(
                    back_testing=back_testing,
                    back_testing_balance=self.back_testing_balance,
                )
                break

            elif api == "swyftx":
                api_key = (
                    ssm.get_parameter(
                        Name="/tabot/swyftx/access_token", WithDecryption=True
                    )
                    .get("Parameter")
                    .get("Value")
                )
                api_dict[api] = SwyftxAPI(
                    api_key=api_key,
                    back_testing=back_testing,
                    back_testing_balance=self.back_testing_balance,
                )

            elif api == "alpaca":
                api_key = (
                    ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=True)
                    .get("Parameter")
                    .get("Value")
                )
                secret_key = (
                    ssm.get_parameter(
                        Name="/tabot/alpaca/security_key", WithDecryption=True
                    )
                    .get("Parameter")
                    .get("Value")
                )
                api_dict[api] = AlpacaAPI(
                    alpaca_key_id=api_key,
                    alpaca_secret_key=secret_key,
                    back_testing=back_testing,
                    back_testing_balance=self.back_testing_balance,
                )
            else:
                raise ValueError(f"Unknown broker specified {api}")

            log_wp.debug(f"Set up {api} in {round(time.time() - start_time,1)}s")

        return api_dict

    def get_date_range(self):
        start_date = None
        end_date = None
        latest_start = None

        if len(self.symbols) == 0:
            return None, None

        for s in self.symbols:
            # if this is the first symbol we're assessing
            if not start_date:
                start_date = self.symbols[s].bars.index.min()
                end_date = self.symbols[s].bars.index.max()
                latest_start = start_date
                latest_symbol = s
            else:
                if start_date > self.symbols[s].bars.index.min():
                    start_date = self.symbols[s].bars.index.min()

                if end_date < self.symbols[s].bars.index.max():
                    end_date = self.symbols[s].bars.index.max()

            # used when back testing to make sure we don't try sampling index -200
            # we're looking to see which symbol starts the LATEST
            # if latest_start > self.symbols[s].bars.index.min():
            if self.symbols[s].bars.index.min() > latest_start:
                latest_start = self.symbols[s].bars.index.min()
                latest_symbol = s

        if self.back_testing:
            latest_start_position = self.symbols[latest_symbol].bars.index.get_loc(
                latest_start
            )
            back_test_start_position = latest_start_position + 250

            start_date = self.symbols[latest_symbol].bars.index[
                back_test_start_position
            ]

        # TODO once you work out why we aren't analysing the most recent data, drop this to level 9
        log_wp.log(
            9,
            f"Range of all symbol bars: latest start date {latest_start} (for {latest_symbol}), latest end date {end_date}",
        )
        return start_date, end_date

    def process_bars(self):
        # update the data
        for s in self.symbols:
            log_wp.log(9, f"{s}: Updating bar data")
            self.symbols[s].update_bars()

        # find the oldest and newest records we're working with
        data_start_date, data_end_date = self.get_date_range()

        # define our starting point - if we're backtesting then go from the beginning of the data
        # if we're running live, then just process the most recent data
        if self.back_testing:
            current_record = data_start_date
        else:
            current_record = data_end_date

        # initialise state for each symbol - check which state each symbol is in, read open order numbers etc
        # self.set_state()

        # iterate through the data until we reach the end
        while current_record <= data_end_date:
            # log_wp.debug(f"Started processing {current_record}")
            for s in self.symbols:
                this_symbol = self.symbols[s]
                if (
                    this_symbol._analyse_date == None
                    or this_symbol._analyse_date < data_end_date
                ):
                    this_symbol.process(current_record)
                else:
                    log_wp.log(9, f"{s}: No new data")
            current_record = current_record + self.interval_delta

        # log_wp.debug(f"Finished processing all records")


def main():
    back_testing = global_back_testing
    interval = "5m"
    log_wp.debug(
        f"Starting up, poll time is {interval}, back testing is {back_testing}"
    )
    global bot_telemetry
    bot_telemetry = BotTelemetry()
    ssm = boto3.client("ssm")
    market_data_source = yf

    # reset back testing rules before we start the run
    if back_testing:
        store = parameter_stores.back_test_store()
        store.put_parameter(
            Name=f"/tabot/rules/backtest/5m",
            Value=json.dumps([]),
            Type="String",
            Overwrite=True,
        )
        api_key = (
            ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=True)
            .get("Parameter")
            .get("Value")
        )
        secret_key = (
            ssm.get_parameter(Name="/tabot/alpaca/security_key", WithDecryption=True)
            .get("Parameter")
            .get("Value")
        )

        store.put_parameter(
            Name=f"/tabot/alpaca/api_key",
            Value=api_key,
        )
        store.put_parameter(
            Name=f"/tabot/alpaca/security_key",
            Value=secret_key,
        )

    else:
        store = ssm

    bot_handler = MacdBot(
        ssm=store,
        market_data_source=market_data_source,
        bot_telemetry=bot_telemetry,
        interval=interval,
        back_testing=back_testing,
        back_testing_balance=100000,
    )

    if len(bot_handler.symbols) == 0:
        print(f"Nothing to do - no symbols to watch/symbols are invalid/no data")
        return

    if back_testing:
        bot_handler.process_bars()
    else:
        while True:
            bot_handler.process_bars()
            bot_handler.bot_telemetry.generate_df()
            start, end = bot_handler.get_date_range()
            pause = get_pause(interval)
            log_wp.debug(f"Finished analysing {end}, sleeping for {round(pause,0)}s")
            time.sleep(pause)

    bot_handler.bot_telemetry.play_df.to_csv("play_report.csv")
    bot_handler.bot_telemetry.orders_df.to_csv("order_report.csv")
    print("banana")


if __name__ == "__main__":
    main()

"""
    def set_state(self):
        stored_state = get_stored_state(ssm=self.ssm, back_testing=self.back_testing)

        for s in self.symbols:
            this_symbol = self.symbols[s]
            for this_state in stored_state:
                # if there is an order for this symbol
                if s == this_state["symbol"]:
                    # update the symbol with the order - the logic is in symbol to work out what the state means
                    broker_name = this_symbol.api.get_broker_name()
                    if broker_name == this_state["broker"]:
                        this_symbol.set_stored_state(stored_state=this_state)
                        log_wp.debug(
                            f"{s}: Found state for {s} using broker {broker_name}"
                        )
                    else:
                        # matched symbol but not broker
                        log_wp.warning(
                            f'{s}: Found state for {s} but brokers did not match (state {this_state["broker"]} vs config {broker_name})'
                        )


"""
