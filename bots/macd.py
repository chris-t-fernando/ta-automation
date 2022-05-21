import logging
import boto3
import parameter_stores
from alpaca_wrapper import AlpacaAPI
from swyftx_wrapper import SwyftxAPI
from back_test_wrapper import BackTestAPI, Position
from itradeapi import ITradeAPI, IPosition
import yfinance as yf
import pandas as pd
import time
import json
from datetime import datetime
from stock_symbol import (
    Symbol,
    NO_POSITION_TAKEN,
    BUY_LIMIT_ORDER_ACTIVE,
    BUY_PRICE_MET,
    POSITION_TAKEN,
    TAKING_PROFIT,
    STOP_LOSS_ACTIVE,
)
from buyplan import BuyPlan
from utils import (
    get_pause,
    check_buy_signal,
    validate_rules,
    get_rules,
    put_rules,
    merge_rules,
    trigger_stop_loss,
    trigger_sell_point,
    trigger_risk_point,
    get_interval_settings,
    get_stored_state,
)
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

# do reporting
# 200 df merge update bring down to just changes - faster faster
# check if market is closed and don't query
# why are weird dataframe end dates occurring?!
# better stop loss pct figures

global_back_testing = False
global_override_broker = False

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

df_report = pd.DataFrame()
df_report_columns = [
    "start",
    "end",
    "capital_start",
    "capital_end",
    "capital_change",
    "capital_change_pct",
    "intervals",
    "trades_total",
    "trades_won",
    "trades_won_rate",
    "trades_lost",
    "trades_lost_rate",
    "trades_skipped",
    "hold_units",
    "hold_start_buy",
    "hold_end_buy",
    "hold_change",
    "hold_change_pct",
    "better_strategy",
]


df_trade_report_columns = [
    "date",
    "symbol",
    "side",
    "order_type",
    "units",
    "unit_price",
    "total_value",
]
df_trade_report = pd.DataFrame(columns=df_trade_report_columns)


class BotReport:
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
        order_result.play_id = play_id
        self.orders.append(order_result)
        self._update_counters()
        self._update_streaks()
        self._update_peaks()

    def generate_df(self):
        symbol = []
        order_id = []
        play_id = []
        status = []
        status_summary = []
        status_text = []
        ordered_unit_quantity = []
        ordered_unit_price = []
        ordered_total_value = []
        filled_unit_quantity = []
        filled_unit_price = []
        filled_total_value = []
        fees = []
        success = []

        for order in self.orders:
            symbol.append(order.symbol)
            order_id.append(order.order_id)
            play_id.append(order.play_id)
            status.append(order.status)
            status_summary.append(order.status_summary)
            status_text.append(order.status_text)
            ordered_unit_quantity.append(order.ordered_unit_quantity)
            ordered_unit_price.append(order.ordered_unit_price)
            ordered_total_value.append(order.ordered_total_value)
            filled_unit_quantity.append(order.filled_unit_quantity)
            filled_unit_price.append(order.filled_unit_price)
            filled_total_value.append(order.filled_total_value)
            fees.append(order.fees)
            success.append(order.success)

        df_dict = {
            "symbol": symbol,
            "order_id": order_id,
            "play_id": play_id,
            "status": status,
            "status_summary": status_summary,
            "status_text": status_text,
            "ordered_unit_quantity": ordered_unit_quantity,
            "ordered_unit_price": ordered_unit_price,
            "ordered_total_value": ordered_total_value,
            "filled_unit_quantity": filled_unit_quantity,
            "filled_unit_price": filled_unit_price,
            "filled_total_value": filled_total_value,
            "fees": fees,
            "success": success,
        }

        self.orders_df = pd.DataFrame(df_dict)

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
        data_source,
        interval="5m",
        back_testing=False,
        back_testing_balance: float = None,
    ):
        self.interval = interval
        self.real_money_trading = False
        self.ssm = ssm
        self.data_source = data_source
        self.back_testing = back_testing
        self.back_testing_balance = back_testing_balance
        self.bot_report = BotReport()

        # get jobs
        mixed_symbols = [
            {"symbol": "AAPL", "api": "alpaca"},
            {"symbol": "AXS", "api": "alpaca"},
            {"symbol": "TSLA", "api": "alpaca"},
            {"symbol": "FB", "api": "alpaca"},
            {"symbol": "GOOG", "api": "alpaca"},
            {"symbol": "MSFT", "api": "alpaca"},
            {"symbol": "NVDA", "api": "alpaca"},
            {"symbol": "NVAX", "api": "alpaca"},
            {"symbol": "BUD", "api": "alpaca"},
            {"symbol": "AMZN", "api": "alpaca"},
            {"symbol": "INFY", "api": "alpaca"},
            {"symbol": "RTX", "api": "alpaca"},
            {"symbol": "ADA-USD", "api": "alpaca"},
            {"symbol": "ETH-USD", "api": "alpaca"},
            {"symbol": "SOL-USD", "api": "alpaca"},
            {"symbol": "XRP-USD", "api": "alpaca"},
            {"symbol": "DOGE-USD", "api": "alpaca"},
            {"symbol": "SHIB-USD", "api": "alpaca"},
            {"symbol": "MATIC-USD", "api": "alpaca"},
            {"symbol": "ATOM-USD", "api": "alpaca"},
            {"symbol": "FTT-USD", "api": "alpaca"},
            {"symbol": "BNB-USD", "api": "alpaca"},
        ]

        nyse_symbols_big = [
            {"symbol": "C", "api": "alpaca"},
            {"symbol": "PFE", "api": "alpaca"},
            {"symbol": "GE", "api": "alpaca"},
            {"symbol": "AIG", "api": "alpaca"},
            {"symbol": "WMT", "api": "alpaca"},
            {"symbol": "IBM", "api": "alpaca"},
            {"symbol": "BAC", "api": "alpaca"},
            {"symbol": "JNJ", "api": "alpaca"},
            {"symbol": "GS", "api": "alpaca"},
            {"symbol": "CVX", "api": "alpaca"},
            {"symbol": "PG", "api": "alpaca"},
            {"symbol": "MO", "api": "alpaca"},
            {"symbol": "JPM", "api": "alpaca"},
            {"symbol": "COP", "api": "alpaca"},
            {"symbol": "VLO", "api": "alpaca"},
            {"symbol": "TXN", "api": "alpaca"},
            {"symbol": "SLB", "api": "alpaca"},
            {"symbol": "HD", "api": "alpaca"},
            {"symbol": "UNH", "api": "alpaca"},
            {"symbol": "MRK", "api": "alpaca"},
            {"symbol": "VZ", "api": "alpaca"},
            {"symbol": "CAT", "api": "alpaca"},
            {"symbol": "PD", "api": "alpaca"},
            {"symbol": "DNA", "api": "alpaca"},
            {"symbol": "GM", "api": "alpaca"},
            {"symbol": "HPQ", "api": "alpaca"},
            {"symbol": "KO", "api": "alpaca"},
            {"symbol": "AXP", "api": "alpaca"},
            {"symbol": "UPS", "api": "alpaca"},
            {"symbol": "MMM", "api": "alpaca"},
            {"symbol": "VIA", "api": "alpaca"},
            {"symbol": "WFC", "api": "alpaca"},
            {"symbol": "HAL", "api": "alpaca"},
            {"symbol": "BA", "api": "alpaca"},
            {"symbol": "F", "api": "alpaca"},
            {"symbol": "X", "api": "alpaca"},
            {"symbol": "LLY", "api": "alpaca"},
            {"symbol": "RIG", "api": "alpaca"},
            {"symbol": "AAPL", "api": "alpaca"},
            {"symbol": "AXS", "api": "alpaca"},
            {"symbol": "TSLA", "api": "alpaca"},
            {"symbol": "FB", "api": "alpaca"},
            {"symbol": "GOOG", "api": "alpaca"},
            {"symbol": "MSFT", "api": "alpaca"},
            {"symbol": "NVDA", "api": "alpaca"},
            {"symbol": "NVAX", "api": "alpaca"},
            {"symbol": "BUD", "api": "alpaca"},
            {"symbol": "AMZN", "api": "alpaca"},
            {"symbol": "INFY", "api": "alpaca"},
            {"symbol": "RTX", "api": "alpaca"},
            {"symbol": "GME", "api": "alpaca"},
        ]

        nyse_symbols_medium = [
            {"symbol": "C", "api": "alpaca"},
            {"symbol": "PFE", "api": "alpaca"},
            {"symbol": "GE", "api": "alpaca"},
            {"symbol": "AIG", "api": "alpaca"},
            {"symbol": "WMT", "api": "alpaca"},
            {"symbol": "IBM", "api": "alpaca"},
            {"symbol": "BAC", "api": "alpaca"},
            {"symbol": "JNJ", "api": "alpaca"},
            {"symbol": "GS", "api": "alpaca"},
            {"symbol": "CVX", "api": "alpaca"},
            {"symbol": "PG", "api": "alpaca"},
            {"symbol": "MO", "api": "alpaca"},
            {"symbol": "JPM", "api": "alpaca"},
            {"symbol": "COP", "api": "alpaca"},
            {"symbol": "VLO", "api": "alpaca"},
            {"symbol": "TXN", "api": "alpaca"},
            {"symbol": "GME", "api": "alpaca"},
        ]

        nyse_symbols = [
            {"symbol": "MSFT", "api": "alpaca"},
            {"symbol": "C", "api": "alpaca"},
        ]

        mixed_symbols_small = [
            {"symbol": "C", "api": "alpaca"},
            {"symbol": "SOL-USD", "api": "alpaca"},
        ]

        symbols = mixed_symbols_small

        df_report = pd.DataFrame(
            columns=df_report_columns, index=[x["symbol"] for x in symbols]
        )

        if back_testing:
            if global_override_broker:
                for s in symbols:
                    s["api"] = "back_test"

        # get brokers and then set them up
        self.api_list = []
        for api in symbols:
            self.api_list.append(api["api"])
            log_wp.debug(f"Found broker {api}")
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
                bot_report=self.bot_report,
                store=ssm,
                data_source=data_source,
                back_testing=back_testing,
            )
            if new_symbol._init_complete:
                self.symbols[s["symbol"]] = new_symbol
                log_wp.info(
                    f'{s["symbol"]}: Set up complete in {round(time.time() - start_time,1)}s'
                )
            else:
                log_wp.error(
                    f'{s["symbol"]}: Failed to set up symbol - check spelling? YF returned {len(new_symbol.bars)} {round(time.time() - start_time,1)}s'
                )

    def setup_brokers(self, api_list, ssm, back_testing: bool = False):
        api_set = set(api_list)
        api_dict = {}

        for api in api_set:
            start_time = time.time()
            if api == "back_test":
                api_dict[api] = BackTestAPI(back_testing=back_testing)
                break

            elif api == "swyftx":
                api_key = (
                    ssm.get_parameter(
                        Name="/tabot/swyftx/access_token", WithDecryption=True
                    )
                    .get("Parameter")
                    .get("Value")
                )
                api_dict[api] = SwyftxAPI(api_key=api_key, back_testing=back_testing)

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

        log_wp.debug(
            f"Range of all symbol bars: latest start date {latest_start} (for {latest_symbol}), latest end date {end_date}"
        )
        return start_date, end_date

    def new_start(self):
        # update the data
        for s in self.symbols:
            log_wp.debug(f"{s}: Updating bar data")
            self.symbols[s].update_bars()

        # find the oldest and newest records we're working with
        data_start_date, data_end_date = self.get_date_range()

        # get interval delta - used at the end of each iteration to work out what to do next
        interval_delta, max_range = get_interval_settings(self.interval)

        # define our starting point
        if self.back_testing:
            current_record = data_start_date
        else:
            current_record = data_end_date

        # initialise state for each symbol - check which state each symbol is in, read open order numbers etc
        # self.set_state()

        # iterate through the data until we reach the end
        while current_record <= data_end_date:
            log_wp.debug(f"Started processing {current_record}")
            for s in self.symbols:
                this_symbol = self.symbols[s]
                if (
                    this_symbol._analyse_date == None
                    or this_symbol._analyse_date < data_end_date
                ):
                    this_symbol.process(current_record)
                else:
                    print(f"{s}: No new data")
            current_record = current_record + interval_delta

        log_wp.debug(f"Finished processing all records")


def main():
    back_testing = global_back_testing
    interval = "1m"
    log_wp.debug(
        f"Starting up, poll time is {interval}, back testing is {back_testing}"
    )
    ssm = boto3.client("ssm")
    data_source = yf

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
        data_source=data_source,
        interval=interval,
        back_testing=back_testing,
        back_testing_balance=10000,
    )

    if len(bot_handler.symbols) == 0:
        print(f"Nothing to do - no symbols to watch/symbols are invalid/no data")
        return

    if back_testing:
        bot_handler.new_start()
    else:
        while True:
            bot_handler.new_start()
            pause = get_pause(interval)
            log_wp.debug(f"Sleeping for {round(pause,0)}s")
            time.sleep(pause)

    bot_handler.bot_report.generate_df()

    global df_trade_report
    df_trade_report.to_csv("trade_report.csv")
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
