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

global_back_testing = True
global_override_broker = True

log_wp = logging.getLogger("macd")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("macd.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.INFO)

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
    def __init__(self, starting_balance: float):
        self.starting_balance = starting_balance
        self.orders = {}
        self.win_count = 0
        self.loss_count = 0
        self.breakeven_count = 0
        self.win_streak = 0
        self.lose_streak = 0
        self.breakeven_streak = 0
        self.peak_orders = 0
        self.peak_capital_balance = 0

    def add_order(self, order_result):
        self.orders[order_result.symbol] = order_result
        self._update_counters()
        self._update_streaks()
        self._update_peaks()

    def _update_counters(self):
        ...

    def _update_streaks(self):
        ...

    def _update_peaks(self):
        ...


class MacdBot:
    jobs = None

    def __init__(
        self, ssm, data_source, back_testing=False, starting_balance: float = None
    ):
        self.interval = "5m"
        self.real_money_trading = False
        self.ssm = ssm
        self.data_source = data_source
        self.back_testing = back_testing

        # get jobs
        symbols = [
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
            {"symbol": "BTC-USD", "api": "alpaca"},
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

        symbols = [
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
        ]

        df_report = pd.DataFrame(
            columns=df_report_columns, index=[x["symbol"] for x in symbols]
        )

        if back_testing:
            self.starting_balance = starting_balance
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
                store=ssm,
                data_source=data_source,
                back_testing=back_testing,
            )
            if new_symbol._init_complete:
                self.symbols[s["symbol"]] = new_symbol
                log_wp.debug(
                    f'{s["symbol"]}: Set up complete in {round(time.time() - start_time,1)}s'
                )
            else:
                log_wp.debug(
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
                    starting_balance=self.starting_balance,
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

                log_wp.debug(
                    f"{s}: Setting start date to {start_date}, end date to {end_date}"
                )
                log_wp.debug(f"{s}: Latest start is {latest_start}")
            else:
                if start_date > self.symbols[s].bars.index.min():
                    log_wp.debug(
                        f"{s}: Changing start date. Was {start_date} for {latest_symbol}, now {self.symbols[s].bars.index.min()}"
                    )
                    start_date = self.symbols[s].bars.index.min()

                if end_date < self.symbols[s].bars.index.max():
                    log_wp.debug(
                        f"{s}: Changing end date. Was {end_date}, now {self.symbols[s].bars.index.max()}"
                    )
                    end_date = self.symbols[s].bars.index.max()

            # used when back testing to make sure we don't try sampling index -200
            # we're looking to see which symbol starts the LATEST
            # if latest_start > self.symbols[s].bars.index.min():
            if self.symbols[s].bars.index.min() > latest_start:
                log_wp.debug(
                    f"{s}: Latest start changed. Was {latest_start}, now {self.symbols[s].bars.index.min()}"
                )
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

        return start_date, end_date

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

    def new_start(self):
        # first find the oldest and newest records we're working with
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
                this_symbol.process(current_record)
            current_record = current_record + interval_delta

        log_wp.debug(f"Finished processing all records")

    def start(self):
        rules = get_rules(ssm=self.ssm, back_testing=self.back_testing)
        validate_rules(rules)
        positions = self.list_positions()
        back_testing_unit_price = None

        while True:
            for s in self.symbols:
                this_symbol = self.symbols[s]
                # get new data
                if self.back_testing:
                    # if we're backtesting, start at the very first record that includes SMA200 plus some buffer to work out direction of market
                    current_record_index = 250

                else:
                    # if we are not backtesting, get the most recent record
                    current_record_index = this_symbol.bars.index.get_loc(
                        this_symbol.bars.index[-1]
                    )

                records_to_process = len(this_symbol.bars.iloc[current_record_index:])

                # check we aren't doubling up (only really relevant for backtrading)
                if this_symbol.bars.index[-1] == this_symbol.last_date_processed:
                    log_wp.debug(
                        f"{s}: No new records to process. Last record was {this_symbol.bars.index[-1]} (back_test={self.back_testing})"
                    )
                    continue

                # while there is data to be processed
                while current_record_index <= this_symbol.bars.index.get_loc(
                    this_symbol.bars.index[-1]
                ):
                    # process the records
                    # get the current record
                    current_record = this_symbol.bars.index[current_record_index]
                    log_wp.debug(
                        f"{s}: Processing {current_record} (back_test={self.back_testing})"
                    )

                    position = this_symbol.api.get_position(symbol=s)

                    # if we are backtesting, we need to read into the future to get the market price we'll be able to sell at
                    if self.back_testing:
                        # get next record
                        try:
                            next_record = this_symbol.bars.index[
                                current_record_index + 1
                            ]
                            back_testing_unit_price = this_symbol.bars.Open.loc[
                                next_record
                            ]
                        except IndexError as e:
                            back_testing_unit_price = None

                    rules = apply_sell_rules(
                        ssm=self.ssm,
                        api=this_symbol.api,
                        rules=rules,
                        position=position,
                        last_close=this_symbol.bars.Close.loc[current_record],
                        symbol=s,
                        period=current_record,
                        back_testing=self.back_testing,
                        back_testing_unit_price=back_testing_unit_price,
                    )

                    # BUY
                    if position.quantity == 0:
                        # check if we have a buy signal
                        buffer = current_record_index - 200
                        # need the +1 otherwise it does not include the record at this index, it gets trimmed
                        bars_slice = this_symbol.bars.iloc[
                            buffer : (current_record_index + 1)
                        ]
                        buy_signal_found = check_buy_signal(bars_slice, symbol=s)

                        if buy_signal_found:
                            # how much can we spend?
                            balance = this_symbol.api.get_account().assets[
                                this_symbol.api.default_currency
                            ]
                            buy_plan = BuyPlan(symbol=s, df=bars_slice)

                            order_result = this_symbol.api.buy_order_limit(
                                symbol=s,
                                units=buy_plan.units,
                                unit_price=buy_plan.entry_unit,
                            )

                            new_rule = {
                                "symbol": buy_plan.symbol,
                                "original_stop_loss": buy_plan.stop_unit,
                                "current_stop_loss": buy_plan.stop_unit,
                                "original_target_price": buy_plan.target_price,
                                "current_target_price": buy_plan.target_price,
                                "steps": 0,
                                "original_risk": buy_plan.risk_unit,
                                "current_risk": buy_plan.risk_unit,
                                "purchase_date": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "purchase_price": buy_plan.entry_unit,
                                "units_held": buy_plan.units,  # TODO
                                "units_sold": 0,
                                "units_bought": buy_plan.units,
                                "order_id": 0,  # TODO
                                "sales": [],
                                "win_point_sell_down_pct": 0.5,
                                "win_point_new_stop_loss_pct": 0.99,
                                "risk_point_sell_down_pct": 0.25,
                                "risk_point_new_stop_loss_pct": 0.98,
                            }
                            global df_trade_report
                            df_trade_report.loc[df_trade_report.shape[0]] = [
                                current_record,
                                s,
                                "BUY",
                                "LIMIT",
                                buy_plan.units,
                                buy_plan.entry_unit,
                                buy_plan.units * buy_plan.entry_unit,
                            ]

                            rules_result = merge_rules(
                                ssm=self.ssm,
                                symbol=s,
                                action="create",
                                new_rule=new_rule,
                                back_testing=self.back_testing,
                            )
                            if rules_result != False:
                                rules = rules_result
                                put_rules(
                                    ssm=self.ssm,
                                    symbol=s,
                                    new_rules=rules,
                                    back_testing=self.back_testing,
                                )

                            else:
                                # returns False if the rule already existed - this should not happen because it should raise a Value exception in that case...
                                ...

                    else:
                        log_wp.debug(
                            f"{s}: Position held so skipped buy analysis (back_test={self.back_testing})"
                        )

                    # move on to the next one
                    current_record_index += 1

                # hold on to last processed record so we can make sure we don't re-process it
                this_symbol.last_date_processed = this_symbol.bars.index[-1]

            # we've processed all data for all symbols
            if self.back_testing:
                # if we get here, we've finished processing
                break
            else:
                # if we get here, we need to sleep til we can get more data
                df_trade_report.to_csv("trade_report.csv")
                pause = get_pause()
                log_wp.debug(f"Sleeping for {round(pause,0)}s")
                time.sleep(pause)

                for s in self.symbols:
                    self.symbols[s].update_bars()

    def list_positions(self):
        positions = {}
        for api in self.api_dict:
            this_api = self.api_dict[api]
            this_api.position = this_api.list_positions()
            positions[api] = this_api.position
        return positions


def apply_sell_rules(
    ssm,
    api: ITradeAPI,
    rules: list,
    position: IPosition,
    last_close: float,
    symbol: str,
    period: pd.Timestamp,
    back_testing: bool = False,
    back_testing_unit_price: float = None,
):
    global df_trade_report
    stop_loss_triggered = []
    sell_point_triggered = []
    risk_point_triggered = []
    trigger_results = []

    # TODO this is so shonky - basically if back_testing is True but back_testing_unit_price is None, assume we're analysing the last record and do nothing
    if back_testing and back_testing_unit_price == None:
        log_wp.debug(
            f"{symbol}: Reached last rule, ignoring apply_sell_rules (back_testing={back_testing})"
        )
        return rules

    for rule in rules:
        rule_symbol = rule["symbol"]

        if rule_symbol == symbol:
            # matched a rule
            trigger_stop = trigger_stop_loss(rule, last_close, period)
            trigger_sell = trigger_sell_point(rule, last_close, period)
            trigger_risk = trigger_risk_point(rule, last_close, period)

            if trigger_stop:
                # stop loss hit! liquidate
                log_wp.warning(
                    f"{symbol}: Stop loss triggered at {period}, closing position at market value (back_testing={back_testing})"
                )
                close_response = api.close_position(symbol, back_testing_unit_price)

                if close_response.success:
                    # also need to write an updated rule to SSM for next run
                    rules_result = merge_rules(
                        ssm=ssm,
                        symbol=symbol,
                        action="delete",
                        back_testing=back_testing,
                    )
                    if rules_result != False:
                        rules = rules_result
                        put_rules(
                            ssm=ssm,
                            symbol=symbol,
                            new_rules=rules_result,
                            back_testing=back_testing,
                        )
                    df_trade_report.loc[df_trade_report.shape[0]] = [
                        period,
                        symbol,
                        "SELL",
                        "STOP",
                        position.quantity,
                        back_testing_unit_price,
                        position.quantity * back_testing_unit_price,
                    ]

                    return rules
                else:
                    # need a better way of notifying me of this stuff
                    log_wp.critical(
                        f"{symbol}: HIT STOP LOSS AT {period} BUT FAILED TO BE LIQUIDATED (back_testing={back_testing})"
                    )
                    raise RuntimeError()

            elif trigger_sell or trigger_risk:
                if trigger_sell:
                    new_target_pct = rule["win_point_sell_down_pct"]
                    # reporting
                    sell_point_triggered.append(
                        {
                            "symbol": symbol,
                            "last_close": last_close,
                            "rule": rule,
                        }
                    )
                else:
                    # trigger risk
                    new_target_pct = rule["risk_point_sell_down_pct"]
                    # reporting
                    risk_point_triggered.append(
                        {
                            "symbol": symbol,
                            "last_close": last_close,
                            "rule": rule,
                        }
                    )
                    new_target_pct = 0

                # hit high watermark of target price
                units_to_sell = position.quantity * new_target_pct
                sell_response = api.sell_order_market(
                    symbol=symbol,
                    units=units_to_sell,
                    back_testing_unit_price=back_testing_unit_price,
                )

                if not sell_response.success:
                    # need a better way of notifying me of this stuff
                    log_wp.critical(
                        f"{symbol}: FAILED TO TAKE PROFIT AT {period} ****** DO NOT IGNORE THIS *****"
                    )
                    raise RuntimeError()

                # order accepted but not filled
                if sell_response.status_summary == "cancelled":
                    raise RuntimeError(
                        f"Order was cancelled immediately by broker {str(sell_response)}"
                    )
                else:
                    if sell_response.status_summary == "open":
                        log_wp.critical(
                            f'{symbol}: Hit target sale point at {period}. Submitted sell market order for {round(rule["win_point_sell_down_pct"]*100,0)}% of units'
                        )
                    elif sell_response.status_summary == "filled":
                        # order filled
                        sell_value = sell_response.total_value
                        log_wp.critical(
                            f'{symbol}: Hit target sale point at {period}. Successfully sold {round(rule["win_point_sell_down_pct"]*100,0)}% of units for total value {round(sell_value,2)}'
                        )

                    new_units_held = api.get_position(symbol=symbol).quantity

                    updated_ssm_rule = rule.copy()

                    new_units_sold = rule["units_sold"] + sell_response.units
                    new_sales_obj = {
                        "units": new_units_sold,
                        "sale_price": sell_response.unit_price,
                    }
                    new_steps = updated_ssm_rule["steps"] + 1
                    new_risk = rule["original_risk"] * new_steps
                    new_stop_loss = (
                        sell_response.unit_price
                    )  # TODO you messed with this!

                    updated_ssm_rule["current_stop_loss"] = new_stop_loss
                    updated_ssm_rule["current_risk"] = new_risk
                    updated_ssm_rule["sales"].append(new_sales_obj)
                    updated_ssm_rule["units_held"] = new_units_held
                    updated_ssm_rule["units_sold"] = new_units_sold
                    updated_ssm_rule["steps"] += new_steps
                    updated_ssm_rule["current_target_price"] = (
                        updated_ssm_rule["current_target_price"]
                        + updated_ssm_rule["original_risk"]
                    )

                    rules_result = merge_rules(
                        ssm=ssm,
                        symbol=symbol,
                        action="replace",
                        new_rule=updated_ssm_rule,
                        back_testing=back_testing,
                    )
                    if rules_result != False:
                        rules = rules_result
                        put_rules(
                            ssm=ssm,
                            symbol=symbol,
                            new_rules=rules_result,
                            back_testing=back_testing,
                        )

                    df_trade_report.loc[df_trade_report.shape[0]] = [
                        period,
                        symbol,
                        "SELL",
                        "PROFIT",
                        sell_response.units,
                        back_testing_unit_price,
                        position.quantity * back_testing_unit_price,
                    ]

                    return rules

            else:
                log_wp.debug(f"{symbol}: Not ready to sell")
                return rules
    return rules


def main():
    back_testing = global_back_testing
    poll_time = 5
    log_wp.debug(
        f"Starting up, poll time is {poll_time}m, back testing is {back_testing}"
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
    else:
        store = ssm

    # TODO delete later!
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
    ## FINISH DELETE LATER

    bot_handler = MacdBot(
        ssm=store,
        data_source=data_source,
        back_testing=back_testing,
        starting_balance=10000,
    )

    if len(bot_handler.symbols) == 0:
        print(f"Nothing to do - no symbols to watch/symbols are invalid/no data")
        return

    bot_handler.new_start()

    global df_trade_report
    df_trade_report.to_csv("trade_report.csv")
    print("banana")


if __name__ == "__main__":
    main()
