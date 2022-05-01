import logging
import boto3
from alpaca_wrapper import AlpacaAPI
from swyftx_wrapper import SwyftxAPI
from back_test_wrapper import BackTestAPI, Position
from itradeapi import ITradeAPI, IPosition
import yfinance as yf
import pandas as pd
import time
import json
from datetime import datetime
from stock_symbol import Symbol
from buyplan import BuyPlan
from utils import (
    get_pause,
    check_buy_signal,
    validate_rules,
    get_rules,
    write_rules,
    merge_rules,
    trigger_stop_loss,
    trigger_sell_point,
    trigger_risk_point,
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


class MacdBot:
    jobs = None

    def __init__(self, ssm, data_source, start_period=None, back_testing=False):
        interval = "5m"
        real_money_trading = False
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
        ]

        if back_testing and global_override_broker:
            for s in symbols:
                s["api"] = "back_test"

        # get brokers and then set them up
        self.api_list = []
        for api in symbols:
            self.api_list.append(api["api"])
            log_wp.debug(f"Found broker {api}")
        self.api_list = list(set(self.api_list))
        self.api_dict = self.setup_brokers(api_list=self.api_list, ssm=ssm)

        # set up individual symbols
        self.symbols = {}
        for s in symbols:
            start_time = time.time()
            self.symbols[s["symbol"]] = Symbol(
                symbol=s["symbol"],
                interval=interval,
                real_money_trading=real_money_trading,
                api=self.api_dict[s["api"]],
                ssm=ssm,
                data_source=data_source,
            )
            log_wp.debug(
                f'Set up {s["symbol"]} in {round(time.time() - start_time,1)}s'
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
                )
            else:
                raise ValueError(f"Unknown broker specified {api}")

            log_wp.debug(f"Set up {api} in {round(time.time() - start_time,1)}s")

        return api_dict

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
                if this_symbol.bars.index[-1] != this_symbol.last_date_processed:
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

                        # SELL
                        # if self.back_testing:
                        #    # default value
                        #    position = Position(symbol=s, quantity=0)
                        #    for api in positions:
                        #        for symbol_position in positions[api]:
                        #            if symbol_position.symbol == s:
                        #                position = symbol_position.quantity
                        #                break
                        # else:
                        #    # if we are not back testing, then i'm okay to call this every 5 minutes or whatever - but for backtesting it doesn't make sense
                        #    position = self.get_position(s)
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

                        # if current_record == pd.Timestamp("2022-03-16 15:45:00"):
                        #    print('banana')

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

                                rules_result = merge_rules(
                                    ssm=self.ssm,
                                    symbol=s,
                                    action="create",
                                    new_rule=new_rule,
                                    back_testing=self.back_testing,
                                )
                                if rules_result != False:
                                    rules = rules_result
                                    write_rules(
                                        ssm=self.ssm,
                                        symbol=s,
                                        new_rules=rules,
                                        back_testing=self.back_testing,
                                    )

                                else:
                                    # returns False if the rule already existed - this should not happen because it should raise a Value exception in that case...
                                    ...
                                print("banana")
                        else:
                            log_wp.debug(
                                f"{s}: Position held so skipped buy analysis (back_test={self.back_testing})"
                            )

                        # move on to the next one
                        current_record_index += 1
                else:
                    log_wp.debug(
                        f"{s}: No new records to process. Last record was {this_symbol.bars.index[-1]} (back_test={self.back_testing})"
                    )

                # hold on to last processed record so we can make sure we don't re-process it
                this_symbol.last_date_processed = this_symbol.bars.index[-1]

            # we've processed all data for all symbols
            if self.back_testing:
                # if we get here, we've finished processing
                break
            else:
                # if we get here, we need to sleep til we can get more data
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
                        write_rules(
                            ssm=ssm,
                            symbol=symbol,
                            new_rules=rules_result,
                            back_testing=back_testing,
                        )
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

                # hit high watermark of target price
                units_to_sell = position.quantity * new_target_pct
                sell_response = api.sell_order_market(
                    symbol=symbol,
                    units=units_to_sell,
                    back_testing_unit_price=back_testing_unit_price,
                )
                sell_value = sell_response.total_value

                if sell_response.success:
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
                    new_stop_loss = sell_response.unit_price + new_risk

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
                        write_rules(
                            ssm=ssm,
                            symbol=symbol,
                            new_rules=rules_result,
                            back_testing=back_testing,
                        )

                    return rules

                else:
                    # need a better way of notifying me of this stuff
                    log_wp.critical(
                        f"{symbol}: FAILED TO TAKE PROFIT AT {period} ****** DO NOT IGNORE THIS *****"
                    )
                    raise RuntimeError()

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
        ssm.put_parameter(
            Name=f"/tabot/rules/backtest/5m",
            Value=json.dumps([]),
            Type="String",
            Overwrite=True,
        )

    bot_handler = MacdBot(ssm, data_source, back_testing=back_testing)
    bot_handler.start()
    print("banan")


if __name__ == "__main__":
    main()
