import logging
import boto3
from alpaca_wrapper import AlpacaAPI
from swyftx_wrapper import SwyftxAPI
from back_test_wrapper import BackTestAPI
import yfinance as yf
import pandas as pd
import time
import json
from stock_symbol import Symbol
from buyplan import BuyPlan
from utils import get_pause, check_buy_signal
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

global_back_testing = False

# log_wp = logging.getLogger(__name__)  # or pass an explicit name here, e.g. "mylogger"
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
            {"symbol": "AAPL", "api": "swyftx"},
            {"symbol": "AXS", "api": "swyftx"},
            {"symbol": "TSLA", "api": "swyftx"},
            {"symbol": "FB", "api": "swyftx"},
            {"symbol": "GOOG", "api": "swyftx"},
            {"symbol": "MSFT", "api": "swyftx"},
            {"symbol": "NVDA", "api": "swyftx"},
            {"symbol": "NVAX", "api": "swyftx"},
            {"symbol": "BUD", "api": "swyftx"},
            {"symbol": "AMZN", "api": "swyftx"},
            {"symbol": "INFY", "api": "swyftx"},
            {"symbol": "RTX", "api": "swyftx"},
            {"symbol": "ADA-AUD", "api": "swyftx"},
            {"symbol": "BTC-AUD", "api": "swyftx"},
            {"symbol": "ETH-AUD", "api": "swyftx"},
            {"symbol": "SOL-AUD", "api": "swyftx"},
            {"symbol": "XRP-AUD", "api": "swyftx"},
            {"symbol": "DOGE-AUD", "api": "swyftx"},
            {"symbol": "SHIB-AUD", "api": "swyftx"},
            {"symbol": "MATIC-AUD", "api": "swyftx"},
            {"symbol": "ATOM-AUD", "api": "swyftx"},
            {"symbol": "FTT-AUD", "api": "swyftx"},
            {"symbol": "BNB-AUD", "api": "swyftx"},
        ]

        # symbols = [
        #    {"symbol": "AAPL", "api": "swyftx"},
        # ]

        if back_testing:
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

    def setup_brokers(self, api_list, ssm):
        api_set = set(api_list)
        api_dict = {}

        for api in api_set:
            start_time = time.time()
            if api == "back_test":
                api_dict[api] = BackTestAPI()
                break

            elif api == "swyftx":
                api_key = (
                    ssm.get_parameter(
                        Name="/tabot/swyftx/access_token", WithDecryption=True
                    )
                    .get("Parameter")
                    .get("Value")
                )
                api_dict[api] = SwyftxAPI(api_key=api_key)

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
                    alpaca_key_id=api_key, alpaca_secret_key=secret_key
                )
            else:
                raise ValueError(f"Unknown broker specified {api}")

            log_wp.debug(f"Set up {api} in {round(time.time() - start_time,1)}s")

        return api_dict

    def start(self):
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
                    # log_wp.debug(
                    #    f"{s}: Starting to process {records_to_process} new record(s) (back_test={self.back_testing})"
                    # )
                    # SELL

                    # BUY
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

                            print(f"{order_result}")
                            exit()

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


def write_rules(symbol: str, action: str, new_rule=None):
    ssm = boto3.client("ssm")
    old_rules = ssm.get_parameter(Name="/tabot/rules/5m").get("Parameter").get("Value")
    rules = json.loads(old_rules)

    changed = False
    if action == "delete":
        new_rules = []
        for rule in rules:
            if rule["symbol"].lower() != symbol.lower():
                new_rules.append(rule)
            else:
                changed = True

    elif action == "replace":
        new_rules = []
        for rule in rules:
            if rule["symbol"].lower() != symbol.lower():
                new_rules.append(rule)
            else:
                new_rules.append(new_rule)
                changed = True
    elif action == "create":
        new_rules = []
        for rule in rules:
            if rule["symbol"].lower() != symbol.lower():
                new_rules.append(rule)
            else:
                raise ValueError(
                    f"Symbol already exists in SSM rules! {symbol.lower()}"
                )

        new_rules.append(new_rule)
        changed = True

    else:
        raise Exception("write_rules: No action specified?")

    if changed == True:
        ssm.put_parameter(
            Name="/tabot/rules/5m",
            Value=json.dumps(new_rules),
            Type="String",
            Overwrite=True,
        )
    else:
        print(f"Symbol {symbol} - tried updating rules but nothing to change")

    return True


def apply_rules(rules, positions, last_close_dict):
    stop_loss_triggered = []
    sell_point_triggered = []
    risk_point_triggered = []
    trigger_results = []

    ssm = boto3.client("ssm")
    api_dict = setup_brokers(broker_list=["alpaca", "swyftx"], ssm=ssm)

    for broker in positions:
        for held in positions[broker]:
            held_symbol = held.symbol.lower()
            held_quantity = float(held.quantity)
            last_close = last_close_dict[held_symbol]

            for rule in rules:
                rule_symbol = rule["symbol"].lower()

                if rule_symbol == held_symbol:
                    # matched a rule and a holding
                    trigger_stop = trigger_stop_loss(rule, last_close)
                    trigger_sell = trigger_sell_point(rule, last_close)
                    trigger_risk = trigger_risk_point(rule, last_close)
                    if trigger_stop:
                        # stop loss hit! liquidate
                        close_response = api_dict[broker].close_position(held_symbol)

                        if close_response.success:
                            # also need to write an updated rule to SSM for next run
                            updated_rules = write_rules(
                                action="delete", symbol=held_symbol
                            )

                            # hold on to this for reporting
                            stop_loss_triggered.append(
                                {
                                    "symbol": held_symbol,
                                    "last_close": last_close,
                                    "rule": rule,
                                }
                            )

                            print(
                                f"Symbol {held_symbol} hit stop loss and was liquidated"
                            )
                        else:
                            # need a better way of notifying me of this stuff
                            print(
                                f"CRITICAL - SYMBOL {held_symbol} HIT STOP LOSS BUT FAILED TO BE LIQUIDATED ****** DO NOT IGNORE THIS *****"
                            )
                            trigger_results.append(
                                f"CRITICAL: SYMBOL {held_symbol} HIT STOP LOSS {last_close} BUT FAILED TO BE LIQUIDATED"
                            )

                    elif trigger_sell or trigger_risk:
                        if trigger_sell:
                            new_target_pct = rule["win_point_sell_down_pct"]
                            # reporting
                            sell_point_triggered.append(
                                {
                                    "symbol": held_symbol,
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
                                    "symbol": held_symbol,
                                    "last_close": last_close,
                                    "rule": rule,
                                }
                            )

                        # hit high watermark of target price
                        units_to_sell = held_quantity * new_target_pct
                        sell_response = api_dict[broker].sell_order_market(
                            symbol=held_symbol, units=units_to_sell
                        )
                        sell_value = sell_response.total_value

                        if sell_response.success:
                            print(
                                f'Symbol {held_symbol} hit target sale point. Successfully sold {round(rule["win_point_sell_down_pct"]*100,0)}% of units for total value {round(sell_value,2)}'
                            )

                            new_units_held = (
                                api_dict[broker]
                                .get_position(symbol=held_symbol)
                                .quantity
                            )

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

                            updated_rules = write_rules(
                                action="replace",
                                symbol=held_symbol,
                                new_rule=updated_ssm_rule,
                            )

                        else:
                            # need a better way of notifying me of this stuff
                            print(
                                f"CRITICAL - SYMBOL {held_symbol} FAILED TO TAKE PROFIT ****** DO NOT IGNORE THIS *****"
                            )
                            trigger_results.append(
                                f"CRITICAL: SYMBOL {held_symbol} FAILED TO TAKE PROFIT"
                            )

                    else:
                        print("do nothing")

    return {
        "stop_loss": stop_loss_triggered,
        "sell_point": sell_point_triggered,
        "risk_point": risk_point_triggered,
    }


def main():
    back_testing = global_back_testing
    poll_time = 5
    log_wp.debug(
        f"Starting up, poll time is {poll_time}m, back testing is {back_testing}"
    )
    ssm = boto3.client("ssm")
    data_source = yf

    # rules = get_rules()
    # validate_rules(rules)
    bot_handler = MacdBot(ssm, data_source, back_testing=back_testing)
    bot_handler.start()


def trigger_sell_point(rule, last_price):
    if rule["current_target_price"] < last_price:
        print(
            f'{rule["symbol"]}: Target price met (market {last_price} vs {rule["current_target_price"]})'
        )
        return True
    else:
        return False


def trigger_risk_point(rule, last_price):
    if (last_price + rule["current_risk"]) < last_price:
        print(
            f'{rule["symbol"]}: Risk price met (market {last_price} vs {(last_price + rule["current_risk"])}'
        )
        return True
    else:
        return False


def trigger_stop_loss(rule, last_price):
    if rule["current_stop_loss"] >= last_price:
        print(
            f'{rule["symbol"]}: Stop loss triggered (market {last_price} vs {rule["current_stop_loss"]})'
        )
        return True
    else:
        return False


def get_rules(ssm):
    return json.loads(
        ssm.get_parameter(Name="/tabot/rules/5m", WithDecryption=False)
        .get("Parameter")
        .get("Value")
    )


def validate_rule(rule):
    required_keys = [
        "symbol",
        "original_stop_loss",
        "current_stop_loss",
        "original_target_price",
        "current_target_price",
        "steps",
        "original_risk",
        "purchase_date",
        "units_held",
        "units_sold",
        "units_bought",
        "win_point_sell_down_pct",
        "win_point_new_stop_loss_pct",
        "risk_point_sell_down_pct",
        "risk_point_new_stop_loss_pct",
    ]

    rule_keys = rule.keys()

    # duplicate key
    duplicate_keys = len(set(rule_keys)) - len(rule_keys)
    if duplicate_keys != 0:
        raise ValueError(
            f'Duplicate rules found for symbol {rule["symbol"]}: {str(set(required_keys) ^ set(rule_keys))}'
        )

    for req_key in required_keys:
        if req_key not in rule_keys:
            raise ValueError(
                f'Invalid rule found for symbol {rule["symbol"]}: {req_key}'
            )


def validate_rules(rules):
    found_symbols = []
    for rule in rules:
        validate_rule(rule)
        if rule["symbol"] in found_symbols:
            raise ValueError(f'More than 1 rule found for {rule["symbol"]}')

        found_symbols.append(rule["symbol"])

    return True


if __name__ == "__main__":
    main()
