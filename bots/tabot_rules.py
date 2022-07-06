# external packages
import json
import pandas as pd
import utils

# my modules
from buyplan import BuyPlan
from iparameter_store import IParameterStore
from itradeapi import IOrderResult
import logging

log_wp = logging.getLogger("tabot_rules")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("tabot_rules.log")
log_wp.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(fhdlr)
log_wp.addHandler(hdlr)


class TABotRules:
    def __init__(self, store: IParameterStore, rules_path: str, state_path: str):
        self.store = store
        self.rules_path = rules_path
        self.state_path = state_path

    # STATE AND RULE FUNCTIONS
    def get_state(self, symbol: str):
        stored_state = self.get_state_all()

        for this_state in stored_state:
            if this_state["symbol"] == symbol:
                return this_state

        return False

    def get_state_all(self):
        try:
            json_stored_state = self.store.get(path=self.state_path)
            return utils.unpickle(json_stored_state)

        except self.store.store.exceptions.ParameterNotFound as e:
            return []

    # writes the symbol to state
    def write_to_state(self, new_state: dict):
        symbol = new_state["symbol"]
        broker = new_state["broker"]
        stored_state = self.get_state_all()
        state_to_write = []

        for this_state in stored_state:
            # needs to match broker and symbol
            s_symbol = this_state["symbol"]
            s_broker = this_state["broker"]

            # no need for validation - its done in stock_symbol since rules has no access to API to query
            if s_symbol == symbol and s_broker == broker:
                log_wp.error(f"{symbol} ({broker}): Found this symbol in state already!")

            else:
                # it's not the state we're looking for so keep it
                state_to_write.append(this_state)

        state_to_write.append(new_state)

        self.put_stored_state(new_state=state_to_write)

        log_wp.log(9, f"{symbol}: Successfully wrote order to state")

    # removes this symbol from the state
    def remove_from_state(self, symbol: str, broker: str):
        stored_state = self.get_state_all()
        found_in_state = False

        new_state = []

        for this_state in stored_state:
            # needs to match broker and symbol
            s_symbol = this_state["symbol"]
            s_broker = this_state["broker"]
            if s_symbol == symbol and s_broker == broker:
                found_in_state = True
            else:
                # it's not the state we're looking for so keep it
                new_state.append(this_state)

        self.put_stored_state(new_state=new_state)

        if found_in_state:
            log_wp.log(9, f"{symbol}: Successfully wrote updated state")
            return True
        else:
            log_wp.warning(f"{symbol}: Tried to remove symbol from state but did not find it")
            return False

    # replaces the rule for this symbol
    def replace_rule(self, new_rule: dict, symbol: str):
        stored_rules = self.get_rules()

        new_rules = []

        for rule in stored_rules:
            if rule["symbol"] == symbol:
                new_rules.append(new_rule)
            else:
                new_rules.append(rule)

        write_result = self.put_rules(symbol=symbol, new_rules=new_rules)

        return write_result

    # adds sybol to rules - will barf if one already exists
    def write_to_rules(self, buy_plan: BuyPlan, order_result: IOrderResult):
        stored_rules = self.get_rules()

        new_rules = []

        for this_state in stored_rules:
            s_symbol = this_state["symbol"]
            if s_symbol == order_result.symbol:
                raise ValueError(
                    f"Tried to add {order_result.symbol} rules, but it already existed"
                )
            else:
                # it's not the state we're looking for so keep it
                new_rules.append(this_state)

        # if we got here, the symbol does not exist in rules so we are okay to add it
        new_rule = {
            "symbol": buy_plan.symbol,
            "play_id": buy_plan.play_id,
            "original_stop_loss": buy_plan.stop_unit,
            "current_stop_loss": buy_plan.stop_unit,
            "original_target_price": buy_plan.target_price,
            "current_target_price": buy_plan.target_price,
            "steps": 0,
            "original_risk": buy_plan.risk_unit,
            "current_risk": buy_plan.risk_unit,
            "purchase_date": buy_plan.blue_cycle_start,
            "purchase_price": order_result.filled_unit_price,
            "units_held": order_result.filled_unit_quantity,
            "units_sold": 0,
            "units_bought": order_result.filled_unit_quantity,
            "order_id": order_result.order_id,
            "sales": [],
            "win_point_sell_down_pct": 0.75,
            "win_point_new_stop_loss_pct": 0.995,
            "risk_point_sell_down_pct": 0.5,
            "risk_point_new_stop_loss_pct": 0.99,
        }

        new_rules.append(new_rule)

        self.put_rules(
            symbol=buy_plan.symbol,
            new_rules=new_rules,
        )

        log_wp.log(9, f"{buy_plan.symbol}: Successfully wrote new buy order to rules")

    # gets rule for this symbol
    def get_rule(self, symbol: str):
        stored_rules = self.get_rules()

        for this_rule in stored_rules:
            if this_rule["symbol"] == symbol:
                return this_rule

        return False

    # removes the symbol from the buy rules in store
    def remove_from_rules(self, symbol: str):
        stored_state = self.get_rules()
        found_in_rules = False

        new_rules = []

        for this_rule in stored_state:
            if this_rule["symbol"] == symbol:
                found_in_rules = True
            else:
                # not the rule we're looking to remove, so retain it
                new_rules.append(this_rule)

        if found_in_rules:
            self.put_rules(
                symbol=symbol,
                new_rules=new_rules,
            )
            log_wp.log(9, f"{symbol}: Successfully wrote updated rules")
            return True
        else:
            log_wp.warning(f"{symbol}: Tried to remove symbol from rules but did not find it")
            return False

    def validate_rule(rule: dict):
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
                raise ValueError(f'Invalid rule found for symbol {rule["symbol"]}: {req_key}')

    def validate_rules(rules):
        if rules == []:
            log_wp.debug(f"No rules found")
            return True

        found_symbols = []
        for rule in rules:
            TABotRules.validate_rule(rule)
            if rule["symbol"] in found_symbols:
                raise ValueError(f'More than 1 rule found for {rule["symbol"]}')

            log_wp.debug(f'Found valid rule for {rule["symbol"]}')
            found_symbols.append(rule["symbol"])

        log_wp.debug(f"Rules are valid")
        return True

    def get_rules(self):
        try:
            rules = self.store.get(path=self.rules_path)

        except self.store.exceptions.ParameterNotFound as e:
            return []

        rules = json.loads(rules)
        for rule in rules:
            rule["purchase_date"] = pd.Timestamp(rule["purchase_date"])
        return rules

    # merges rules but does not write them - just returns list of rule dicts
    def merge_rules(
        self,
        symbol: str,
        action: str,
        new_rule: dict = None,
    ) -> dict:
        rules = self.get_rules(store=self.store, rules_path=self.rules_path)

        changed = False
        if action == "delete":
            new_rules = []
            for rule in rules:
                if rule["symbol"] != symbol:
                    new_rules.append(rule)
                else:
                    changed = True

        elif action == "replace":
            new_rules = []
            for rule in rules:
                if rule["symbol"] != symbol:
                    new_rules.append(rule)
                else:
                    new_rules.append(new_rule)
                    changed = True

        elif action == "create":
            new_rules = []
            for rule in rules:
                if rule["symbol"] != symbol:
                    new_rules.append(rule)
                else:
                    # TODO this can actually happen - then what happens?!
                    # raise ValueError(
                    #    f"Cannot create {symbol} - symbol already exists in store rules!"
                    # )
                    ...

            new_rules.append(new_rule)
            changed = True

        else:
            log_wp.error(f"{symbol}: No action specified")
            raise Exception("No action specified")

        if changed == True:
            log_wp.log(9, f"{symbol}: Merged rules successfully")
            return new_rules
        else:
            log_wp.log(9, f"{symbol}: No rules changed!")
            return False

    def put_rules(self, symbol: str, new_rules: list):
        # convert Datetime objects to strings
        for rule in new_rules:
            rule["purchase_date"] = str(rule["purchase_date"])

        self.store.put(path=self.rules_path, value=json.dumps(new_rules))
        log_wp.log(9, f"{symbol}: Successfully wrote updated rules")

        return True

    def put_stored_state(self, new_state: list):
        pickled_state = utils.pickle(new_state)
        self.store.put(path=self.state_path, value=pickled_state)


# from parameter_stores import Ssm
# rules = TABotRules(store=Ssm(), rules_path="/tabot/paper/rules/5m", state_path="/tabot/paper/state")
# rules.write_to_state( new_state={"symbol":"def", "broker":"banana"})
# rules.write_to_state( new_state={"symbol":"hij", "broker":"banana"})
# rules.write_to_state( new_state={"symbol":"def", "broker":"apples"})
