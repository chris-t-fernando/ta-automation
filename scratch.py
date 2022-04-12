import yfinance as yf
from datetime import datetime
import math


class RuleValidationException(Exception):
    ...


# model from which validation works
valid_rule_model = {
    "rule_name": str,
    "trigger_pct": int,
    "action": str,
    "action_amount_pct": int,
    "ignore_interval": int,
}
valid_action_strings = {"pass", "sell"}
valid_rule_keys = list(valid_rule_model)


# check typing
def check_rule_types(ruleset):
    for rule in ruleset:
        for valid_key in valid_rule_keys:
            if not isinstance(rule[valid_key], valid_rule_model[valid_key]):
                raise RuleValidationException(
                    f"key '{valid_key}' must be type {valid_rule_model[valid_key]}, instead found {type(rule[valid_key])}"
                )


# check that percentages are between 1 and 100
def check_valid_pcts(ruleset):
    for rule in ruleset:
        if rule["trigger_pct"] < 1 or rule["trigger_pct"] > 100:
            raise RuleValidationException(
                f'Percentages must be between 1 and 100. Found {rule["trigger_pct"]} in \'trigger_pct\''
            )

        if rule["action_amount_pct"] < 0 or rule["action_amount_pct"] > 100:
            raise RuleValidationException(
                f'Percentages must be between 1 and 100. Found {rule["action_amount_pct"]} in \'action_amount_pct\''
            )
    return True


# check that all keys are specified in each rule
def check_rule_keys(ruleset):
    for rule in ruleset:
        for valid_key in valid_rule_keys:
            if valid_key not in rule.keys():
                raise RuleValidationException(
                    f"Malformed rule - missing key '{valid_key}' (reminder that rules are case sensitive)"
                )

    return True


# check that no keys are duplicated
def check_for_duplicates(ruleset, key):
    rule_pct_list = []
    for rule in ruleset:
        rule_pct_list.append(rule[key])

    rule_pct_set = set(rule_pct_list)

    if len(rule_pct_set) != len(rule_pct_list):
        raise RuleValidationException(f"Duplicate {key} values in ruleset")

    return True


# check that rules are ordered based on trigger_pct ascending
def check_rule_value_order(ruleset):
    last_rule_trigger_pct = 0
    for rule in ruleset:
        if rule["trigger_pct"] <= last_rule_trigger_pct:
            raise RuleValidationException(
                f'trigger_pct must be ascending.  Found {last_rule_trigger_pct} followed by {rule["trigger_pct"]}'
            )
        last_rule_trigger_pct = rule["trigger_pct"]

    return True


def check_passes_have_no_action_amount_pct(ruleset):
    for rule in ruleset:
        if rule["action"] == "pass":
            if rule["action_amount_pct"] != 0:
                raise RuleValidationException(
                    f'action_amount_pct must be 0 when action is set to pass. Found: {rule["action_amount_pct"]} instead'
                )


def check_rule_actions(ruleset):
    for rule in ruleset:
        if rule["action"] not in valid_action_strings:
            raise RuleValidationException(
                f'Valid rule actions are {str(valid_action_strings)}. Found {rule["action"]} instead'
            )
        # check to make sure actions are lower case
        if rule["action"].lower() != rule["action"]:
            raise RuleValidationException(
                f'Rule actions must be lower case. Found {rule["action"]} instead'
            )


def validate_ruleset(ruleset):
    check_rule_keys(ruleset=ruleset)
    check_rule_types(ruleset=ruleset)
    # can't have two rules that have the same trigger_pct, else how do you know which to execute?
    check_for_duplicates(ruleset=ruleset, key="trigger_pct")
    check_rule_value_order(ruleset=ruleset)
    check_valid_pcts(ruleset=ruleset)
    check_passes_have_no_action_amount_pct(ruleset=ruleset)
    check_rule_actions(ruleset=ruleset)


ruleset = [
    {
        "rule_name": "hodl",
        "trigger_pct": 20,
        "action": "pass",
        "action_amount_pct": 0,
        "ignore_interval": 0,
    },
    {
        "rule_name": "hold out hope",
        "trigger_pct": 50,
        "action": "sell",
        "action_amount_pct": 20,
        "ignore_interval": 5,
    },
    {
        "rule_name": "yolo",
        "trigger_pct": 70,
        "action": "sell",
        "action_amount_pct": 70,
        "ignore_interval": 5,
    },
    {
        "rule_name": "bail out",
        "trigger_pct": 100,
        "action": "sell",
        "action_amount_pct": 100,
        "ignore_interval": 5,
    },
]

# check that our rules are valid
validate_ruleset(ruleset)

prices = [
    33.3,
    34.5,
    34.0,
    34.2,
    35.3,
    35.1,
    36.7,
    38.1,
    37.1,
    37.3,
    40.3,
    40.4,
    39.2,
    39.0,
    39.0,
    40.6,
    40.0,
    39.2,
    36.7,
    36.8,
    36.7,
    35.9,
    33.0,
]

buy_unit_price = 33.3
buy_unit_quantity = 100
buy_total_value = buy_unit_price * buy_unit_quantity
cur_unit_price = buy_unit_price
cur_unit_quantity = buy_unit_quantity
cur_total_value = buy_total_value
cur_profit = 0
ath_unit_price = 0
# don't need ath unit quantity since its not relevant
ath_total_value = 0
ath_profit = 0

sell_orders = []


def evaluate_ruleset(ruleset, cur_drop_against_ath_pct, interval):
    for rule in ruleset:
        if cur_drop_against_ath_pct <= rule["trigger_pct"]:
            if interval > rule["ignore_interval"]:
                return rule
            else:
                # print(
                #    f'Would have triggered {rule["rule_name"]} but interval {interval} is within ignore_interval of {rule["ignore_interval"]} specified by this rule'
                # )
                break

    # this can happen if the interval is within the ignore window
    return False


# count the number of data intervals we've processed so far - used when processing the ignore_interval setting
# gets incremented at the end of the coming for loop
interval = 0

# simulate price changes
for price in prices:
    # check if there is a new ath
    if price > ath_unit_price:
        ath_unit_price = price
        # i don't know about this - am i working out the max i could have earned if I held? or am i working out the max that i could currently earn?
        ath_total_value = price * buy_unit_quantity
        ath_profit = price - buy_unit_price

    # calculate % gap against ath
    cur_drop_against_ath = ath_unit_price - price
    try:
        cur_drop_against_ath_pct = cur_drop_against_ath / ath_profit
    except ZeroDivisionError as e:
        cur_drop_against_ath_pct = 0.0

    # evaluate rules
    matched_rule = evaluate_ruleset(
        ruleset=ruleset,
        cur_drop_against_ath_pct=cur_drop_against_ath_pct * 100,
        interval=interval,
    )

    if matched_rule == False:
        print(
            f"Interval {interval}: No rule matched, likely because a rule was ignored because we're only at {interval}"
        )
    else:
        print(
            f'Interval {interval}: ATH drop of {round(cur_drop_against_ath_pct * 100,1)}% matched rule \'{matched_rule["rule_name"]}\'. Executing {matched_rule["action"]}'
        )

        if matched_rule["action"] == "pass":
            # print(f"Interval {interval}: Action is pass, doing nothing")
            ...
        elif matched_rule["action"] == "sell":
            units_to_sell = math.ceil(
                cur_unit_quantity * (matched_rule["action_amount_pct"] / 100)
            )

            this_sale = {
                "units": units_to_sell,
                "interval": interval,
                "unit_price": price,
                "total_value": units_to_sell * cur_unit_price,
            }

            sell_orders.append(this_sale)

            print(
                f'Interval {interval}: Sold {this_sale["units"]} units at {this_sale["unit_price"]} for {this_sale["total_value"]}'
            )

            cur_unit_quantity -= units_to_sell
            cur_total_value = cur_unit_quantity * price

    cur_unit_price = price
    interval += 1

# calculate summary data and format it
sold_value = 0
for sale in sell_orders:
    sold_value += sale["total_value"]

held_value = cur_unit_quantity * price
end_total_value = sold_value + held_value

lost_profit = (ath_unit_price * buy_unit_quantity) - end_total_value
saved_profit = end_total_value - (price * buy_unit_quantity)

formatted_buy_total_value = "{:,}".format(round(buy_total_value, 0))
formatted_end_total_value = "{:,}".format(round(end_total_value, 0))
formatted_sold_value = "{:,}".format(round(sold_value, 0))
formatted_held_value = "{:,}".format(round(held_value, 0))
formatted_total_profit = "{:,}".format(round(end_total_value - buy_total_value, 0))
formatted_lost_profit = "{:,}".format(round(lost_profit, 0))
formatted_saved_profit = "{:,}".format(round(saved_profit, 0))
formatted_lost_profit_pct = (
    round(lost_profit / (ath_unit_price * buy_unit_quantity), 1) * 100
)
formatted_saved_profit_pct = 100 - formatted_lost_profit_pct

print(f"Starting amount: ${formatted_buy_total_value}")
print(
    f"Ending amount: ${formatted_end_total_value} (${formatted_sold_value} sold, ${formatted_held_value} held)"
)
print(f"Profit: ${formatted_total_profit}")
print(
    f"Lost profit (if we'd sold at the highest point): ${formatted_lost_profit} ({formatted_lost_profit_pct}%)"
)
print(f"Strategy saved ${formatted_saved_profit}")
