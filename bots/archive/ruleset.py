class RuleValidationException(Exception):
    ...


class RuleSet:
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
    
    def __init__(self, ruleset):
        self.check_rule_keys(ruleset=ruleset)
        self.check_rule_types(ruleset=ruleset)
        # can't have two rules that have the same trigger_pct, else how do you know which to execute?
        self.check_for_duplicates(ruleset=ruleset, key="trigger_pct")
        self.check_rule_value_order(ruleset=ruleset)
        self.check_valid_pcts(ruleset=ruleset)
        self.check_passes_have_no_action_amount_pct(ruleset=ruleset)
        self.check_rule_actions(ruleset=ruleset)

        self.ruleset = ruleset

    # check typing
    def check_rule_types(self, ruleset):
        for rule in ruleset:
            for valid_key in self.valid_rule_keys:
                if not isinstance(rule[valid_key], self.valid_rule_model[valid_key]):
                    raise RuleValidationException(
                        f"key '{valid_key}' must be type {self.valid_rule_model[valid_key]}, instead found {type(rule[valid_key])}"
                    )
        return True

    # check that percentages are between 1 and 100
    def check_valid_pcts(self, ruleset):
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
    def check_rule_keys(self, ruleset):
        for rule in ruleset:
            for valid_key in self.valid_rule_keys:
                if valid_key not in rule.keys():
                    raise RuleValidationException(
                        f"Malformed rule - missing key '{valid_key}' (reminder that rules are case sensitive)"
                    )

        return True

    # check that no keys are duplicated
    def check_for_duplicates(self, ruleset, key):
        rule_pct_list = []
        for rule in ruleset:
            rule_pct_list.append(rule[key])

        rule_pct_set = set(rule_pct_list)

        if len(rule_pct_set) != len(rule_pct_list):
            raise RuleValidationException(f"Duplicate {key} values in ruleset")

        return True

    # check that rules are ordered based on trigger_pct ascending
    def check_rule_value_order(self, ruleset):
        last_rule_trigger_pct = 0
        for rule in ruleset:
            if rule["trigger_pct"] <= last_rule_trigger_pct:
                raise RuleValidationException(
                    f'trigger_pct must be ascending.  Found {last_rule_trigger_pct} followed by {rule["trigger_pct"]}'
                )
            last_rule_trigger_pct = rule["trigger_pct"]

        return True

    def check_passes_have_no_action_amount_pct(self, ruleset):
        for rule in ruleset:
            if rule["action"] == "pass":
                if rule["action_amount_pct"] != 0:
                    raise RuleValidationException(
                        f'action_amount_pct must be 0 when action is set to pass. Found: {rule["action_amount_pct"]} instead'
                    )

    def check_rule_actions(self, ruleset):
        for rule in ruleset:
            if rule["action"] not in self.valid_action_strings:
                raise RuleValidationException(
                    f'Valid rule actions are {str(self.valid_action_strings)}. Found {rule["action"]} instead'
                )
            # check to make sure actions are lower case
            if rule["action"].lower() != rule["action"]:
                raise RuleValidationException(
                    f'Rule actions must be lower case. Found {rule["action"]} instead'
                )
