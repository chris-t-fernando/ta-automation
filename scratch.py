text = "symbol=abc date_from=2022-01-01T04:16:13+10:00 algos=awesome-oscillator,stoch confidence=7"

parameters = text.split()

valid_parameters = {}
found_keys = set()
errors = ""
error_found = False

mandatory_keys = {"symbol", "date_from", "algos", "apple"}
optional_keys = {
    "date_to",
    "resolution",
    "search_period",
    "notify_method",
    "notify_recipient",
    "target_ta_confidence",
    "confidence",
}

valid_keys = mandatory_keys.union(optional_keys)

for parameter in parameters:
    split_parameter = parameter.split("=")
    if len(split_parameter) != 2:
        errors += f"Input parameter set without value assignment: {split_parameter[0]}=what?\n"
        error_found = True

    valid_parameters[split_parameter[0]] = split_parameter[1]
    found_keys.add(split_parameter[0])

# see if there's an invalid parameter specified
if len(found_keys.difference(valid_keys)) > 0:
    errors += f"Invalid key specified: {str(found_keys.difference(valid_keys))}\n"
    error_found = True


input_parameters = valid_parameters
# if a mandatory key was omitted
if len(mandatory_keys.difference(found_keys)) > 0:
    errors += f"Mandatory key missing: {str(mandatory_keys.difference(found_keys))}\n"
    error_found = True

job = {
    "jobs": [
        {
            "symbol": input_parameters["symbol"],
            "date_from": input_parameters["date_from"],
            "ta_algos": [{input_parameters["algos"]: None}],
        }
    ]
}

for optional in optional_keys:
    if input_parameters.get(optional) != None:
        job["jobs"][0][optional] = input_parameters[optional]
print("hel")
