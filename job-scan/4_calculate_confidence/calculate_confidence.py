import json

# import requests


def lambda_handler(event, context):

    # https://stackoverflow.com/questions/58774789/merging-json-outputs-of-parallel-states-in-step-function
    #
    # really annoyingly, there is no way using Step Functions to append stuff to a json arrays
    # so we need to get into the data flow here instead of using Step Functions to do it
    # this is really disappointing, since as soon as you run a map function, you get an array back - so
    # every time you map, you also need a lambda function to pull the bits you care about out of an array
    # and back into a dict so that Step Functions can address them ongoing
    #
    # i am really quite dirty about this.

    jobs = {}
    jobs["summary"] = {
        "overall_ta_confidence": 8.8,
        "target_ta_confidence": event["Payload"][0]["target_ta_confidence"],
    }
    jobs["analyses"] = []
    for ta_result in event["Payload"]:
        this_job = ta_result.copy()
        del this_job["symbol_data"]
        jobs["analyses"].append(this_job)

    return jobs


payload = {
    "Payload": [
        {
            "target_ta_confidence": 7.5,
            "symbol": "bhp",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "awesome-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "target_ta_confidence": 10,
            "symbol": "bhp",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "stoch",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "bhp",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "accelerator-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "tls",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "awesome-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "tls",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "stoch",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "tls",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "accelerator-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "nea",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "awesome-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "nea",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "stoch",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "nea",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "accelerator-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "ivv",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "awesome-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "ivv",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "stoch",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "ivv",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "accelerator-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "fang",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "awesome-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "fang",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "stoch",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "fang",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "accelerator-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "sol",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "awesome-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "sol",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "stoch",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
        {
            "symbol": "sol",
            "date_from": "2016-03-14T01:59:00Z",
            "date_to": "2016-03-14T01:59:00Z",
            "ta_algo": "accelerator-oscillator",
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-2",
            "symbol_data": {
                "2016-03-14T01:59:00Z": {"open": 1.5, "high": 2, "low": 0, "close": 1},
                "2016-03-14T02:59:00Z": {"open": 2.5, "high": 3, "low": 1, "close": 2},
            },
            "ta_analysis": {"ta_confidence": 6},
        },
    ]
}


if __name__ == "__main__":
    lambda_handler(payload, None)
