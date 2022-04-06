import json


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

    total = 0
    for job in event["Payload"]:
        total += job["ta_analysis"]["confidence"]

    average_confidence = total / len(event["Payload"])

    jobs = {}
    jobs["job"] = {
        "symbol": event["Payload"][0]["symbol"],
        "date_from": event["Payload"][0]["date_from"],
        "date_to": event["Payload"][0]["date_to"],
        "ta_algo": event["Payload"][0]["ta_algo"],
        "resolution": event["Payload"][0]["resolution"],
        "search_period": event["Payload"][0]["search_period"],
        "notify_method": event["Payload"][0]["notify_method"],
        "notify_recipient": event["Payload"][0]["notify_recipient"],
        "target_ta_confidence": event["Payload"][0]["target_ta_confidence"],
    }

    jobs["ta_summary"] = {
        # basically this is just a big passthrough for now, until i work out how to allow
        # the decision making to be set programmatically to give an overall score
        # but also im not 100% sure why I'd want this
        # i think this part is going to be super complex
        "overall_ta_confidence": average_confidence,
        "target_ta_confidence": event["Payload"][0]["target_ta_confidence"],
    }

    jobs["symbol_data"] = event["Payload"][0]["symbol_data"]

    jobs["ta_analyses"] = []
    for ta_result in event["Payload"]:
        this_job = ta_result.copy()
        del this_job["symbol_data"]
        jobs["ta_analyses"].append(this_job)

    return jobs


if __name__ == "__main__":
    payload = {
        "Payload": [
            {
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-03-30T04:16:13+10:00",
                "resolution": "1d",
                "search_period": 7,
                "notify_method": "pushover",
                "notify_recipient": "some-pushover-app-1",
                "target_ta_confidence": 7.5,
                "symbol": "bhp",
                "ta_algo": {
                    "awesome-oscillator": {"strategy": "saucer", "direction": "bullish"}
                },
                "symbol_data": {
                    "Open": {"1648512000000": 74.25},
                    "High": {"1641168000000": 58.1281684373},
                    "Low": {"1641168000000": 57.5160922104},
                    "Close": {"1641168000000": 57.7073669434},
                    "Volume": {"1641168000000": 1584400},
                    "Dividends": {"1641168000000": 0},
                    "Stock Splits": {"1641168000000": 0},
                },
                "ta_analysis": {
                    "confidence": 10,
                    "ta_data": {
                        "awesome-oscillator-buy-price": [None],
                        "awesome-oscillator-sell-price": [None],
                        "awesome-oscillator-signal": [0],
                        "awesome-oscillator": [0],
                    },
                },
                "graph_urls": "http://s3-ap-southeast-2.amazonaws.com/mfers-graphs/4038882483579503150.png",
            },
            {
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-03-30T04:16:13+10:00",
                "resolution": "1d",
                "search_period": 7,
                "notify_method": "pushover",
                "notify_recipient": "some-pushover-app-1",
                "target_ta_confidence": 7.5,
                "symbol": "tls",
                "ta_algo": {
                    "awesome-oscillator": {"strategy": "saucer", "direction": "bullish"}
                },
                "symbol_data": {
                    "Open": {"1641168000000": 15.7700004578},
                    "High": {"1641168000000": 16.1499996185},
                    "Low": {"1641168000000": 15.3000001907},
                    "Close": {"1641168000000": 16, "1641254400000": 15.3599996567},
                    "Volume": {"1641168000000": 440700},
                    "Dividends": {"1641168000000": 0},
                    "Stock Splits": {"1641168000000": 0},
                },
                "ta_analysis": {
                    "confidence": 0,
                    "ta_data": {
                        "awesome-oscillator-buy-price": [None],
                        "awesome-oscillator-sell-price": [None],
                        "awesome-oscillator-signal": [0],
                        "awesome-oscillator": [0],
                    },
                },
                "graph_urls": "http://s3-ap-southeast-2.amazonaws.com/mfers-graphs/173519927423286297.png",
            },
            {
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-03-30T04:16:13+10:00",
                "resolution": "1d",
                "search_period": 7,
                "notify_method": "pushover",
                "notify_recipient": "some-pushover-app-1",
                "target_ta_confidence": 7.5,
                "symbol": "nea",
                "ta_algo": {
                    "awesome-oscillator": {"strategy": "saucer", "direction": "bullish"}
                },
                "symbol_data": {
                    "Open": {"1641168000000": 15.3951891448},
                    "High": {"1641168000000": 15.4248140043},
                    "Low": {"1641168000000": 15.3161888918},
                    "Close": {"1641168000000": 15.3458137512},
                    "Volume": {"1641168000000": 451900},
                    "Dividends": {"1641168000000": 0},
                    "Stock Splits": {"1641168000000": 0},
                },
                "ta_analysis": {
                    "confidence": 0,
                    "ta_data": {
                        "awesome-oscillator-buy-price": [None],
                        "awesome-oscillator-sell-price": [None],
                        "awesome-oscillator-signal": [0],
                        "awesome-oscillator": [0],
                    },
                },
                "graph_urls": "http://s3-ap-southeast-2.amazonaws.com/mfers-graphs/7266681256766173335.png",
            },
        ]
    }
    lambda_handler(payload, None)
