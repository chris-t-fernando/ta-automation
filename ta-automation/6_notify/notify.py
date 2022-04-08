from pushover import init, Client
import boto3


def lambda_handler(event, context):
    report_string = f'Symbol: {event["Payload"]["job"]["symbol"]}\n'
    report_string += f'Date from: {event["Payload"]["job"]["date_from"]}\n'
    report_string += f'Date to: {event["Payload"]["job"]["date_to"]}\n'
    report_string += (
        f'TA signal search: Last {event["Payload"]["job"]["search_period"]} quotes\n'
    )
    report_string += f'Minimum signal confidence: {event["Payload"]["job"]["target_ta_confidence"]}\n'
    report_string += f'Quote resolution: {event["Payload"]["job"]["resolution"]}\n'
    report_string += f"\n"

    for analysis in event["Payload"]["ta_analyses"]:
        for algo_name in analysis["ta_algo"]:
            # get the parameters handed to the algo
            parameters = []
            if analysis["ta_algo"][algo_name] == None:
                report_string += f"TA algo: {algo_name}\n"
            else:
                for parameter_key in analysis["ta_algo"][algo_name]:
                    parameters.append(
                        f'{parameter_key}: {analysis["ta_algo"][algo_name][parameter_key]}'
                    )
                report_string += f'TA algo: {algo_name} ({", ".join(parameters)})\n'

            report_string += (
                f'Signal confidence: {analysis["ta_analysis"]["confidence"]}\n'
            )
            report_string += f'Graph: {analysis["graph_url"]}\n'
            report_string += f"\n"

    if event["Payload"]["job"]["notify_method"] == "pushover":
        ssm = boto3.client("ssm")

        try:
            pushover_api_key = (
                ssm.get_parameter(Name="/tabot/pushover/api_key", WithDecryption=False)
                .get("Parameter")
                .get("Value")
            )

        #        pushover_user_key = (
        #            ssm.get_parameter(Name="/tabot/pushover/user_key", WithDecryption=False)
        #            .get("Parameter")
        #            .get("Value")
        #        )

        except Exception as e:
            print(f"Failed to retrieve Pushover config.  Error: {str(e)}")
            raise e

        init(pushover_api_key)
        Client(event["Payload"]["job"]["notify_recipient"]).send_message(
            report_string,
            title=f'Technical analysis for {event["Payload"]["job"]["symbol"]} complete',
        )

    return True


if __name__ == "__main__":
    payload = {}
    payload["Payload"] = {
        "job": {
            "symbol": "bhp",
            "date_from": "2022-01-01T04:16:13+10:00",
            "date_to": "2022-03-30T04:16:13+10:00",
            "ta_algo": {
                "awesome-oscillator": {"strategy": "saucer", "direction": "bullish"}
            },
            "resolution": "1d",
            "search_period": 20,
            "notify_method": "pushover",
            "notify_recipient": "some-pushover-app-1",
            "target_ta_confidence": 3,
        },
        "ta_summary": {"overall_ta_confidence": 10, "target_ta_confidence": 3},
        "symbol_data": {
            "Open": {"1641168000000": 57.9464594475},
            "High": {"1641168000000": 58.1281684373},
            "Low": {"1641168000000": 57.5160922104},
            "Close": {"1641168000000": 57.7073669434},
            "Volume": {"1641168000000": 1584400},
            "Dividends": {"1641168000000": 0},
            "Stock Splits": {"1641168000000": 0},
        },
        "ta_analyses": [
            {
                "symbol": "bhp",
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-03-30T04:16:13+10:00",
                "resolution": "1d",
                "search_period": 20,
                "notify_method": "pushover",
                "notify_recipient": "some-pushover-app-1",
                "target_ta_confidence": 3,
                "ta_algo": {
                    "awesome-oscillator": {"strategy": "saucer", "direction": "bullish"}
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
                "graph_url": "http://s3-ap-southeast-2.amazonaws.com/mfers-graphs/-8643543188091677687.png",
            },
            {
                "symbol": "bhp",
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-03-30T04:16:13+10:00",
                "resolution": "1d",
                "search_period": 20,
                "notify_method": "pushover",
                "notify_recipient": "some-pushover-app-1",
                "target_ta_confidence": 3,
                "ta_algo": {"stoch": None},
                "ta_analysis": {"confidence": 10, "ta_data": None},
                "graph_url": "Algo not implemented",
            },
            {
                "symbol": "bhp",
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-03-30T04:16:13+10:00",
                "resolution": "1d",
                "search_period": 20,
                "notify_method": "pushover",
                "notify_recipient": "some-pushover-app-1",
                "target_ta_confidence": 3,
                "ta_algo": {"accumulation-distribution": None},
                "ta_analysis": {"confidence": 10, "ta_data": None},
                "graph_url": "Algo not implemented",
            },
        ],
    }
    lambda_handler(payload, None)
