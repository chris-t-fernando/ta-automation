import boto3
import json
import random


class StepFunctionNotFoundException(Exception):
    ...


def get_step_function(client):
    # find the TA-analysis step function via tagging
    ta_automation_machine = None
    machines = client.list_state_machines()

    for machine in machines["stateMachines"]:
        tags = client.list_tags_for_resource(resourceArn=machine["stateMachineArn"])

        for tag in tags["tags"]:
            if tag["key"] == "aws:cloudformation:stack-name":
                if tag["value"] == "ta-automation":
                    ta_automation_machine = machine
                    print(
                        f'Found ta-automation step function: {machine["stateMachineArn"]}'
                    )
                    break

    if ta_automation_machine == None:
        raise StepFunctionNotFoundException(
            "Unable to find step function with tag aws:cloudformation:stack-name=ta-automation"
        )

    return ta_automation_machine


def process_request(respond, body):
    client = boto3.client("stepfunctions")
    ta_automation_machine = get_step_function(client)

    job = {
        "jobs": [
            {
                "symbol": "btc-aud",
                "date_from": "2022-01-01T04:16:13+10:00",
                "date_to": "2022-03-30T04:16:13+10:00",
                "ta_algos": [
                    {
                        "awesome-oscillator": {
                            "strategy": "saucer",
                            "direction": "bullish",
                        }
                    },
                    {"stoch": None},
                    {"accumulation-distribution": None},
                ],
                "resolution": "1d",
                "search_period": 20,
                "notify_method": "pushover",
                "notify_recipient": "ucYyQ2tLc9CqDUqGXVpZvKiyuCDx9x",
                "target_ta_confidence": 3,
            }
        ]
    }

    state_machine_invocation = client.start_execution(
        stateMachineArn=ta_automation_machine["stateMachineArn"],
        name=body["trigger_id"],
        input=json.dumps(job),
    )

    finished = False
    while not finished:
        job_execution = client.describe_execution(
            executionArn=state_machine_invocation["executionArn"]
        )

        if job_execution["status"] == "SUCCEEDED":
            # respond(f"TA job finished!")
            finished = True
            break

    state_machine_output = json.loads(job_execution["output"])

    response_message = ""
    for analysis in state_machine_output["ta_analyses"]:
        response_message += f'{analysis["graph_url"]}\n'

    print(response_message)


def respond(string):
    print(string)


body = {"trigger_id": str(random.randint(1, 100000))}

process_request(None, body)
