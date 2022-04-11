import logging
import time
import json
import boto3
from slack_bolt import App
from slack_bolt.adapter.aws_lambda import SlackRequestHandler

# set up input keys
mandatory_keys = {"symbol", "date_from", "algos"}
optional_keys = {
    "date_to",
    "resolution",
    "search_period",
    "notify_method",
    "notify_recipient",
    "target_ta_confidence",
}


# used to raise an exception when we can't find the ta-automation step function
class StepFunctionNotFoundException(Exception):
    ...


# process_before_response must be True when running on FaaS
app = App(
    process_before_response=True,
    #    token=bot_token,
    #    signing_secret=signing_token,
)


@app.middleware  # or app.use(log_request)
def log_request(logger, body, next):
    logger.debug(body)
    return next()


def respond_to_slack_within_3_seconds(body, ack):
    # i don't understand this block of boilerplate at all, given that the execution proceeds regardless of usage
    #    if body.get("text") is None:
    #        ack(f":x: Usage: {command} (description here)")
    #    else:
    #        title = body["text"]

    # for whatever reason I can't find a way of killing the call here, so I'll have to let it through to the state machine and let it fail
    ack(f"Just a sec while I check your inputs")


def get_step_function(client):
    # find the TA-analysis step function via tagging
    # client is an instance of boto3 stepfunctions client
    machines = client.list_state_machines()

    # machines is a list of all of the state machines, so need to iterate through looking for the right one
    for machine in machines["stateMachines"]:
        # grab the tags for this state machine
        tags = client.list_tags_for_resource(resourceArn=machine["stateMachineArn"])

        # iterate through all tags to the find the one we want. i could use if "tag" in tags.keys() but whatever
        for tag in tags["tags"]:
            if tag["key"] == "aws:cloudformation:stack-name":
                if tag["value"] == "ta-automation":
                    return machine

    # if we got here, we failed
    raise StepFunctionNotFoundException(
        "Unable to find step function with tag aws:cloudformation:stack-name=ta-automation"
    )


def do_ta_usage():
    usage = "Mandatory parameters: symbol date_from and algos\n"
    usage += "Optional parameters: date_to resolution search_period notify_method and notify_recipient\n"
    usage += "Examples:\n"
    usage += "/ta symbol=abc date_from=2022-01-01T04:16:13+10:00 algos=awesome-oscillator,stoch target_ta_confidence=7\n"
    usage += "/ta symbol=abc date_from=2022-01-01T04:16:13+10:00 date_to=2022-01-05T12:12:12+10:00 algos=accumulation-distribution target_ta_confidence=7 resolution=5d search_period=15\n"
    return usage


def da_ta_validate(text):
    parameters = text.split()

    valid_parameters = {}
    found_keys = set()
    errors = ""
    error_found = False

    valid_keys = mandatory_keys.union(optional_keys)

    for parameter in parameters:
        split_parameter = parameter.split("=")
        if len(split_parameter) != 2:
            errors += f"Input parameter set without value assignment: {split_parameter[0]}=what?\n"
            error_found = True
        else:
            valid_parameters[split_parameter[0]] = split_parameter[1]
            found_keys.add(split_parameter[0])

    # see if there's an invalid parameter specified
    if len(found_keys.difference(valid_keys)) > 0:
        errors += f"Invalid key specified: {str(found_keys.difference(valid_keys))}\n"
        error_found = True

    # if a mandatory key was omitted
    if len(mandatory_keys.difference(found_keys)) > 0:
        errors += (
            f"Mandatory key missing: {str(mandatory_keys.difference(found_keys))}\n"
        )
        error_found = True

    return error_found, errors, valid_parameters


# execute the slash command
def do_ta(respond, body):
    # validate the inputs
    if body.get("text") is None:
        # no parameters were given
        respond(do_ta_usage())
    else:
        # validate the input
        errors_found, error_messages, valid_parameters = da_ta_validate(body["text"])

        if errors_found:
            # input is bad - missing or unknown parameters
            respond(error_messages)
            respond(do_ta_usage())

        else:
            # execute the step function and return the output

            # first we need to format the step machine request
            # expand algos
            algo_list = []
            # first check if more than one was specified
            if "," in valid_parameters["algos"]:
                input_algos = valid_parameters["algos"].split(",")
                for this_algo in input_algos:
                    algo_list.append({this_algo: None})
            else:
                algo_list.append(valid_parameters["algos"])

            # job structure
            # mandatories first
            ta_job = {
                "jobs": [
                    {
                        "symbol": valid_parameters["symbol"],
                        "date_from": valid_parameters["date_from"],
                        "ta_algos": algo_list,
                    }
                ]
            }

            for optional in optional_keys:
                if valid_parameters.get(optional) != None:
                    ta_job["jobs"][0][str(optional)] = valid_parameters[str(optional)]

            respond(
                f"Okay your inputs are all good, now sending it for processing. This might take 15 seconds or more so be patient please"
            )
            # find the step machine so we can get its arn
            ta_automation_machine = get_step_function(client)

            # call the state machine
            state_machine_invocation = client.start_execution(
                stateMachineArn=ta_automation_machine["stateMachineArn"],
                name=body["trigger_id"],
                input=json.dumps(ta_job),
            )

            # loop til we get the state machine response back
            while True:
                time.sleep(5)

                # check if the state machine is still running
                job_execution = client.describe_execution(
                    executionArn=state_machine_invocation["executionArn"]
                )

                # if its done, then we can bust out
                if job_execution["status"] == "SUCCEEDED":
                    break

            # grab the output and load it as json
            state_machine_output = json.loads(job_execution["output"])

            # start responding
            response_message = (
                f'Finished analysis for {state_machine_output["job"]["symbol"]}:\n'
            )

            for analysis in state_machine_output["ta_analyses"]:
                response_message += f' - {str(list(analysis["ta_algo"].keys())[0])} {analysis["ta_analysis"]["confidence"]}/10 confidence <{analysis["graph_url"]}|Graph link>\n'

            respond(response_message)


# used by all functions so its a global
client = boto3.client("stepfunctions")

# register commands and handlers
command = "/ta"
app.command(command)(ack=respond_to_slack_within_3_seconds, lazy=[do_ta])

SlackRequestHandler.clear_all_log_handlers()
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


def lambda_handler(event, context):
    print("got here1")
    slack_handler = SlackRequestHandler(app=app)
    return slack_handler.handle(event, context)
