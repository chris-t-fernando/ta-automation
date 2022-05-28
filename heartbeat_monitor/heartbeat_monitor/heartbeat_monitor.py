from pushover import init, Client
from slack_sdk import WebClient
from pandas import Timestamp
import boto3
from dateutil.relativedelta import relativedelta
from datetime import datetime


def setup_slack(ssm):
    slack_token = (
        ssm.get_parameter(Name="/tabot/slack/bot_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )

    return WebClient(token=slack_token)


def setup_pushover(ssm):
    pushover_api_key = (
        ssm.get_parameter(Name="/tabot/pushover/api_key", WithDecryption=False)
        .get("Parameter")
        .get("Value")
    )

    pushover_user_key = (
        ssm.get_parameter(Name="/tabot/pushover/user_key", WithDecryption=False)
        .get("Parameter")
        .get("Value")
    )

    # init(pushover_user_key, api_token=pushover_api_key)
    return Client(pushover_user_key, api_token=pushover_api_key)


def send_message(slack_client, pushover_client, message, channel):
    # send messages
    if channel[0:12] == "/tabot/prod/":
        title = "PROD HEARTBEAT"
    else:
        title = "Paper heartbeat"
    print(message)
    # pushover_client.send_message(
    #    message=message,
    #    title=title,
    # )

    # slack_client.chat_postMessage(
    #    text=message,
    #    channel=channel,
    # )


def valid_heartbeat(last_heartbeat_str: str):
    try:
        last_heartbeat = Timestamp(last_heartbeat_str)
    except Exception as e:
        return False

    ten_minutes = relativedelta(minutes=10)
    ten_minutes_ago = datetime.now() - ten_minutes

    if last_heartbeat < ten_minutes_ago:
        # last heartbeat was longer than 10 minutes ago
        return False

    return True


def lambda_handler(event, context):
    valid_heartbeat_results = ["up", "down"]
    ssm = boto3.client("ssm")
    slack_client = setup_slack(ssm)
    pushover_client = setup_pushover(ssm)

    slack_paper_channel = (
        ssm.get_parameter(
            Name="/tabot/paper/slack/heartbeat_channel",
            WithDecryption=False,
        )
        .get("Parameter")
        .get("Value")
    )

    slack_prod_channel = (
        ssm.get_parameter(
            Name="/tabot/prod/slack/heartbeat_channel",
            WithDecryption=False,
        )
        .get("Parameter")
        .get("Value")
    )

    paper_heartbeat = (
        ssm.get_parameter(
            Name="/tabot/paper/heartbeat",
            WithDecryption=False,
        )
        .get("Parameter")
        .get("Value")
    )

    paper_heartbeat_result = (
        ssm.get_parameter(
            Name="/tabot/paper/heartbeat_result",
            WithDecryption=False,
        )
        .get("Parameter")
        .get("Value")
    )

    prod_heartbeat = (
        ssm.get_parameter(
            Name="/tabot/prod/heartbeat",
            WithDecryption=False,
        )
        .get("Parameter")
        .get("Value")
    )

    prod_heartbeat_result = (
        ssm.get_parameter(
            Name="/tabot/prod/heartbeat_result",
            WithDecryption=False,
        )
        .get("Parameter")
        .get("Value")
    )

    paper_heartbeat_found = valid_heartbeat(paper_heartbeat)

    if paper_heartbeat_found and paper_heartbeat_result == "down":
        # its come up
        send_message(
            slack_client,
            pushover_client,
            message="Paper heartbeat has started",
            channel=slack_paper_channel,
        )
        ssm.put_parameter(
            Name="/tabot/paper/heartbeat_result", Value="up", Overwrite=True
        )
    elif paper_heartbeat_found and paper_heartbeat_result == "up":
        # its still up, do nothing
        ...
    elif not paper_heartbeat_found and paper_heartbeat_result == "down":
        # its still down, do nothing
        ...
    elif not paper_heartbeat_found and paper_heartbeat_result == "up":
        # it is not up, but it was before
        send_message(
            slack_client,
            pushover_client,
            message="Paper heartbeat lost!",
            channel=slack_paper_channel,
        )
        ssm.put_parameter(
            Name="/tabot/paper/heartbeat_result", Value="down", Overwrite=True
        )
    elif paper_heartbeat_result not in valid_heartbeat_results:
        send_message(
            slack_client,
            pushover_client,
            message=f"Weird value found in last heartbeat result - found {paper_heartbeat_result}",
            channel=slack_paper_channel,
        )

    prod_heartbeat_found = valid_heartbeat(prod_heartbeat)
    if prod_heartbeat_found and prod_heartbeat_result == "down":
        # its come up
        send_message(
            slack_client,
            pushover_client,
            message="Prod heartbeat has started",
            channel=slack_prod_channel,
        )
        ssm.put_parameter(
            Name="/tabot/prod/heartbeat_result", Value="up", Overwrite=True
        )
    elif prod_heartbeat_found and prod_heartbeat_result == "up":
        # its still up, do nothing
        ...
    elif not prod_heartbeat_found and prod_heartbeat_result == "down":
        # its still down, do nothing
        ...
    elif not prod_heartbeat_found and prod_heartbeat_result == "up":
        # it is not up, but it was before
        send_message(
            slack_client,
            pushover_client,
            message="Prod heartbeat lost!",
            channel=slack_prod_channel,
        )
        ssm.put_parameter(
            Name="/tabot/prod/heartbeat_result", Value="down", Overwrite=True
        )
    elif prod_heartbeat_result not in valid_heartbeat_results:
        send_message(
            slack_client,
            pushover_client,
            message=f"Weird value found in last heartbeat result - found {prod_heartbeat_result}",
            channel=slack_prod_channel,
        )

    return True


if __name__ == "__main__":
    lambda_handler(None, None)
