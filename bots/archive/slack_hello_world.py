import logging
import os

# Import WebClient from Python SDK (github.com/slackapi/python-slack-sdk)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import boto3

ssm = boto3.client("ssm")
slack_token = (
    ssm.get_parameter(Name="/tabot/slack/bot_key", WithDecryption=True)
    .get("Parameter")
    .get("Value")
)
slack_heartbeat_channel = (
    ssm.get_parameter(Name="/tabot/slack/heartbeat_channel", WithDecryption=False)
    .get("Parameter")
    .get("Value")
)
slack_announcements_channel = (
    ssm.get_parameter(Name="/tabot/slack/announcements_channel", WithDecryption=False)
    .get("Parameter")
    .get("Value")
)


# WebClient instantiates a client that can call API methods
# When using Bolt, you can use either `app.client` or the `client` passed to listeners.
client = WebClient(token=slack_token)


logger = logging.getLogger(__name__)
# ID of channel you want to post message to
channel_id = "C03BX75AQ6Q"

try:
    # Call the conversations.list method using the WebClient
    result = client.chat_postMessage(
        channel=channel_id,
        text="Hello world!"
        # You could also use a blocks[] array to send richer content
    )
    # Print result, which includes information about the message (like TS)
    print(result)

except SlackApiError as e:
    print(f"Error: {e}")
