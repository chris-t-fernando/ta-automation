import logging
import time

from slack_bolt import App
from slack_bolt.adapter.aws_lambda import SlackRequestHandler
from slack_bolt.oauth.oauth_settings import OAuthSettings

ACCESS_TOKEN = "xapp-1-A03B0H0RQV7-3376911024913-051a12aad68a095409edcd0e4112d5d2a644d053d13d921711a2dd6fdc52ff77"
bottoken = "xoxb-3352336312436-3359152407141-yj3y8szQwjCm2UqrTgZqePXF"
client_id = "3352336312436.3374578874993"
client_secret = "62a2bc44d941b0ca490e24e24775b66c"
signing = "28831ea678df078862585c2312cdccb5"

# process_before_response must be True when running on FaaS
app = App(
    process_before_response=True,
    token="xoxb-3352336312436-3359152407141-yj3y8szQwjCm2UqrTgZqePXF",
    signing_secret = signing
)


@app.middleware  # or app.use(log_request)
def log_request(logger, body, next):
    logger.debug(body)
    return next()

def respond_to_slack_within_3_seconds(body, ack):
    if body.get("text") is None:
        ack(f":x: Usage: {command} (description here)")
    else:
        title = body["text"]
        ack(f"Accepted! (task: {title})")


def process_request(respond, body):
    time.sleep(5)
    title = body["text"]
    respond(f"Completed! (task: {title})")

@app.command("/submit-job")
def command(say, ack):
    ack()
    say(text="sky")


command = "/submit-job"
app.command(command)(ack=respond_to_slack_within_3_seconds, lazy=[process_request])

SlackRequestHandler.clear_all_log_handlers()
logging.basicConfig(format="%(asctime)s %(message)s", level=logging.DEBUG)


def lambda_handler(event, context):
    slack_handler = SlackRequestHandler(app=app)
    return slack_handler.handle(event, context)
