# external packages
import logging
from pushover import init, Client
from slack_sdk import WebClient

# my modules
from inotification_service import INotificationService
from iparameter_store import IParameterStore


log_wp = logging.getLogger(
    "notification_services"
)  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("notification_services.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)


class Pushover(INotificationService):
    def __init__(
        self,
        store: IParameterStore,
        back_testing: bool = False,
        real_money_trading: bool = False,
    ):
        self.back_testing = back_testing

        if not back_testing:
            pushover_api_key = (
                store.get_parameter(
                    Name="/tabot/pushover/api_key", WithDecryption=False
                )
                .get("Parameter")
                .get("Value")
            )

            pushover_user_key = (
                store.get_parameter(
                    Name="/tabot/pushover/user_key", WithDecryption=False
                )
                .get("Parameter")
                .get("Value")
            )

            # init(pushover_user_key, api_token=pushover_api_key)
            self.client = Client(pushover_user_key, api_token=pushover_api_key)

    def send(self, message: str, subject: str = None) -> bool:
        if self.back_testing:
            log_wp.debug(f"Notification: {message}")
        else:
            if not subject:
                subject = "tabot notification"

            self.client.send_message(
                message=message,
                title=subject,
            )


class Slack(INotificationService):
    def __init__(
        self,
        store: IParameterStore,
        back_testing: bool = False,
        real_money_trading: bool = False,
    ):
        self.back_testing = back_testing

        if back_testing == False:
            slack_token = (
                store.get_parameter(Name="/tabot/slack/bot_key", WithDecryption=True)
                .get("Parameter")
                .get("Value")
            )
            self.slack_announcements_channel = (
                store.get_parameter(
                    Name="/tabot/paper/slack/announcements_channel",
                    WithDecryption=False,
                )
                .get("Parameter")
                .get("Value")
            )
            self.client = WebClient(token=slack_token)
        elif real_money_trading:
            slack_token = (
                store.get_parameter(Name="/tabot/slack/bot_key", WithDecryption=True)
                .get("Parameter")
                .get("Value")
            )
            self.slack_announcements_channel = (
                store.get_parameter(
                    Name="/tabot/prod/slack/announcements_channel", WithDecryption=False
                )
                .get("Parameter")
                .get("Value")
            )
            self.client = WebClient(token=slack_token)

    def send(self, message: str, subject: str = None) -> bool:
        if self.back_testing:
            log_wp.debug(f"Notification: {message}")
        else:
            self.client.chat_postMessage(
                channel=self.slack_announcements_channel,
                text=message,
            )


"""
import parameter_stores

ssm = parameter_stores.ssm()
p = Pushover(store=ssm)
p.send("abc", "123")

slack = Slack(store=ssm)
slack.send("abc")

print("banana")
"""
