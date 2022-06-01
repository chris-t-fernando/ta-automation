# external packages
import logging
from pushover import Pushover
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
        api_key:str,
        user_key:str,
    ):
        #self.client = Client(user_key, api_token=api_key)
        self.client = Pushover(api_key)
        self.client.user(user_key)

    def send(self, message: str,subject: str = None) -> bool:
        if not subject:
            subject = "tabot notification"

        po_message = self.client.msg(message)
        po_message.set("title", subject)
        self.client.send(po_message)


# finish changing get parameter and put parameter
# finish moving from two bools to just reading run_type
# generally just keep implementing new config objects


class Slack(INotificationService):
    def __init__(
        self,
        bot_key:str,
        channel:str
    ):
        self.channel = channel
        self.client = WebClient(token=bot_key)

    def send(self, message: str, subject: str = None) -> bool:
        self.client.chat_postMessage(
            channel=self.channel,
            text=message,
        )

class LocalEcho(INotificationService):
    def __init__(
        self,
        bot_key=None,
        channel=None
    ):
        ...

    def send(self, message: str, subject: str = None) -> bool:
        print(f"LocalEcho: {message}")

"""
import parameter_stores

ssm = parameter_stores.ssm()
p = Pushover(store=ssm)
p.send("abc", "123")

slack = Slack(store=ssm)
slack.send("abc")

print("banana")
"""
