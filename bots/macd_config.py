# external packages
import logging
import yfinance as yf

# my modules
from bot_telemetry import BotTelemetry
from parameter_stores import Ssm, BackTestStore
from iparameter_store import IParameterStore
import notification_services

log_wp = logging.getLogger("macd_config")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("macd_config.log")
log_wp.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(fhdlr)
log_wp.addHandler(hdlr)


class MacdConfig:
    _PREFIX = "tabot"
    PATH_ORDER_SIZE = f"/{_PREFIX}/order_size"
    SAVED_SYMBOL_DATA_BUCKET = "mfers-tabot"
    SAVED_SYMBOL_KEY_BASE = "symbol_data/"
    PATH_PAPER_ALPACA_API_KEY = f"/{_PREFIX}/paper/alpaca/api_key"
    PATH_PAPER_ALPACA_SECURITY_KEY = f"/{_PREFIX}/paper/alpaca/security_key"
    PAPER_HEARTBEAT = f"/{_PREFIX}/paper/heartbeat"
    PAPER_HEARTBEAT_RESULT = f"/{_PREFIX}/paper/heartbeat_result"
    PATH_PAPER_SLACK_ANNOUNCEMENTS_CHANNEL = f"/{_PREFIX}/paper/slack/announcements_channel"
    PATH_PAPER_SLACK_HEARTBEAT_CHANNEL = f"/{_PREFIX}/paper/slack/heartbeat_channel"
    PATH_PAPER_SWYFTX_ACCESS_TOKEN = f"/{_PREFIX}/paper/swyftx/access_token"
    PATH_PAPER_SWYFTX_API_KEY = f"/{_PREFIX}/paper/swyftx/api_key"
    PAPER_RULES = f"/{_PREFIX}/paper/rules/5m"
    PAPER_STATE = f"/{_PREFIX}/paper/state"
    PAPER_TELEMETRY_S3_BUCKET = "mfers-tabot"
    PAPER_TELEMETRY_S3_PREFIX = "telemetry/backtests/"
    PATH_PROD_ALPACA_API_KEY = f"/{_PREFIX}/prod/alpaca/api_key"
    PATH_PROD_ALPACA_SECURITY_KEY = f"/{_PREFIX}/prod/alpaca/security_key"
    PROD_HEARTBEAT = f"/{_PREFIX}/prod/heartbeat"
    PROD_HEARTBEAT_RESULT = f"/{_PREFIX}/prod/heartbeat_result"
    PATH_PROD_SLACK_ANNOUNCEMENTS_CHANNEL = f"/{_PREFIX}/prod/slack/announcements_channel"
    PATH_PROD_SLACK_HEARTBEAT_CHANNEL = f"/{_PREFIX}/prod/slack/heartbeat_channel"
    PATH_PROD_SWYFTX_ACCESS_TOKEN = f"/{_PREFIX}/prod/swyftx/access_token"
    PATH_PROD_SWYFTX_API_KEY = f"/{_PREFIX}/prod/swyftx/api_key"
    PROD_RULES = f"/{_PREFIX}/prod/rules/5m"
    PROD_STATE = f"/{_PREFIX}/prod/state"
    PROD_TELEMETRY_S3_BUCKET = "mfers-tabot"
    PROD_TELEMETRY_S3_PREFIX = "telemetry/backtests/"
    PATH_PUSHOVER_API_KEY = f"/{_PREFIX}/pushover/api_key"
    PATH_PUSHOVER_USER_KEY = f"/{_PREFIX}/pushover/user_key"
    PATH_SLACK_BOT_KEY = f"/{_PREFIX}/slack/bot_key"
    PATH_SLACK_SIGNING_TOKEN = f"/{_PREFIX}/slack/signing_token"

    order_size: float
    saved_symbol_data_bucket: str
    saved_symbol_key_base: str
    path_alpaca_api_key: str
    path_alpaca_security_key: str
    path_heartbeat: str
    path_heartbeat_result: str
    path_slack_announcements_channel: str
    path_slack_heartbeat_channel: str
    path_pushover_api_key: str
    path_pushover_user_key: str
    path_slack_bot_key: str
    path_slack_signing_token: str
    path_swyftx_access_token: str
    path_swyftx_api_key: str
    path_rules: str
    path_state: str
    telemetry_s3_prefix: str
    telemetry_s3_bucket: str
    back_testing: bool = False
    paper_testing: bool = False
    production_run: bool
    back_testing_balance: float
    back_testing_override_broker: bool = False
    back_testing_skip_bar_update: bool = False
    interval: str = "5m"
    bot_telemetry: BotTelemetry
    market_data_source = None
    symbols: list
    path_notification_service: str = "slack"
    store: IParameterStore = None
    run_type: str

    def __init__(self, args):
        self.interval = args.interval
        self.run_type = args.run_type
        self.market_data_source = yf
        self.symbol_group = args.symbols
        self.buy_market = args.buy_market
        self.production_run = False
        self.paper_testing = False
        self.back_testing = False
        self.back_testing_balance = None
        self.saved_symbol_data_bucket = self.SAVED_SYMBOL_DATA_BUCKET
        self.saved_symbol_key_base = f"{self.SAVED_SYMBOL_KEY_BASE}{self.interval}/"

        if args.run_type == "prod":
            self.path_order_size = self.PATH_ORDER_SIZE
            self.path_alpaca_api_key = self.PATH_PROD_ALPACA_API_KEY
            self.path_alpaca_security_key = self.PATH_PROD_ALPACA_SECURITY_KEY
            self.heartbeat = self.PROD_HEARTBEAT
            self.heartbeat_result = self.PROD_HEARTBEAT_RESULT
            self.path_slack_announcements_channel = self.PATH_PROD_SLACK_ANNOUNCEMENTS_CHANNEL
            self.path_slack_heartbeat_channel = self.PATH_PROD_SLACK_HEARTBEAT_CHANNEL
            self.path_pushover_api_key = self.PATH_PUSHOVER_API_KEY
            self.path_pushover_user_key = self.PATH_PUSHOVER_USER_KEY
            self.path_slack_bot_key = self.PATH_SLACK_BOT_KEY
            self.path_slack_signing_token = self.PATH_SLACK_SIGNING_TOKEN
            self.path_swyftx_access_token = self.PATH_PROD_SWYFTX_ACCESS_TOKEN
            self.path_swyftx_api_key = self.PATH_PROD_SWYFTX_API_KEY
            self.path_rules = self.PROD_RULES
            self.path_state = self.PROD_STATE
            self.telemetry_s3_prefix = self.PROD_TELEMETRY_S3_PREFIX
            self.telemetry_s3_bucket = self.PROD_TELEMETRY_S3_BUCKET
            self.production_run = True

            self.store = Ssm()

        elif args.run_type == "paper":
            self.path_order_size = self.PATH_ORDER_SIZE
            self.path_alpaca_api_key = self.PATH_PAPER_ALPACA_API_KEY
            self.path_alpaca_security_key = self.PATH_PAPER_ALPACA_SECURITY_KEY
            self.heartbeat = self.PAPER_HEARTBEAT
            self.heartbeat_result = self.PAPER_HEARTBEAT_RESULT
            self.path_slack_announcements_channel = self.PATH_PAPER_SLACK_ANNOUNCEMENTS_CHANNEL
            self.path_slack_heartbeat_channel = self.PATH_PAPER_SLACK_HEARTBEAT_CHANNEL
            self.path_pushover_api_key = self.PATH_PUSHOVER_API_KEY
            self.path_pushover_user_key = self.PATH_PUSHOVER_USER_KEY
            self.path_slack_bot_key = self.PATH_SLACK_BOT_KEY
            self.path_slack_signing_token = self.PATH_SLACK_SIGNING_TOKEN
            self.path_swyftx_access_token = self.PATH_PAPER_SWYFTX_ACCESS_TOKEN
            self.path_swyftx_api_key = self.PATH_PAPER_SWYFTX_API_KEY
            self.path_rules = self.PAPER_RULES
            self.path_state = self.PAPER_STATE
            self.telemetry_s3_prefix = self.PROD_TELEMETRY_S3_PREFIX
            self.telemetry_s3_bucket = self.PROD_TELEMETRY_S3_BUCKET
            self.paper_testing = True

            # TODO uncomment this
            # self.store = Ssm()

            self.store = BackTestStore()
            self.store._bootstrap(
                self.path_alpaca_api_key,
                self.path_alpaca_security_key,
                self.path_slack_bot_key,
                self.path_slack_announcements_channel,
                self.path_pushover_api_key,
                self.path_pushover_user_key,
                self.path_slack_signing_token,
                self.path_slack_heartbeat_channel,
                self.path_swyftx_access_token,
                self.path_swyftx_api_key,
                self.path_order_size,
            )

        elif args.run_type == "back_test":
            # I'm lazy and haven't made back_test paths yet so just borrow from paper
            self.path_order_size = self.PATH_ORDER_SIZE
            self.path_alpaca_api_key = self.PATH_PAPER_ALPACA_API_KEY
            self.path_alpaca_security_key = self.PATH_PAPER_ALPACA_SECURITY_KEY
            self.heartbeat = self.PAPER_HEARTBEAT
            self.heartbeat_result = self.PAPER_HEARTBEAT_RESULT
            self.path_slack_announcements_channel = self.PATH_PAPER_SLACK_ANNOUNCEMENTS_CHANNEL
            self.path_slack_heartbeat_channel = self.PATH_PAPER_SLACK_HEARTBEAT_CHANNEL
            self.path_pushover_api_key = self.PATH_PUSHOVER_API_KEY
            self.path_pushover_user_key = self.PATH_PUSHOVER_USER_KEY
            self.path_slack_bot_key = self.PATH_SLACK_BOT_KEY
            self.path_slack_signing_token = self.PATH_SLACK_SIGNING_TOKEN
            self.path_swyftx_access_token = self.PATH_PAPER_SWYFTX_ACCESS_TOKEN
            self.path_swyftx_api_key = self.PATH_PAPER_SWYFTX_API_KEY
            self.path_rules = self.PAPER_RULES
            self.path_state = self.PAPER_STATE
            self.telemetry_s3_prefix = self.PAPER_TELEMETRY_S3_PREFIX
            self.telemetry_s3_bucket = self.PAPER_TELEMETRY_S3_BUCKET
            self.back_testing = True
            self.back_testing_balance = args.back_testing_balance
            self.back_testing_override_broker = args.back_testing_override_broker
            self.back_testing_skip_bar_update = args.back_testing_skip_bar_update

            self.store = BackTestStore()
            self.store._bootstrap(
                self.path_alpaca_api_key,
                self.path_alpaca_security_key,
                self.path_slack_bot_key,
                self.path_slack_announcements_channel,
                self.path_pushover_api_key,
                self.path_pushover_user_key,
                self.path_slack_signing_token,
                self.path_slack_heartbeat_channel,
                self.path_swyftx_access_token,
                self.path_swyftx_api_key,
            )

        else:
            raise ValueError(
                f"Unknown run_type {args.run_type}. Must be either 'prod', 'paper', or 'back_test'"
            )

        self.bot_telemetry = BotTelemetry(back_testing=self.back_testing)

        self.pushover_api_key = self.store.get(path=self.path_pushover_api_key)
        self.pushover_user_key = self.store.get(path=self.path_pushover_user_key)
        self.slack_bot_key = self.store.get(path=self.path_slack_bot_key)
        self.slack_signing_token = self.store.get(path=self.path_slack_signing_token)
        self.slack_heartbeat_channel = self.store.get(path=self.path_slack_heartbeat_channel)
        self.swyftx_access_token = self.store.get(path=self.path_swyftx_access_token)
        self.swyftx_api_key = self.store.get(path=self.path_swyftx_api_key)
        self.alpaca_api_key = self.store.get(path=self.path_alpaca_api_key)
        self.alpaca_security_key = self.store.get(path=self.path_alpaca_security_key)
        self.slack_announcements_channel = self.store.get(
            path=self.path_slack_announcements_channel
        )
        self.order_size = float(self.store.get(path=self.path_order_size))

        if self.back_testing:
            self.notification_service = notification_services.LocalEcho()

        elif args.notification_service == "pushover":
            self.notification_service = notification_services.Pushover(
                api_key=self.pushover_api_key, user_key=self.pushover_user_key
            )

        elif args.notification_service == "slack":
            self.notification_service = notification_services.Slack(
                bot_key=self.slack_bot_key, channel=self.slack_announcements_channel
            )
