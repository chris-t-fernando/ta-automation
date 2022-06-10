# external packages
from datetime import datetime
import logging
from re import L
import time

# my modules
from broker_alpaca import AlpacaAPI
from broker_swyftx import SwyftxAPI
from broker_back_test import BackTestAPI
from macd_config import MacdConfig
from macd_worker import (
    MacdWorker,
    NO_POSITION_TAKEN,
    BUY_LIMIT_ORDER_ACTIVE,
    BUY_PRICE_MET,
    POSITION_TAKEN,
    TAKING_PROFIT,
    STOP_LOSS_ACTIVE,
)
from tabot_rules import TABotRules
import utils

log_wp = logging.getLogger("macd")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("macd.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)

class StrategyTides:
    def __init__(self, symbols: list, config: MacdConfig):
        self.config = config
        self.store = config.store
        self.interval = config.interval
        self.interval_delta, __ = utils.get_interval_settings(self.interval)
        self.back_testing_balance = config.back_testing_balance
        self.real_money_trading = config.production_run
        self.bot_telemetry = config.bot_telemetry
        self.notification_service = config.notification_service
        self.rules = TABotRules(store=self.config.store, rules_path=self.config.path_rules, state_path=self.config.path_state)



#get_date_range()
#process_bars()