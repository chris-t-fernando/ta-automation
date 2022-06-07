# external packages
from datetime import datetime
import logging
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


class MacdBot:
    jobs = None

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

        if config.back_testing:
            # override broker to back_test
            if config.back_testing_override_broker:
                for s in symbols:
                    s["api"] = "back_test"

        # get brokers and then set them up
        self.api_list = []
        for api in symbols:
            self.api_list.append(api["api"])
            log_wp.log(9, f"Found symbol {api['symbol']} using broker {api['api']}")

        # configure brokers
        self.api_list = list(set(self.api_list))
        self.api_dict = self.setup_brokers()

        # set up individual symbols
        self.symbols = {}
        for s in symbols:
            start_time = time.time()
            new_symbol = MacdWorker(
                symbol=s["symbol"],
                api=self.api_dict[s["api"]],
                rules=self.rules,
                config=config
            )
            key = s["api"] + s["symbol"]
            if new_symbol._init_complete:
                self.symbols[key] = new_symbol
                log_wp.info(
                    f'{s["symbol"]} ({s["api"]}): Set up complete in {round(time.time() - start_time,1)}s'
                )
            else:
                log_wp.error(
                    f'{s["symbol"]}: Failed to set up this symbol. Skipping'
                )

    def setup_brokers(self):
        # use a set to drop any duplicates
        api_set = set(self.api_list)
        api_dict = {}

        for api in api_set:
            start_time = time.time()
            if api == "back_test":
                api_dict[api] = BackTestAPI(
                    back_testing=self.config.back_testing,
                    back_testing_balance=self.config.back_testing_balance,
                )
                break

            elif api == "swyftx":
                api_dict[api] = SwyftxAPI(
                    access_token=self.config.swyftx_access_token,
                    back_testing=self.config.back_testing,
                    back_testing_balance=self.back_testing_balance,
                    real_money_trading=self.real_money_trading,
                )

            elif api == "alpaca":
                api_dict[api] = AlpacaAPI(
                    alpaca_key_id=self.config.alpaca_api_key,
                    alpaca_secret_key=self.config.alpaca_security_key,
                    back_testing=self.config.back_testing,
                    back_testing_balance=self.back_testing_balance,
                    real_money_trading=self.real_money_trading,
                )

            else:
                raise ValueError(f"Unknown broker specified {api}")

            log_wp.debug(f"Set up {api} in {round(time.time() - start_time,1)}s")

        return api_dict

    def get_date_range(self):
        start_date = None
        end_date = None
        latest_start = None

        if len(self.symbols) == 0:
            return None, None

        for s in self.symbols:
            # if this is the first symbol we're assessing
            if not start_date:
                start_date = self.symbols[s].bars.index.min()
                end_date = self.symbols[s].bars.index.max()
                latest_start = start_date
                latest_symbol = s
            else:
                if start_date > self.symbols[s].bars.index.min():
                    start_date = self.symbols[s].bars.index.min()

                if end_date < self.symbols[s].bars.index.max():
                    end_date = self.symbols[s].bars.index.max()

            # used when back testing to make sure we don't try sampling index -200
            # we're looking to see which symbol starts the LATEST
            # if latest_start > self.symbols[s].bars.index.min():
            if self.symbols[s].bars.index.min() > latest_start:
                latest_start = self.symbols[s].bars.index.min()
                latest_symbol = s

        if self.config.back_testing:
            latest_start_position = self.symbols[latest_symbol].bars.index.get_loc(
                latest_start
            )
            back_test_start_position = latest_start_position + 250

            start_date = self.symbols[latest_symbol].bars.index[
                back_test_start_position
            ]

        log_wp.log(
            9,
            f"Range of all symbol bars: latest start date {latest_start}"
            f"(for {latest_symbol}), latest end date {end_date}",
        )
        return start_date, end_date

    def process_bars(self):
        # update the data
        for s in self.symbols:
            log_wp.log(9, f"{s}: Updating bar data")
            self.symbols[s].update_bars()

        # find the oldest and newest records we're working with
        data_start_date, data_end_date = self.get_date_range()

        # define our starting point - if we're backtesting then go from the beginning of the data
        # if we're running live, then just process the most recent data
        if self.config.back_testing:
            current_record = data_start_date
        else:
            current_record = data_end_date

        # initialise state for each symbol - check which state each symbol is in, read open order numbers etc
        # self.set_state()

        self.bot_telemetry.next_cycle(timestamp=datetime.now())

        # iterate through the data until we reach the end
        while current_record <= data_end_date:
            # log_wp.debug(f"Started processing {current_record}")
            for s in self.symbols:
                this_symbol = self.symbols[s]
                if (
                    this_symbol._analyse_date == None
                    or this_symbol._analyse_date < data_end_date
                ):
                    this_symbol.process(current_record)
                else:
                    log_wp.log(9, f"{s}: No new data")
            current_record = current_record + self.interval_delta

        self.bot_telemetry.save_cycle()

        # log_wp.debug(f"Finished processing all records")
