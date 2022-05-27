# external packages
import logging
from datetime import datetime
import time

# my modules
from bot_telemetry import BotTelemetry
from broker_alpaca import AlpacaAPI
from broker_swyftx import SwyftxAPI
from broker_back_test import BackTestAPI
from inotification_service import INotificationService
from iparameter_store import IParameterStore
from stock_symbol import (
    Symbol,
    NO_POSITION_TAKEN,
    BUY_LIMIT_ORDER_ACTIVE,
    BUY_PRICE_MET,
    POSITION_TAKEN,
    TAKING_PROFIT,
    STOP_LOSS_ACTIVE,
)
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

    def __init__(
        self,
        ssm: IParameterStore,
        market_data_source,
        bot_telemetry: BotTelemetry,
        notification_service: INotificationService,
        symbols: list,
        interval: str = "5m",
        real_money_trading: bool = False,
        back_testing: bool = False,
        back_testing_balance: float = None,
        back_testing_override_broker: bool = False,
        back_testing_skip_bar_update: bool = False,
    ):
        self.interval = interval
        self.interval_delta, max_range = utils.get_interval_settings(self.interval)
        self.real_money_trading = real_money_trading
        self.ssm = ssm
        self.market_data_source = market_data_source
        self.back_testing = back_testing
        self.back_testing_balance = back_testing_balance
        self.bot_telemetry = bot_telemetry
        self.notification_service = notification_service
        self.back_testing_skip_bar_update = back_testing_skip_bar_update

        if back_testing:
            # override broker to back_test
            if back_testing_override_broker:
                for s in symbols:
                    s["api"] = "back_test"

        # get brokers and then set them up
        self.api_list = []
        for api in symbols:
            self.api_list.append(api["api"])
            log_wp.log(9, f"Found symbol {api['symbol']} using broker {api['api']}")

        # configure brokers
        self.api_list = list(set(self.api_list))
        self.api_dict = self.setup_brokers(
            api_list=self.api_list, store=ssm, back_testing=back_testing
        )

        # set up individual symbols
        self.symbols = {}
        for s in symbols:
            start_time = time.time()
            new_symbol = Symbol(
                symbol=s["symbol"],
                interval=self.interval,
                real_money_trading=self.real_money_trading,
                api=self.api_dict[s["api"]],
                bot_telemetry=self.bot_telemetry,
                store=ssm,
                market_data_source=market_data_source,
                back_testing=back_testing,
                back_testing_skip_bar_update=back_testing_skip_bar_update,
                notification_service=notification_service,
            )
            if new_symbol._init_complete:
                self.symbols[s["symbol"]] = new_symbol
                log_wp.info(
                    f'{s["symbol"]}: Set up complete in {round(time.time() - start_time,1)}s'
                )
            else:
                log_wp.error(
                    f'{s["symbol"]}: Failed to set up symbol - check spelling? YF returned {len(new_symbol.bars)} bars {round(time.time() - start_time,1)}s'
                )

    def setup_brokers(self, api_list, store, back_testing: bool = False):
        api_set = set(api_list)
        api_dict = {}

        for api in api_set:
            start_time = time.time()
            if api == "back_test":
                api_dict[api] = BackTestAPI(
                    back_testing=back_testing,
                    back_testing_balance=self.back_testing_balance,
                )
                break

            elif api == "swyftx":
                if self.real_money_trading:
                    access_token_path = "/tabot/prod/swyftx/access_token"
                else:
                    access_token_path = "/tabot/paper/swyftx/access_token"

                api_key = (
                    store.get_parameter(Name=access_token_path, WithDecryption=True)
                    .get("Parameter")
                    .get("Value")
                )

                api_dict[api] = SwyftxAPI(
                    api_key=api_key,
                    back_testing=back_testing,
                    back_testing_balance=self.back_testing_balance,
                )

            elif api == "alpaca":
                if self.real_money_trading:
                    api_key_path = "/tabot/prod/alpaca/api_key"
                    security_key_path = "/tabot/prod/alpaca/security_key"
                else:
                    api_key_path = "/tabot/paper/alpaca/api_key"
                    security_key_path = "/tabot/paper/alpaca/security_key"

                api_key = (
                    store.get_parameter(Name=api_key_path, WithDecryption=True)
                    .get("Parameter")
                    .get("Value")
                )
                secret_key = (
                    store.get_parameter(Name=security_key_path, WithDecryption=True)
                    .get("Parameter")
                    .get("Value")
                )

                api_dict[api] = AlpacaAPI(
                    alpaca_key_id=api_key,
                    alpaca_secret_key=secret_key,
                    back_testing=back_testing,
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

        if self.back_testing:
            latest_start_position = self.symbols[latest_symbol].bars.index.get_loc(
                latest_start
            )
            back_test_start_position = latest_start_position + 250

            start_date = self.symbols[latest_symbol].bars.index[
                back_test_start_position
            ]

        # TODO once you work out why we aren't analysing the most recent data, drop this to level 9
        log_wp.log(
            9,
            f"Range of all symbol bars: latest start date {latest_start} (for {latest_symbol}), latest end date {end_date}",
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
        if self.back_testing:
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