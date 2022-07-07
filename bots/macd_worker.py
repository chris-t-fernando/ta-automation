# external packages
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
from math import floor
import pandas as pd
import pytz
from typing import Callable

# my modules
from bot_telemetry import BotTelemetry
from buyplan import (
    BuyPlan,
    OrderQuantitySmallerThanMinimum,
    OrderValueSmallerThanMinimum,
    ZeroUnitsOrdered,
    InsufficientBalance,
    StopPriceAlreadyMet,
    TakeProfitAlreadyMet,
)
from inotification_service import INotificationService
from iparameter_store import IParameterStore
from itradeapi import (
    ITradeAPI,
    MARKET_BUY,
    MARKET_SELL,
    LIMIT_BUY,
    LIMIT_SELL,
    STOP_LIMIT_BUY,
    STOP_LIMIT_SELL,
    UnknownSymbolError,
    DelistedAssetError,
    UntradeableAssetError,
    MalformedOrderResult,
    ZeroUnitsOrderedError,
    ApiRateLimitError,
    BuyImmediatelyTriggeredError,
)
from macd_config import MacdConfig
from tabot_rules import TABotRules
import utils

log_wp = logging.getLogger("macd_worker")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("macd_worker.log")
log_wp.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(fhdlr)
log_wp.addHandler(hdlr)


NO_POSITION_TAKEN = 0
BUY_LIMIT_ORDER_ACTIVE = 1
BUY_PRICE_MET = 2
POSITION_TAKEN = 3
TAKING_PROFIT = 4
STOP_LOSS_ACTIVE = 5

STATE_MAP = {
    "NO_POSITION_TAKEN": NO_POSITION_TAKEN,
    "BUY_LIMIT_ORDER_ACTIVE": BUY_LIMIT_ORDER_ACTIVE,
    "BUY_PRICE_MET": BUY_PRICE_MET,
    "POSITION_TAKEN": POSITION_TAKEN,
    "TAKING_PROFIT": TAKING_PROFIT,
    "STOP_LOSS_ACTIVE": STOP_LOSS_ACTIVE,
}
STATE_MAP_INVERTED = {y: x for x, y in STATE_MAP.items()}


class SymbolForbidden(Exception):
    ...


# symbol can be backtest naive
class MacdWorker:
    symbol: str
    api: ITradeAPI
    broker_name: str
    rules: TABotRules
    config: MacdConfig
    store: IParameterStore
    buy_market: bool
    notification_service: INotificationService
    bot_telemetry: BotTelemetry
    interval: str
    # market_data_source is any type
    run_type: str
    back_testing: bool
    paper_testing: bool
    production_run: bool
    _init_complete: bool
    interval_delta: relativedelta
    max_range: relativedelta
    precision: int
    market: str
    state_const: int
    current_check: Callable
    active_order_id: str
    buy_plan: BuyPlan
    active_rule: dict
    enter_position_timeout: relativedelta
    _analyse_date: pd.Timestamp
    _back_testing_date: pd.Timestamp
    bars: pd.DataFrame
    min_quantity_increment: float
    min_quantity: float
    min_price_increment: float

    def __init__(self, symbol: str, api: ITradeAPI, config: MacdConfig, rules: TABotRules):
        self.symbol = symbol
        self.api = api
        self.broker_name = self.api.get_broker_name()
        self.rules = rules
        self.config = config
        self.store = config.store
        self.buy_market = config.buy_market
        self.notification_service = config.notification_service
        self.bot_telemetry = config.bot_telemetry
        self.interval = config.interval
        self.market_data_source = config.market_data_source
        self.run_type = config.run_type
        self.back_testing = config.back_testing
        self.paper_testing = config.paper_testing
        self.production_run = config.production_run
        self._init_complete = False

        try:
            if not self.is_valid_symbol():
                log_wp.error(f"{symbol}: Invalid symbol (delisted or untradeable)")
                return
        except UnknownSymbolError as e:
            # bad symbol, bail out
            log_wp.error(f"{symbol}: Invalid symbol ({str(e)})")
            return

        self.interval_delta, self.max_range = utils.get_interval_settings(self.interval)

        # next check precision on order - normal stocks are only to the thousandth, crypto is huge
        self.precision = self.api.get_precision(yf_symbol=self.symbol)
        self._set_order_size_and_increment()

        # work out if this symbol is only traded in certain hours
        self.market = self.get_market()

        # state machine config
        self.state_const = NO_POSITION_TAKEN
        self.current_check = self.check_state_no_position_taken
        self.active_order_id = None
        self.buy_plan = None
        self.active_rule = None
        self.play_id = None

        # when raising an initial buy order, how long should we wait for it to be filled til killing it?
        self.enter_position_timeout = self.interval_delta

        # pointer to current record to assess
        self._analyse_date = None

        # this is hacky - if back_testing is True then this will be the same date as _analyse_date
        self._back_testing_date = None

        bars = self._get_bars(
            initialised=False,
        )

        if len(bars) == 0:
            self.bars = []
            log_wp.error(f"{symbol}: No YF data for this symbol")
            return
        else:
            self.bars = utils.add_signals(bars, self.interval)
            self._init_complete = True

    def is_valid_symbol(self):
        # can't easily map alpaca and YFinance
        # UNI-USD in unicorn token, in alpaca UNIUSD is Uniswap token - different tokens
        # TODO: pull this from store
        forbidden_symbols = ["UNI-USD", "GRT-USD", "ONG-USD"]
        if self.symbol in forbidden_symbols:
            raise SymbolForbidden(
                f"Cannot map token {self.symbol} between broker API and Yahoo "
                f"Finance. Don't use it."
            )

        # next check that the symbol is known to the broker and tradeable
        try:
            return self.api.validate_symbol(symbol=self.symbol)

        except (UnknownSymbolError, DelistedAssetError, UntradeableAssetError) as e:
            raise

    def get_market(self):
        # there's the right way and the fast way to do this
        # we're doing the fast way
        if self.symbol[-4:] == "-USD":
            return "ccc_market"
        else:
            return "us_market"

        # this is how you'd do the right way but it will take ages to boot even a small number of symbols
        self.market = self.market_data_source.Ticker("ACN").info["market"]

    def _set_order_size_and_increment(self):
        asset = self.api.get_asset(self.symbol)
        self.min_quantity_increment = asset.min_quantity_increment
        self.min_quantity = asset.min_quantity
        self.min_price_increment = asset.min_price_increment

    def process(self, datestamp):
        # i'm too lazy to pass datestamp around so save it in object
        self._analyse_date = datestamp

        if self.back_testing:
            self._back_testing_date = self._analyse_date

        # if we have no data for this datestamp, then no action
        if datestamp not in self.bars.index:
            return

        self._analyse_index = self.bars.index.get_loc(self._analyse_date)

        # keep progressing through the state machine until we hit a stop
        while True:
            # run the current check - will return reference to a transition function if the check says we're ready for next state
            next_transition = self.current_check()
            # not ready for next state, break
            if next_transition == False:
                break

            # do the next transition, which will set self.current_check to whatever the next state check is, ready for next loop
            if next_transition():
                ...

            # a loop can happen while backtesting where:
            # we do a buy order
            # then a take profit gets triggered because 1.25% of risk/gain is less than current High
            # then the new take profit stop loss is set to 99% of that, which is lower than the Low of that same cycle
            # so it does a stop loss
            # then after it does a stop loss it goes back to searching for a buy signal in the same cycle
            # i think this loop should break if we've done a stop loss/order cancellation
            if self.back_testing and next_transition == self.trans_take_profit_again:
                log_wp.info(
                    f"{self.symbol}: Finished trans_take_profit_again. Finishing this "
                    "cycle of the state machine"
                )
                break

            if self.back_testing and next_transition == self.trans_close_position:
                log_wp.info(
                    f"{self.symbol}: Finished trans_close_position. Finishing this "
                    "cycle of the state machine"
                )
                break

        # TODO: not sure what to return?!
        return

    def get_rule(self):
        return self.rules.get_rule(symbol=self.symbol)

    def write_to_rules(self, buy_plan, order_result):
        return self.rules.write_to_rules(buy_plan=buy_plan, order_result=order_result)

    def replace_rule(self, new_rule):
        return self.rules.replace_rule(symbol=self.symbol, new_rule=new_rule)

    def remove_from_rules(self):
        log_wp.debug(f"{self.symbol}: Removing from {self.broker_name} rules")
        return self.rules.remove_from_rules(symbol=self.symbol)

    def get_state(self):
        return self.rules.get_state(symbol=self.symbol)

    def get_state_all(self):
        return self.rules.get_state_all()

    # TODO move logic into tabot rules
    def write_to_state(self, order):
        existing_state = self.get_state_all()

        for state in existing_state:
            if state["symbol"] == self.symbol and state["broker"] == self.broker_name:
                # there's already a symbol for this broker in state. check if its still live or stale
                closed_statuses = ["cancelled", "filled"]
                existing_order = self.api.get_order(order_id=state["order_id"])

                if existing_order.status_summary not in closed_statuses:
                    raise ValueError(
                        f"Tried to add {self.symbol} on broker {self.broker_name} to state, "
                        "but it already existed"
                    )

        # if we got here then its safe to add this entry to state
        new_state = {
            "symbol": self.symbol,
            "order_id": order.order_id,
            "broker": self.broker_name,
            "state": STATE_MAP_INVERTED[self.state_const],
            "play_id": self.play_id,
        }

        self.rules.write_to_state(new_state=new_state)

    def remove_from_state(self):
        log_wp.debug(f"{self.symbol}: Removing from {self.broker_name} state")
        return self.rules.remove_from_state(symbol=self.symbol, broker=self.broker_name)

    # START BAR FUNCTIONS
    def _get_bars(self, from_date=None, to_date=None, initialised: bool = True):
        saved_data = False
        yf_end = None

        if self.back_testing and self.config.back_testing_skip_bar_update:
            saved_bars = utils.load_bars(
                self.symbol,
                bucket=self.config.saved_symbol_data_bucket,
                key_base=self.config.saved_symbol_key_base,
            )
            # this means we got data from s3
            if type(saved_bars) == pd.core.frame.DataFrame:
                yf_start = saved_bars.index[-1]
                saved_data = True
                bars = saved_bars
                log_wp.log(9, f"{self.symbol}: Found valid S3 data")
            else:
                raise RuntimeError(
                    f"back_testing is True and back_testing_skip_bar_updates is True, "
                    f"but we have no data for {self.symbol}"
                )

        else:
            if initialised == False:
                # we actually need to grab everything
                # first check to see if we have any data in s3
                # if i wasn't lazy i'd make a
                saved_bars = utils.load_bars(
                    self.symbol,
                    bucket=self.config.saved_symbol_data_bucket,
                    key_base=self.config.saved_symbol_key_base,
                )

                # this means we got data from s3
                if type(saved_bars) == pd.core.frame.DataFrame:
                    yf_start = saved_bars.index[-1]
                    saved_data = True
                    log_wp.log(9, f"{self.symbol}: Found valid S3 data")
                else:
                    # this means we didn't get data from s3
                    yf_start = datetime.now() - self.max_range
                    log_wp.debug(f"{self.symbol}: No data found in S3")

            else:
                # if we've specified a date, we're probably refreshing our dataset over time
                if from_date:
                    # widen the window out, just to make sure we don't miss any data in the refresh
                    yf_start = from_date - (self.interval_delta * 2)
                else:
                    # we're refreshing but didn't specify a date, so assume its in the last x minutes/hours
                    yf_start = datetime.now() - (self.interval_delta * 2)

            # didn't specify an end date so go up til now
            if to_date == None:
                # yf_end = datetime.now()
                yf_end = None
            else:
                # specified an end date so use it
                yf_end = datetime.strptime(to_date, "%Y-%m-%d %H:%M:%S")

            # no end required - we want all of the data
            bars = self.market_data_source.Ticker(self.symbol).history(
                start=yf_start,
                interval=self.interval,
                actions=False,
                debug=False,
            )

            bars = bars.tz_convert(pytz.utc)

            if saved_data:
                bars = utils.merge_bars(saved_bars, bars)

        if len(bars) == 0:
            # something went wrong - usually bad symbol and search parameters
            if not yf_end:
                end_string = "now"
            else:
                end_string = yf_end

            log_wp.warning(f"{self.symbol}: No data returned between {yf_start} til {end_string}")
            return bars

        interval_mod = utils.get_interval_integer(self.interval)

        bars = bars.loc[(bars.index.minute % interval_mod == 0) & (bars.index.second == 0)]

        # put bar data into the api so that the back_testing broker API has some data to work with
        if self.back_testing:
            self.api._put_bars(symbol=self.symbol, bars=bars)

        return bars

    def update_bars(self, from_date=None, to_date=None):
        if from_date == None:
            from_date = self.bars.index[-1]

        new_bars = self._get_bars(
            from_date=from_date,
            to_date=to_date,
        )

        if len(new_bars) > 0:
            # pad new bars to 200 rows so that macd and sma200 work
            # TODO merge these bars in before running add_signals - this way we're doing signals for only a couple rows instead of 200
            if len(new_bars) < 300:
                new_bars = utils.merge_bars(new_bars=new_bars, bars=self.bars.iloc[-300:])

            new_bars = utils.add_signals(new_bars, interval=self.interval)
            self.bars = utils.merge_bars(self.bars, new_bars)

            if self.back_testing:
                self.api._put_bars(symbol=self.symbol, bars=self.bars)

        else:
            log_wp.log(9, f"{self.symbol}: No new data since {from_date}")

    # END  BAR FUNCTIONS

    # START CHECK_STATE FUNCTIONS
    def check_state_no_position_taken(self):
        # get iloc of analyse_index

        # TODO - if the last data is too far in the past, bail out here!
        bars_slice = self.get_data_window()

        if len(bars_slice) < 200:
            log_wp.warning(
                f"{self.symbol}: Less than 200 bar samples - high probability of "
                f"exception/error so bailing out"
            )
            return False

        # check to see if the signal was found in the last record in bars_slice
        buy_signal_found = utils.check_buy_signal(
            df=bars_slice, symbol=self.symbol, bot_telemetry=self.bot_telemetry
        )

        # if we found a buy signal, return the transition function to run
        if buy_signal_found:
            # first how much cash do we have to spend?
            account = self.api.get_account()
            balance = account.assets["USD"]
            play_id = "play-" + self.symbol + utils.generate_id()

            # MACD stuff
            blue_cycle_start = self.get_blue_cycle_start(df=bars_slice)
            red_cycle_start = self.get_red_cycle_start(df=bars_slice, before_date=blue_cycle_start)

            stop_loss_unit = self.calculate_stop_loss_unit_price(
                df=bars_slice,
                start_date=red_cycle_start,
                end_date=blue_cycle_start,
            )

            stop_unit_date = self.calculate_stop_loss_date(
                df=bars_slice,
                start_date=red_cycle_start,
                end_date=blue_cycle_start,
            )

            intervals_since_stop = self.count_intervals(df=bars_slice, start_date=stop_unit_date)

            entry_unit = bars_slice.Close.iloc[-1]
            last_low = bars_slice.Low.iloc[-1]
            last_high = bars_slice.High.iloc[-1]

            log_wp.debug(
                f"{self.symbol}: Last cycle started on {red_cycle_start}, "
                f"{intervals_since_stop} intervals ago"
            )
            log_wp.debug(
                f"{self.symbol}: The lowest price during that cycle was {stop_loss_unit} "
                f"on {stop_unit_date}. This will be used as the stop loss for the play"
            )

            # TODO you need to add a toggle to buy at market or limit - you keep missing potential swyftx opps because of their shitty pricing
            try:
                buy_plan = BuyPlan(
                    symbol=self.symbol,
                    balance=balance,
                    play_id=play_id,
                    df=bars_slice,
                    precision=self.precision,
                    min_quantity_increment=self.min_quantity_increment,
                    min_quantity=self.min_quantity,
                    min_price_increment=self.min_price_increment,
                    max_play_value=self.config.order_size,
                )

            except (
                OrderQuantitySmallerThanMinimum,
                OrderValueSmallerThanMinimum,
                ZeroUnitsOrderedError,
                InsufficientBalance,
                StopPriceAlreadyMet,
                TakeProfitAlreadyMet,
            ) as e:
                log_wp.info(
                    f"{self.symbol}: Found buy signal but failed to generate BuyPlan: balance is {balance}, error is '{str(e)}'"
                )
                return False
            except Exception as e:
                log_wp.critical(
                    f"{self.symbol}: Found buy signal, but something went wrong in "
                    f"BuyPlan. Balance {balance:,.2f}"
                )
                return False

            self.buy_plan = buy_plan
            log_wp.info(f"{self.symbol}: Found buy signal, next step is trans_entering_position")
            return self.trans_entering_position

        # if we got here, nothing to do
        # log_wp.debug(f"{self.symbol}: No buy signal found, no action to take")
        return False

    def check_state_entering_position(self):
        # get status of buy order at self.active_order_id
        order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._back_testing_date
        )

        if order.status_summary == "cancelled":
            # the order got cancelled for some reason, so transition back to no position taken
            log_wp.debug(
                f"{self.symbol}: Order {order.order_id}: cancelled, next action is trans_buy_cancelled"
            )
            return self.trans_buy_order_cancelled
        elif order.status_summary == "filled":
            # buy got filled so transition to position taken
            self.active_order_result = order
            self.active_order_id = order.order_id

            log_wp.debug(
                f"{self.symbol}: Order {order.order_id}: filled, next action is trans_buy_order_filled"
            )
            return self.trans_buy_order_filled
        elif order.status_summary == "open" or order.status_summary == "pending":
            # check timeout
            if self._is_position_timed_out(now=self._analyse_date, order=order):
                # transition back to no position taken
                log_wp.debug(
                    f"{self.symbol}: Order {order.order_id}: has timed out, next "
                    " action is trans_buy_order_timed_out"
                )
                return self.trans_buy_order_timed_out
            log_wp.log(9, f"{self.symbol}: Order {order.order_id}: is still open or pending")

        # do nothing - still open, not timedout
        log_wp.debug(
            f"{self.symbol}: Order {order.order_id} is still open but not filled. Last "
            f"High was {self.bars.High.loc[self._analyse_date]:,} last Low was "
            f"{self.bars.Low.loc[self._analyse_date]:,}, limit price is {order.ordered_unit_price:,}"
        )
        return False

    def check_state_position_taken(self):
        self.position = self.api.get_position(symbol=self.symbol)

        # position liquidated
        if self.position.quantity == 0:
            log_wp.warning(
                f"{self.symbol}: 0 units held, assuming that position has been "
                "externally liquidated"
            )
            return self.trans_externally_liquidated

        # get inputs for next checks
        last_close = self.bars.Close.loc[self._analyse_date]
        self.active_rule = self.get_rule()

        # for some reason there is no rule for this - we're lost, so stop loss and punch out - should never happen
        if not self.active_rule:
            log_wp.critical(
                f"{self.symbol}: Can't find rule for this position, next action is "
                "trans_position_taken_to_stop_loss"
            )
            return self.trans_position_taken_to_stop_loss

        # stop loss hit?
        stop_loss = self.active_rule["current_stop_loss"]
        if last_close < stop_loss:
            log_wp.warning(
                f"{self.symbol} {self._analyse_date}: Stop loss hit, next action "
                "is trans_position_taken_to_stop_loss"
            )
            return self.trans_position_taken_to_stop_loss

        # otherwise move straight on to take profit
        log_wp.debug(f"{self.symbol}: Position established, next action is trans_take_profit")
        return self.trans_take_profit

    def check_state_take_profit(self):
        # get current position for this symbol
        self.position = self.api.get_position(symbol=self.symbol)

        # get order
        order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._back_testing_date
        )
        self.active_order_id = order.order_id

        # get last close
        last_close = self.bars.Close.loc[self._analyse_date]

        # get rules
        self.active_rule = self.get_rule()

        # first check to see if the take profit order has been filled
        if order.status_summary == "filled":
            # no comms needed - i already do it in the trans_take_profit (and again) methods
            # _value = order.filled_unit_quantity * order.filled_unit_price
            # self.notification_service.send(
            #    message=f"MACD algo took profit on {self.symbol} ({self.api.get_broker_name()}) | "
            #    f"${_value:,.2f} total sale value | "
            #    f"${order.filled_unit_price:,.2f} sold price | "
            #    f"{order.filled_unit_quantity:,} units sold"
            # )

            # do we have any units left?
            if self.position.quantity == 0:
                # nothing left to sell
                self.notification_service.send(
                    message=f"I don't hold any more units of {self.symbol} ({self.api.get_broker_name()}). Play complete",
                )
                log_wp.warning(
                    f"{self.symbol}: No units still held. Play complete, next action is trans_close_position"
                )
                return self.trans_close_position
            else:
                # no slack notification needed here - I do it in trans_take_profit_again
                # still some left to sell, so transition back to same state
                log_wp.debug(
                    f"{self.symbol}: Units still held. Play is still going, next action is trans_take_profit_again"
                )
                return self.trans_take_profit_again

        # position liquidated but not using our fill order
        if self.position.quantity == 0:
            self.notification_service.send(
                message=f"MACD algo terminated for {self.symbol} ({self.api.get_broker_name()}). Hold no units but I didn't "
                "liquidate them. Did these units get liquidated outside of my workflow?",
            )
            log_wp.critical(
                f"{self.symbol} {self._analyse_date}: No units held but liquidated outside "
                "of this sell order, next action is trans_externally_liquidated"
            )
            return self.trans_externally_liquidated

        # for some reason there is no rule for this - we're lost, so stop loss and punch out - should never happen
        if not self.active_rule:
            self.notification_service.send(
                message=f"MACD algo terminated for {self.symbol} ({self.api.get_broker_name()}). The rules for this play "
                "(stop loss etc) can't found in state storage. You need to liquidate this holding manually.",
            )
            log_wp.critical(
                f"{self.symbol} {self._analyse_date}: Can't find rule for this position, "
                "next action is trans_take_profit_to_stop_loss"
            )
            return self.trans_take_profit_to_stop_loss

        # stop loss hit?
        stop_loss = self.active_rule["current_stop_loss"]
        if last_close < stop_loss:
            # no notification required here - will be handled in trans_ method
            log_wp.warning(
                f"{self._analyse_date} {self.symbol}: Stop loss hit (last close {last_close:,} "
                f"< stop loss {stop_loss:,}), next action is trans_take_profit_to_stop_loss"
            )
            return self.trans_take_profit_to_stop_loss

        if order.status_summary == "cancelled":
            # the order got cancelled for some reason. we still have a position, so try to re-raise it
            # no notification required here - will be handled in trans_ method
            log_wp.critical(
                f"{self.symbol} {self._analyse_date}: Sell order was cancelled for some reason (maybe "
                "by broker?), so trying to re-raise it. Next action is trans_take_profit_retry"
            )
            return self.trans_take_profit_retry

        # nothing to do
        return False

    def check_state_stop_loss(self):
        position = self.api.get_position(symbol=self.symbol)

        # get inputs for next checks
        self.active_rule = self.get_rule()

        # for some reason there is no rule for this - we're lost, so stop loss and punch out - should never happen
        if not self.active_rule:
            log_wp.critical(
                f"{self.symbol} {self._analyse_date}: Can't find rule for this position, next "
                "action is trans_take_profit_to_stop_loss"
            )

            return self.trans_position_taken_to_stop_loss

        # get order
        order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._back_testing_date
        )

        if order.status_summary == "cancelled":
            # the order got cancelled for some reason. we still have a position, so try to re-raise it
            log_wp.critical(
                f"{self.symbol} {self._analyse_date} {self.api.get_broker_name()}: Sell order was cancelled for some "
                "reason (maybe be broker?), so trying to re-raise it. Next action is trans_take_profit_retry"
            )
            self.notification_service.send(
                message=f"Stop loss triggered for {self.symbol} ({self.api.get_broker_name()}) but sell order got cancelled. Trying again.",
            )
            return self.trans_stop_loss_retry

        elif order.status_summary == "filled":
            # stop loss got filled, now need to fully close position
            log_wp.info(
                f"{self.symbol} {self._analyse_date} {self.api.get_broker_name()}: No units still held, next action "
                "is trans_close_position"
            )
            _total_value = order.filled_unit_quantity * order.filled_unit_price
            self.notification_service.send(
                message=f"Stop loss filled for {self.symbol} ({self.api.get_broker_name()}) | "
                f"${_total_value:,.2f} total sale value | "
                f"${order.filled_unit_price:,} sold unit price | "
                f"{order.filled_unit_quantity:,} units sold"
            )
            return self.trans_close_position

        elif order.status_summary == "open" or order.status_summary == "pending":
            # is the order still open but we don't own any? if so, it got liquidated outside of this process
            if position.quantity == 0:
                log_wp.critical(
                    f"{self.symbol} {self._analyse_date} {self.api.get_broker_name()}: No units held but liquidated "
                    "outside of this sell order, next action is trans_externally_liquidated"
                )
                self.notification_service.send(
                    message=f"Stop loss triggered for {self.symbol} ({self.api.get_broker_name()}) and I successfully "
                    "raised a stop order, but the position got liquidated some other way. "
                    "Did you do it manually?",
                )
                return self.trans_externally_liquidated

            # nothing to do
            log_wp.debug(
                f"{self.symbol} {self._analyse_date}: Stop loss order still open, no next action"
            )
            return False

    # END  CHECK_STATE FUNCTIONS

    # START TRANSITION FUNCTIONS
    def trans_entering_position(self):
        # submit buy order
        log_wp.log(9, f"{self.symbol}: Started trans_entering_position")
        self.play_id = self.buy_plan.play_id

        # there is a toggle to do market buy or limit buy
        if self.buy_market:
            order_result = self.api.buy_order_market(
                symbol=self.symbol,
                units=self.buy_plan.units,
                back_testing_date=self._back_testing_date,
            )
        else:
            try:
                order_result = self.api.buy_order_limit(
                    symbol=self.symbol,
                    units=self.buy_plan.units,
                    unit_price=self.buy_plan.entry_unit,
                    back_testing_date=self._back_testing_date,
                )
            except BuyImmediatelyTriggeredError as e:
                # fall back to a market order, since our limit order was immediately met
                order_result = self.api.buy_order_market(
                    symbol=self.symbol,
                    units=self.buy_plan.units,
                    back_testing_date=self._back_testing_date,
                )

        accepted_statuses = ["open", "filled", "pending"]
        if order_result.status_summary not in accepted_statuses:
            log_wp.error(
                f"{self.symbol} {self._analyse_date} {self.api.get_broker_name()}: Failed to submit buy order "
                f"{order_result.order_id}: {order_result.status_text}"
            )
            self.notification_service.send(
                message=f"Buy conditions met for {self.symbol} ({self.api.get_broker_name()}) but when I tried to "
                f"raise a buy order it failed with error message '{order_result.status_text}'",
            )

            return self.trans_buy_order_cancelled

        # hold on to order ID
        self.active_order_id = order_result.order_id

        self.bot_telemetry.add_order(order_result=order_result, play_id=self.play_id)

        self.state_const = BUY_LIMIT_ORDER_ACTIVE

        # write state
        self.write_to_state(order_result)

        # set self.current_check to check_position_taken
        self.current_check = self.check_state_entering_position

        log_wp.warning(
            f"{self._analyse_date} {self.symbol}: Buy order {order_result.order_id} (state "
            f"{order_result.status_summary}) at unit price {order_result.ordered_unit_price} submitted"
        )
        log_wp.log(9, f"{self.symbol}: Finished trans_entering_position")
        return True

    def trans_buy_order_timed_out(self):
        # get state
        state = self.get_state()

        if state == False:
            log_wp.critical(
                f"{self.symbol} {self._analyse_date}: Unable to find order for this symbol in state! "
                "There may be an unmanaged buy order in the market!"
            )
        else:
            # cancel order
            log_wp.info(f"{self.symbol}: Deleting order")
            order_result = self.api.cancel_order(
                order_id=state["order_id"], back_testing_date=self._analyse_date
            )

            self.bot_telemetry.add_order(order_result=order_result, play_id=self.play_id)

        # clear any variables set at symbol
        self.active_order_id = None
        self.buy_plan = None
        self.play_id = None

        # clear state
        self.remove_from_state()

        # set current check
        self.current_check = self.check_state_no_position_taken

        self.state_const = NO_POSITION_TAKEN

        return True

    def trans_buy_order_cancelled(self):
        # get state
        state = self.get_state()

        if state == False:
            log_wp.warning(
                f"{self.symbol} {self._analyse_date}: Unable to find order for this symbol "
                "in state! May be an orphaned buy order!"
            )
        else:
            # no need to cancel order - it already got nuked
            ...

        order_result = self.api.cancel_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        self.bot_telemetry.add_order(order_result=order_result, play_id=self.play_id)

        # clear any variables set at symbol
        self.active_order_id = None
        self.buy_plan = None
        self.play_id = None

        # clear state
        self.remove_from_state()

        # set current check
        self.current_check = self.check_state_no_position_taken
        self.state_const = NO_POSITION_TAKEN

        return True

    def trans_buy_order_filled(self):
        # clear state
        self.remove_from_state()

        # add rule
        self.write_to_rules(buy_plan=self.buy_plan, order_result=self.active_order_result)

        order_result = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        self.bot_telemetry.add_order(order_result=order_result, play_id=self.play_id)

        # update active_order_id
        self.active_order_id = None

        # set current check
        self.current_check = self.check_state_position_taken
        _position_value = self.buy_plan.entry_unit * self.buy_plan.units
        self.notification_service.send(
            message=f"MACD algo took position in {self.symbol} ({self.api.get_broker_name()}) | "
            f"${_position_value:,.2f} total value | "
            f"${self.buy_plan.entry_unit:,} entry price | "
            f"${self.buy_plan.target_price:,} target price | "
            f"${self.buy_plan.stop_unit:,} stop loss price | "
            f"{self.buy_plan.units:,} units bought",
        )

        self.state_const = POSITION_TAKEN

        return True

    def trans_take_profit(self):
        # self.active_rule already set in check phase
        # self.position already set in check phase

        # raise sell order
        pct = self.active_rule["risk_point_sell_down_pct"]
        units = self.position.quantity

        # TODO: i don't love this code. it needs full test coverage
        # units_to_sell = floor(pct * units)
        units_to_sell = pct * units
        units_to_sell_mod = units_to_sell % self.min_quantity_increment
        units_to_sell = floor(units_to_sell - units_to_sell_mod)

        if units_to_sell == 0:
            units_to_sell = 1

        # TODO this is temporary while testing only
        # units_to_sell = self.position.quantity

        order = self.api.sell_order_limit(
            symbol=self.symbol,
            units=units_to_sell,
            unit_price=self.buy_plan.target_price,
            back_testing_date=self._back_testing_date,
        )

        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # hold on to active_order_id
        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_take_profit

        self.state_const = TAKING_PROFIT

        return True

    def trans_take_profit_to_stop_loss(self):
        # self.position already held from check
        # self.active_order_id already held from check

        # cancel take profit order
        cancelled_order = self.api.cancel_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        if cancelled_order:
            log_wp.warning(
                f"{self.symbol}: Successfully cancelled take_profit order {cancelled_order.order_id}"
            )
        else:
            log_wp.warning(
                f"{self.symbol}: Cancelling take_profit order returned False - already deleted?"
            )

        self.bot_telemetry.add_order(order_result=cancelled_order, play_id=self.play_id)

        # submit stop loss
        order = self.api.sell_order_market(
            symbol=self.symbol,
            units=self.position.quantity,
            back_testing_date=self._back_testing_date,
        )

        if order.status_summary == "cancelled":
            log_wp.critical(
                f"{self.symbol} {self._analyse_date}: Unable to submit stop loss order "
                f"for symbol! API returned {order.status_text}"
            )
            return False

        log_wp.warning(f"{self.symbol}: Successfully submitted stop_loss order {order.order_id}")

        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # update active_order_id
        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_stop_loss
        self.state_const = STOP_LOSS_ACTIVE

        return True

    def trans_position_taken_to_stop_loss(self):
        # self.position already held from check

        # submit stop loss
        order = self.api.sell_order_market(
            symbol=self.symbol,
            units=self.position.quantity,
            back_testing_date=self._back_testing_date,
        )

        if order.status_summary == "cancelled":
            log_wp.critical(
                f"{self.symbol} {self._analyse_date}: Unable to submit stop loss order "
                f"for symbol! API returned {order.status_text}"
            )
            return False

        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # update active_order_id
        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_stop_loss

        self.state_const = STOP_LOSS_ACTIVE

        return True

    def trans_externally_liquidated(self):
        # happens if it gets liquidated immediately after purchase
        if self.active_order_id is not None:
            order = self.api.get_order(
                order_id=self.active_order_id, back_testing_date=self._analyse_date
            )
            self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # already don't hold any, so no need to delete orders
        # just need to clean up the object and delete rules
        self.remove_from_rules()
        self.active_order_id = None
        self.active_order_result = None
        self.buy_plan = None
        self.play_id = None

        # TODO add to win/loss as unknown outcome

        self.current_check = self.check_state_no_position_taken

        self.state_const = NO_POSITION_TAKEN

    def trans_close_position(self):
        order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # clear active order details
        self.active_order_id = None
        self.active_order_result = None
        self.buy_plan = None
        self.play_id = None

        # delete rules
        self.remove_from_rules()

        # set check
        self.current_check = self.check_state_no_position_taken

        self.state_const = NO_POSITION_TAKEN

    def trans_stop_loss_retry(self):
        # our take profit order was cancelled for some reason
        # i'm not sure what i want to do here actually. this needs more thought than just spamming new orders

        # no need to close the previous order - its dead
        order = self.api.close_position(
            symbol=self.symbol, back_testing_date=self._back_testing_date
        )

        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_take_profit

        self.state_const = STOP_LOSS_ACTIVE

        return True

    def trans_take_profit_retry(self):
        # our take profit order was cancelled for some reason
        # i'm not sure what i want to do here actually. this needs more thought than just spamming new orders

        # self.active_rule already set in check phase
        # self.position already set in check phase

        pct = self.active_rule["risk_point_sell_down_pct"]
        units = self.position.quantity
        units_to_sell = floor(pct * units)

        order = self.api.sell_order_limit(
            symbol=self.symbol,
            units=units_to_sell,
            unit_price=self.active_rule["current_target_price"],
            back_testing_date=self._back_testing_date,
        )

        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # hold on to active_order_id
        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_take_profit

        self.state_const = TAKING_PROFIT

        return True

    def trans_take_profit_again(self):
        # our old take profit order was filled, so need to raise a new one
        # no need to check if we still have a position - got checked at check stage

        # self.active_rule already set in check phase
        # self.position already set in check phase

        filled_order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._back_testing_date
        )
        filled_value = filled_order.filled_unit_quantity * filled_order.filled_unit_price
        self.bot_telemetry.add_order(order_result=filled_order, play_id=self.play_id)
        log_wp.warning(
            f"{self.symbol} {self._analyse_date}: Successfully took profit: order ID "
            f"{filled_order.order_id} sold {filled_order.filled_unit_quantity:,} at "
            f"{filled_order.filled_unit_price:,} for value {filled_value:,}"
        )

        updated_plan = self.buy_plan.take_profit(
            filled_order=filled_order,
            active_rule=self.active_rule,
            new_position_quantity=self.position.quantity,
        )

        if updated_plan["new_target_unit_price"] == None:
            print("new_target_unit_price_banana")

        order = self.api.sell_order_limit(
            symbol=self.symbol,
            units=updated_plan["new_units_to_sell"],
            unit_price=updated_plan["new_target_unit_price"],
            back_testing_date=self._back_testing_date,
        )

        # hold on to active_order_id
        self.active_order_id = order.order_id

        # update rules
        new_target_unit_price = updated_plan["new_target_unit_price"]
        new_units_held = updated_plan["new_units_held"]
        new_units_sold = updated_plan["new_units_sold"]
        new_rule = updated_plan["new_rule"]
        new_stop_loss = updated_plan["new_stop_loss"]

        if not self.replace_rule(new_rule=new_rule):
            log_wp.critical(
                f"{self.symbol} {self._analyse_date}: Failed to update rules with new rule! Likely orphaned order"
            )
        _total_value = filled_order.filled_unit_quantity * filled_order.filled_unit_price
        self.notification_service.send(
            message=f"MACD algo took profit on {self.symbol} ({self.api.get_broker_name()}) | "
            f"${_total_value:,.2f} total sale value | "
            f"${filled_order.filled_unit_price:,} sold price | "
            f"{filled_order.filled_unit_quantity:,} units sold"
        )
        self.notification_service.send(
            message=f"I still hold {new_units_held:,} units of {self.symbol} | "
            f"${new_target_unit_price:,} is new target price | ${new_stop_loss:,} is new stop loss",
        )

        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # set current check
        self.current_check = self.check_state_take_profit

        new_value = order.ordered_unit_quantity * order.ordered_unit_price
        log_wp.warning(
            f"{self.symbol} {self._analyse_date}: Successfully lodged new take profit: order ID "
            f"{order.order_id} (state {order.status_summary}) to sell {order.ordered_unit_quantity:,} "
            f"unit at {order.ordered_unit_price:,} for value "
            f"{new_value:,} with new stop loss {new_stop_loss:,}"
        )

        self.state_const = TAKING_PROFIT
        return True

    # END TRANSITION FUNCTIONS

    # START SUPPORTING FUNCTIONS
    def get_data_window(self, length: int = 200):
        # get the last 200 records before this one
        first_record = self._analyse_index - length
        # need the +1 otherwise it does not include the record at this index, it gets trimmed
        last_record = self._analyse_index + 1
        bars = self.bars.iloc[first_record:last_record]
        return bars

    # returns True/False depending on whether an OrderResult order has passed the configured timeout window
    def _is_position_timed_out(self, now, order):
        cutoff_date = now.astimezone(pytz.utc) - self.enter_position_timeout
        cutoff_date = cutoff_date.astimezone(pytz.utc)
        if order.create_time < cutoff_date:
            return True
        return False

    # END SUPPORTING FUNCTIONS

    # START PRICING FUNCTIONS
    def get_red_cycle_start(self, df: pd.DataFrame, before_date):
        return df.loc[
            (df["macd_cycle"] == "blue") & (df.index < before_date) & (df.macd_crossover == True)
        ].index[-1]

    def get_blue_cycle_start(self, df: pd.DataFrame):
        return df.loc[(df.macd_crossover == True) & (df.macd_macd < 0)].index[-1]

    def calculate_stop_loss_unit_price(self, df: pd.DataFrame, start_date, end_date):
        return df.loc[start_date:end_date].Close.min()

    # TODO there is 100% a better way of doing this by sorting Timestamp indexes instead of iloc
    def calculate_stop_loss_date(self, df: pd.DataFrame, start_date, end_date):
        return df.loc[start_date:end_date].Close.idxmin()

    def count_intervals(self, df: pd.DataFrame, start_date, end_date=None):
        if end_date == None:
            return len(df.loc[start_date:])
        else:
            return len(df.loc[start_date:end_date])

    # END PRICING FUNCTIONS
