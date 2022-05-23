from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
from itradeapi import (
    ITradeAPI,
    MARKET_BUY,
    MARKET_SELL,
    LIMIT_BUY,
    LIMIT_SELL,
    STOP_LIMIT_BUY,
    STOP_LIMIT_SELL,
)
import utils
import yfinance as yf
import logging
import warnings
from buyplan import BuyPlan
from math import floor
import pandas as pd

warnings.simplefilter(action="ignore", category=FutureWarning)

log_wp = logging.getLogger(
    "stock_symbol"
)  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
log_wp.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
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

# symbol can be backtest naive
class Symbol:
    def __init__(
        self,
        symbol: str,
        api: ITradeAPI,
        interval: str,
        store,
        bot_telemetry,
        data_source,
        real_money_trading: bool = False,
        to_date: str = None,
        back_testing: bool = False,
    ):
        self.back_testing = back_testing
        self.bot_telemetry = bot_telemetry
        self.symbol = symbol
        self.api = api
        self.interval = interval
        self.real_money_trading = real_money_trading
        self.store = store
        self.data_source = data_source
        self.initialised = False
        self.interval_delta, self.max_range = utils.get_interval_settings(self.interval)

        # state machine config
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
            to_date=to_date,
            initialised=False,
        )

        if len(bars) == 0:
            self.bars = []
            self._init_complete = False
        else:
            self.bars = utils.add_signals(bars, interval)
            self._init_complete = True

    def process(self, datestamp):
        # i'm too lazy to pass datestamp around so save it in object
        self._analyse_date = datestamp

        if self.back_testing:
            self._back_testing_date = self._analyse_date

        # if we have no data for this datestamp, then no action
        if datestamp not in self.bars.index:
            return
        # print(f"{self.symbol} bar count {len(self.bars)}")
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

        # TODO: not sure what to return?!
        return

    # STATE AND RULE FUNCTIONS
    def get_state(self):
        stored_state = utils.get_stored_state(
            store=self.store, back_testing=self.back_testing
        )

        for this_state in stored_state:
            if this_state["symbol"] == self.symbol:
                return this_state

        return False

    # writes the symbol to state
    def _write_to_state(self, order):
        stored_state = utils.get_stored_state(
            store=self.store, back_testing=self.back_testing
        )
        broker_name = self.api.get_broker_name()

        new_state = []

        for this_state in stored_state:
            # needs to match broker and symbol
            s_symbol = this_state["symbol"]
            s_broker = this_state["broker"]
            if s_symbol == self.symbol and s_broker == broker_name:
                raise ValueError(
                    f"Tried to add {self.symbol} on broker {broker_name} to state, but it already existed"
                )
            else:
                # it's not the state we're looking for so keep it
                new_state.append(this_state)

        # if we got here, the symbol/broker combination does not exist in state so we are okay to add it
        new_state.append(
            {
                "symbol": self.symbol,
                "order_id": order.order_id,
                "broker": broker_name,
                "state": STATE_MAP_INVERTED[order.status],
                "play_id": self.play_id,
            }
        )

        utils.put_stored_state(
            store=self.store, new_state=new_state, back_testing=self.back_testing
        )

        log_wp.log(9, f"{self.symbol}: Successfully wrote order to state")

    # removes this symbol from the state
    def _remove_from_state(self):
        stored_state = utils.get_stored_state(
            store=self.store, back_testing=self.back_testing
        )
        broker_name = self.api.get_broker_name()
        found_in_state = False

        new_state = []

        for this_state in stored_state:
            # needs to match broker and symbol
            s_symbol = this_state["symbol"]
            s_broker = this_state["broker"]
            if s_symbol == self.symbol and s_broker == broker_name:
                found_in_state = True
            else:
                # it's not the state we're looking for so keep it
                new_state.append(this_state)

        utils.put_stored_state(
            store=self.store, new_state=new_state, back_testing=self.back_testing
        )

        if found_in_state:
            log_wp.log(9, f"{self.symbol}: Successfully wrote updated state")
            return True
        else:
            log_wp.warning(
                f"{self.symbol}: Tried to remove symbol from state but did not find it"
            )
            return False

    # replaces the rule for this symbol
    def _replace_rule(self, new_rule):
        stored_rules = utils.get_rules(store=self.store, back_testing=self.back_testing)

        new_rules = []

        for rule in stored_rules:
            if rule["symbol"] == self.symbol:
                new_rules.append(new_rule)
            else:
                new_rules.append(rule)

        write_result = utils.put_rules(
            store=self.store,
            symbol=self.symbol,
            new_rules=new_rules,
            back_testing=self.back_testing,
        )

        return write_result

    # adds sybol to rules - will barf if one already exists
    def _write_to_rules(self, buy_plan, order_result):
        stored_rules = utils.get_rules(store=self.store, back_testing=self.back_testing)

        new_rules = []

        for this_state in stored_rules:
            s_symbol = this_state["symbol"]
            if s_symbol == self.symbol:
                raise ValueError(
                    f"Tried to add {self.symbol} rules, but it already existed"
                )
            else:
                # it's not the state we're looking for so keep it
                new_rules.append(this_state)

        # if we got here, the symbol does not exist in rules so we are okay to add it
        new_rule = {
            "symbol": buy_plan.symbol,
            "play_id": self.play_id,
            "original_stop_loss": buy_plan.stop_unit,
            "current_stop_loss": buy_plan.stop_unit,
            "original_target_price": buy_plan.target_price,
            "current_target_price": buy_plan.target_price,
            "steps": 0,
            "original_risk": buy_plan.risk_unit,
            "current_risk": buy_plan.risk_unit,
            "purchase_date": self._analyse_date,
            "purchase_price": order_result.filled_unit_price,
            "units_held": order_result.filled_unit_quantity,
            "units_sold": 0,
            "units_bought": order_result.filled_unit_quantity,
            "order_id": order_result.order_id,
            "sales": [],
            "win_point_sell_down_pct": 0.5,
            "win_point_new_stop_loss_pct": 0.995,
            "risk_point_sell_down_pct": 0.25,
            "risk_point_new_stop_loss_pct": 0.99,
        }

        new_rules.append(new_rule)

        utils.put_rules(
            symbol=self.symbol,
            store=self.store,
            new_rules=new_rules,
            back_testing=self.back_testing,
        )

        log_wp.log(9, f"{self.symbol}: Successfully wrote new buy order to rules")

    # gets rule for this symbol
    def get_rule(self):
        stored_rules = utils.get_rules(store=self.store, back_testing=self.back_testing)

        for this_rule in stored_rules:
            if this_rule["symbol"] == self.symbol:
                return this_rule

        return False

    # removes the symbol from the buy rules in store
    def _remove_from_rules(self):
        stored_state = utils.get_rules(store=self.store, back_testing=self.back_testing)
        found_in_rules = False

        new_rules = []

        for this_rule in stored_state:
            if this_rule["symbol"] == self.symbol:
                found_in_rules = True
            else:
                # not the rule we're looking to remove, so retain it
                new_rules.append(this_rule)

        if found_in_rules:
            utils.put_rules(
                symbol=self.symbol,
                store=self.store,
                new_rules=new_rules,
                back_testing=self.back_testing,
            )
            log_wp.log(9, f"{self.symbol}: Successfully wrote updated rules")
            return True
        else:
            log_wp.warning(
                f"{self.symbol}: Tried to remove symbol from rules but did not find it"
            )
            return False

    # END STATE AND RULE FUNCTIONS

    # START BAR FUNCTIONS
    def _get_bars(self, from_date=None, to_date=None, initialised: bool = True):
        saved_data = False
        if initialised == False:
            # we actually need to grab everything
            # first check to see if we have any data in s3
            saved_bars = utils.load_bars([self.symbol])[self.symbol]
            if type(saved_bars) == pd.core.frame.DataFrame:
                yf_start = saved_bars.index[-1]
                saved_data = True
            else:
                yf_start = datetime.now() - self.max_range
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
        bars = yf.Ticker(self.symbol).history(
            start=yf_start,
            interval=self.interval,
            actions=False,
        )

        bars = bars.tz_convert(pytz.utc)

        if saved_data:
            bars = utils.merge_bars(saved_bars, bars)

        if len(bars) == 0:
            # something went wrong - usually bad symbol and search parameters
            log_wp.debug(
                f"{self.symbol}: No data returned for start {yf_start} end {yf_end}"
            )
            return bars

        interval_mod = utils.get_interval_integer(self.interval)

        trimmed_new_bars = bars.loc[
            (bars.index.minute % interval_mod == 0) & (bars.index.second == 0)
        ]
        # if len(bars) != len(trimmed_new_bars):
        #    print("banana")

        bars = trimmed_new_bars

        # bars = bars.loc[bars.index <= yf_end]

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

        # trimmed_new_bars = new_bars.loc[
        #    (new_bars.index.minute % 5 == 0) & (new_bars.index.second == 0)
        # ]
        # if len(new_bars) != len(trimmed_new_bars):
        #    print("banana")

        # new_bars = trimmed_new_bars

        if len(new_bars) > 0:
            # pad new bars to 200 rows so that macd and sma200 work
            # TODO merge these bars in before running add_signals - this way we're doing signals for only a couple rows instead of 200
            if len(new_bars) < 300:
                new_bars = utils.merge_bars(
                    new_bars=new_bars, bars=self.bars.iloc[-300:]
                )

            new_bars = utils.add_signals(new_bars, interval=self.interval)
            self.bars = utils.merge_bars(self.bars, new_bars)

            if self.back_testing:
                self.api._put_bars(symbol=self.symbol, bars=self.bars)

        else:
            log_wp.debug(f"{self.symbol}: No new data since {from_date}")

    # END  BAR FUNCTIONS

    # START CHECK_STATE FUNCTIONS
    def check_state_no_position_taken(self):
        # get iloc of analyse_index

        bars_slice = self.get_data_window()

        if len(bars_slice) < 200:
            print("banana")

        # check to see if the signal was found in the last record in bars_slice
        buy_signal_found = utils.check_buy_signal(df=bars_slice, symbol=self.symbol)

        # if we found a buy signal, return the transition function to run
        if buy_signal_found:
            # first how much cash do we have to spend?
            account = self.api.get_account()
            balance = account.assets["USD"]

            # next check precision on order - normal stocks are only to the thousandth, crypto is huge
            precision = self.api.get_precision(yf_symbol=self.symbol)

            buy_plan = BuyPlan(symbol=self.symbol, df=bars_slice, precision=precision)

            if balance <= buy_plan.entry_unit:
                log_wp.info(
                    f"{self.symbol}: Found buy signal, but insufficient balance to execute. Balance {balance} vs unit price {buy_plan.entry_unit} - skipping"
                )
                return False

            if BuyPlan.ORDER_SIZE < buy_plan.entry_unit:
                log_wp.info(
                    f"{self.symbol}: Found buy signal, but price {buy_plan.entry_unit} exceeds BuyOrder max size {BuyPlan.ORDER_SIZE} - skipping"
                )
                return False

            self.buy_plan = buy_plan
            log_wp.debug(
                f"{self.symbol}: Found buy signal, next step is trans_entering_position"
            )
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
            log_wp.debug(
                f"{self.symbol}: Order {order.order_id}: filled, next action is trans_buy_order_filled"
            )
            return self.trans_buy_order_filled
        elif order.status_summary == "open" or order.status_summary == "pending":
            # check timeout
            if self._is_position_timed_out(now=self._analyse_date, order=order):
                # transition back to no position taken
                log_wp.debug(
                    f"{self.symbol}: Order {order.order_id}: has timed out, next action is trans_buy_order_timed_out"
                )
                return self.trans_buy_order_timed_out
            log_wp.log(
                9, f"{self.symbol}: Order {order.order_id}: is still open or pending"
            )

        # do nothing - still open, not timedout
        log_wp.debug(
            f"{self.symbol}: Order {order.order_id} is still open but not filled. Last High was {self.bars.High.loc[self._analyse_date]} last Low was {self.bars.Low.loc[self._analyse_date]}, limit price is {order.ordered_unit_price}"
        )
        return False

    def check_state_position_taken(self):
        self.position = self.api.get_position(symbol=self.symbol)

        # position liquidated
        if self.position.quantity == 0:
            log_wp.debug(
                f"{self.symbol}: 0 units held, assuming that position has been externally liquidated"
            )
            return self.trans_externally_liquidated

        # get inputs for next checks
        last_close = self.bars.Close.loc[self._analyse_date]
        self.active_rule = self.get_rule()
        # utils.get_rules(store=self.store, back_testing=self.back_testing)

        # for some reason there is no rule for this - we're lost, so stop loss and punch out - should never happen
        if not self.active_rule:
            log_wp.critical(
                f"{self.symbol}: Can't find rule for this position, next action is trans_position_taken_to_stop_loss"
            )
            return self.trans_position_taken_to_stop_loss

        # stop loss hit?
        stop_loss = self.active_rule["current_stop_loss"]
        if last_close < stop_loss:
            log_wp.warning(
                f"{self._analyse_date} {self.symbol}: Stop loss hit, next action is trans_position_taken_to_stop_loss"
            )
            return self.trans_position_taken_to_stop_loss

        # otherwise move straight on to take profit
        log_wp.debug(
            f"{self.symbol}: Position established, next action is trans_take_profit"
        )
        return self.trans_take_profit

    def check_state_take_profit(self):
        if self._back_testing_date == pd.Timestamp("2022-05-19 21:45:00+00:00"):
            print("basd")
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
            # do we have any units left?
            if self.position.quantity == 0:
                # nothing left to sell
                log_wp.warning(
                    f"{self.symbol}: No units still held, next action is trans_close_position"
                )
                return self.trans_close_position
            else:
                # still some left to sell, so transition back to same state
                log_wp.debug(
                    f"{self.symbol}: Units still held, next action is trans_take_profit_again"
                )
                return self.trans_take_profit_again

        # position liquidated but not using our fill order
        if self.position.quantity == 0:
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: No units held but liquidated outside of this sell order, next action is trans_externally_liquidated"
            )
            return self.trans_externally_liquidated

        # for some reason there is no rule for this - we're lost, so stop loss and punch out - should never happen
        if not self.active_rule:
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Can't find rule for this position, next action is trans_take_profit_to_stop_loss"
            )
            return self.trans_take_profit_to_stop_loss

        # stop loss hit?
        stop_loss = self.active_rule["current_stop_loss"]
        if last_close < stop_loss:
            log_wp.warning(
                f"{self._analyse_date} {self.symbol}: Stop loss hit (last close {round(last_close,2)} < stop loss {round(stop_loss,2)}), next action is trans_take_profit_to_stop_loss"
            )
            return self.trans_take_profit_to_stop_loss

        if order.status_summary == "cancelled":
            # the order got cancelled for some reason. we still have a position, so try to re-raise it
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Sell order was cancelled for some reason (maybe be broker?), so trying to re-raise it. Next action is trans_take_profit_retry"
            )
            return self.trans_take_profit_retry

        # nothing to do
        return False

    def check_state_stop_loss(self):
        position = self.api.get_position(symbol=self.symbol)

        # get inputs for next checks
        # rules = utils.get_rules(store=self.store, back_testing=self.back_testing)
        self.active_rule = self.get_rule()

        # for some reason there is no rule for this - we're lost, so stop loss and punch out - should never happen
        if not self.active_rule:
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Can't find rule for this position, next action is trans_take_profit_to_stop_loss"
            )
            return self.trans_position_taken_to_stop_loss

        # get order
        order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._back_testing_date
        )

        if order.status_summary == "cancelled":
            # the order got cancelled for some reason. we still have a position, so try to re-raise it
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Sell order was cancelled for some reason (maybe be broker?), so trying to re-raise it. Next action is trans_take_profit_retry"
            )
            return self.trans_stop_loss_retry

        elif order.status_summary == "filled":
            # stop loss got filled, now need to fully close position
            log_wp.info(
                f"{self._analyse_date} {self.symbol}: No units still held, next action is trans_close_position"
            )
            return self.trans_close_position

        elif order.status_summary == "open" or order.status_summary == "pending":
            # is the order still open but we don't own any? if so, it got liquidated outside of this process
            if position.quantity == 0:
                log_wp.critical(
                    f"{self._analyse_date} {self.symbol}: No units held but liquidated outside of this sell order, next action is trans_externally_liquidated"
                )
                return self.trans_externally_liquidated

            # nothing to do
            log_wp.debug(
                f"{self._analyse_date} {self.symbol}: Stop loss order still open, no next action"
            )
            return False

    # END  CHECK_STATE FUNCTIONS

    # START TRANSITION FUNCTIONS
    def trans_entering_position(self):
        # submit buy order
        log_wp.debug(f"{self.symbol}: Started trans_entering_position")

        self.play_id = "play-" + utils.generate_id()

        order_result = self.api.buy_order_limit(
            symbol=self.symbol,
            units=self.buy_plan.units,
            unit_price=self.buy_plan.entry_unit,
            back_testing_date=self._back_testing_date,
        )

        open_statuses = ["open", "filled"]
        if order_result.status_summary not in open_statuses:
            log_wp.error(
                f"{self._analyse_date} {self.symbol}: Failed to submit buy order {order_result.order_id}: {order_result.status_text}"
            )
            return False

        # hold on to order ID
        self.active_order_id = order_result.order_id

        self.bot_telemetry.add_order(order_result, play_id=self.play_id)

        # write state
        self._write_to_state(order_result)

        # set self.current_check to check_position_taken
        self.current_check = self.check_state_entering_position

        log_wp.warning(
            f"{self._analyse_date} {self.symbol}: Buy order {order_result.order_id} (state {order_result.status_summary}) at unit price {order_result.ordered_unit_price} submitted"
        )
        log_wp.debug(f"{self.symbol}: Finished trans_entering_position")
        return True

    def trans_buy_order_timed_out(self):
        # get state
        state = self.get_state()

        if state == False:
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Unable to find order for this symbol in state! There may be an unmanaged buy order in the market!"
            )
        else:
            # cancel order
            log_wp.info(f"{self.symbol}: Deleting order")
            order_result = self.api.cancel_order(
                order_id=state["order_id"], back_testing_date=self._analyse_date
            )

        self.bot_telemetry.add_order(order_result, self.play_id)

        # clear any variables set at symbol
        self.active_order_id = None
        self.buy_plan = None
        self.play_id = None

        # clear state
        self._remove_from_state()

        # set current check
        self.current_check = self.check_state_no_position_taken

        return True

    def trans_buy_order_cancelled(self):
        # get state
        state = self.get_state()

        if state == False:
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Unable to find order for this symbol in state! May be an orphaned buy order!"
            )
        else:
            # no need to cancel order - it already got nuked
            ...

        order_result = self.api.cancel_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        self.bot_telemetry.add_order(order_result, self.play_id)

        # clear any variables set at symbol
        self.active_order_id = None
        self.buy_plan = None
        self.play_id = None

        # clear state
        self._remove_from_state()

        # set current check
        self.current_check = self.check_state_no_position_taken

        return True

    def trans_buy_order_filled(self):
        # clear state
        self._remove_from_state()

        # add rule
        self._write_to_rules(
            buy_plan=self.buy_plan, order_result=self.active_order_result
        )

        order_result = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        self.bot_telemetry.add_order(order_result, self.play_id)

        # update active_order_id
        self.active_order_id = None

        # set current check
        self.current_check = self.check_state_position_taken

        return True

    def trans_take_profit(self):
        # self.active_rule already set in check phase
        # self.position already set in check phase

        # raise sell order
        pct = self.active_rule["risk_point_sell_down_pct"]
        units = self.position.quantity

        units_to_sell = floor(pct * units)

        order = self.api.sell_order_limit(
            symbol=self.symbol,
            units=units_to_sell,
            unit_price=self.buy_plan.target_price,
            back_testing_date=self._back_testing_date,
        )

        self.bot_telemetry.add_order(order, self.play_id)

        # hold on to active_order_id
        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_take_profit

        return True

    def trans_take_profit_to_stop_loss(self):
        # self.position already held from check
        # self.active_order_id already held from check

        # cancel take profit order
        cancelled_order = self.api.cancel_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        log_wp.warning(
            f"{self.symbol}: Successfully cancelled take_profit order {cancelled_order.order_id}"
        )

        self.bot_telemetry.add_order(cancelled_order, self.play_id)

        # submit stop loss
        order = self.api.sell_order_market(
            symbol=self.symbol,
            units=self.position.quantity,
            back_testing_date=self._back_testing_date,
        )

        if order.status_summary == "cancelled":
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Unable to submit stop loss order for symbol! API returned {order.status_text}"
            )
            return False

        log_wp.warning(
            f"{self.symbol}: Successfully submitted stop_loss order {order.order_id}"
        )
        self.bot_telemetry.add_order(order, self.play_id)

        # update active_order_id
        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_stop_loss

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
                f"{self._analyse_date} {self.symbol}: Unable to submit stop loss order for symbol! API returned {order.status_text}"
            )
            return False

        self.bot_telemetry.add_order(order, self.play_id)

        # update active_order_id
        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_stop_loss

        return True

    def trans_externally_liquidated(self):
        order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        self.bot_telemetry.add_order(order, self.play_id)

        # already don't hold any, so no need to delete orders
        # just need to clean up the object and delete rules
        self._remove_from_rules()
        self.active_order_id = None
        self.active_order_result = None
        self.buy_plan = None
        self.play_id = None

        # TODO add to win/loss as unknown outcome

        self.current_check = self.check_state_no_position_taken

    def trans_close_position(self):
        order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        self.bot_telemetry.add_order(order, self.play_id)

        # clear active order details
        self.active_order_id = None
        self.active_order_result = None
        self.buy_plan = None
        self.play_id = None

        # delete rules
        self._remove_from_rules()

        # TODO add to win/loss

        # set check
        self.current_check = self.check_state_no_position_taken

    def trans_stop_loss_retry(self):
        # our take profit order was cancelled for some reason
        # i'm not sure what i want to do here actually. this needs more thought than just spamming new orders

        # no need to close the previous order - its dead
        order = self.api.close_position(
            symbol=self.symbol, back_testing_date=self._back_testing_date
        )

        self.bot_telemetry.add_order(order, self.play_id)

        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_take_profit

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

        self.bot_telemetry.add_order(order, self.play_id)

        # hold on to active_order_id
        self.active_order_id = order.order_id

        # set current check
        self.current_check = self.check_state_take_profit

        return True

    def trans_take_profit_again(self):
        # our old take profit order was filled, so need to raise a new one
        # no need to check if we still have a position - got checked at check stage

        # self.active_rule already set in check phase
        # self.position already set in check phase

        filled_order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._back_testing_date
        )
        filled_value = (
            filled_order.filled_unit_quantity * filled_order.filled_unit_price
        )
        self.bot_telemetry.add_order(filled_order, self.play_id)
        log_wp.warning(
            f"{self._analyse_date} {self.symbol}: Successfully took profit: order ID {filled_order.order_id} sold {filled_order.filled_unit_quantity} at {filled_order.filled_unit_price} for value {filled_value}"
        )

        # raise sell order
        pct = self.active_rule["risk_point_sell_down_pct"]
        units = self.position.quantity

        units_to_sell = floor(pct * units)

        # units_to_sell might be less than 1 whole unit - in this case, just sell 1 unit? TODO nominal/fractional shares
        if units_to_sell == 0:
            units_to_sell = 1

        new_steps = self.active_rule["steps"] + 1
        new_target_profit = self.active_rule["original_risk"] * new_steps
        new_target_unit_price = (
            self.active_rule["current_target_price"] + new_target_profit
        )

        order = self.api.sell_order_limit(
            symbol=self.symbol,
            units=units_to_sell,
            unit_price=new_target_unit_price,
            back_testing_date=self._back_testing_date,
        )

        # hold on to active_order_id
        self.active_order_id = order.order_id

        # update rules
        new_sales_obj = {
            "units": filled_order.filled_unit_quantity,
            "sale_price": filled_order.filled_unit_price,
        }
        new_units_held = self.api.get_position(symbol=self.symbol).quantity
        new_units_sold = (
            self.active_rule["units_sold"] + filled_order.filled_unit_quantity
        )

        new_rule = self.active_rule
        new_stop_loss = new_target_unit_price * new_rule["risk_point_new_stop_loss_pct"]
        new_rule["current_stop_loss"] = new_stop_loss
        new_rule["current_risk"] = new_target_profit
        new_rule["sales"].append(new_sales_obj)
        new_rule["units_held"] = new_units_held
        new_rule["units_sold"] = new_units_sold
        new_rule["steps"] += new_steps
        new_rule["current_target_price"] = new_target_unit_price
        new_rule["play_id"] = self.play_id

        if not self._replace_rule(new_rule=new_rule):
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Failed to update rules with new rule! Likely orphaned order"
            )

        self.bot_telemetry.add_order(order, self.play_id)

        # set current check
        self.current_check = self.check_state_take_profit

        new_value = order.ordered_unit_quantity * order.ordered_unit_price
        log_wp.warning(
            f"{self._analyse_date} {self.symbol}: Successfully lodged new take profit: order ID {order.order_id} (state {order.status_summary}) to sell {order.ordered_unit_quantity} unit at {round(order.ordered_unit_price,2)} for value {round(new_value,2)} with new stop loss {round(new_stop_loss,2)}"
        )
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
