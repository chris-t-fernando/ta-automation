# external packages
from datetime import datetime
import logging
from math import floor
import pandas as pd
import pytz

# my modules
from buyplan import BuyPlan
from itradeapi import (
    ITradeAPI,
    MARKET_BUY,
    MARKET_SELL,
    LIMIT_BUY,
    LIMIT_SELL,
    STOP_LIMIT_BUY,
    STOP_LIMIT_SELL,
)
from inotification_service import INotificationService
import utils

log_wp = logging.getLogger(
    "stock_symbol"
)  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("stock_symbol.log")
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
class Symbol:
    def __init__(
        self,
        symbol: str,
        api: ITradeAPI,
        interval: str,
        store,
        bot_telemetry,
        market_data_source,
        notification_service: INotificationService,
        real_money_trading: bool = False,
        to_date: str = None,
        back_testing: bool = False,
        back_testing_skip_bar_update: bool = False,
    ):
        # can't easily map alpaca and YFinance
        # UNI-USD in unicorn token, in alpaca UNIUSD is Uniswap token - different tokens
        if symbol == "UNI-USD":
            raise SymbolForbidden(f"Cannot map Uniswap token between Alpaca (UNIUSD) and Yahoo Finance (UNI3-USD). Don't use it.")

        self.back_testing = back_testing
        self.back_testing_skip_bar_update = back_testing_skip_bar_update
        self.bot_telemetry = bot_telemetry
        self.symbol = symbol
        self.api = api
        self.interval = interval
        self.real_money_trading = real_money_trading
        self.store = store
        self.market_data_source = market_data_source
        self.initialised = False
        self.notification_service = notification_service
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
            to_date=to_date,
            initialised=False,
        )

        if len(bars) == 0:
            self.bars = []
            self._init_complete = False
        else:
            self.bars = utils.add_signals(bars, interval)
            self._init_complete = True

    def get_market(self):
        # there's the right way and the fast way to do this
        # we're doing the fast way
        if self.symbol[-4:] == "-USD":
            return "ccc_market"
        else:
            return "us_market"

        return
        self.market = self.market_data_source.Ticker("ACN").info["market"]

    def _set_order_size_and_increment(self):
        asset = self.api.get_asset(symbol=self.symbol)
        if hasattr(asset, "min_order_size"):
            self.min_order_size = float(asset.min_order_size)
            self.min_trade_increment = float(asset.min_trade_increment)
            self.min_price_increment = float(asset.min_price_increment)
        else:
            self.min_order_size = 1
            self.min_trade_increment = 1
            self.min_price_increment = 0.001

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

            # a loop can happen while backtesting where:
            # we do a buy order
            # then a take profit gets triggered because 1.25% of risk/gain is less than current High
            # then the new take profit stop loss is set to 99% of that, which is lower than the Low of that same cycle
            # so it does a stop loss
            # then after it does a stop loss it goes back to searching for a buy signal in the same cycle
            # i think this loop should break if we've done a stop loss/order cancellation
            if self.back_testing and next_transition == self.trans_take_profit_again:
                log_wp.info(
                    f"{self.symbol}: Finished trans_take_profit_again. Finishing this cycle of the state machine"
                )
                break

            if self.back_testing and next_transition == self.trans_close_position:
                log_wp.info(
                    f"{self.symbol}: Finished trans_close_position. Finishing this cycle of the state machine"
                )
                break

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
                "state": STATE_MAP_INVERTED[self.state_const],
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
        yf_end = None

        if self.back_testing and self.back_testing_skip_bar_update:
            saved_bars = utils.load_bars([self.symbol])[self.symbol]
            # this means we got data from s3
            if type(saved_bars) == pd.core.frame.DataFrame:
                yf_start = saved_bars.index[-1]
                saved_data = True
                bars = saved_bars
            else:
                raise RuntimeError(
                    f"back_testing is True and back_testing_skip_bar_updates is True, but we have no data for {self.symbol}"
                )

        else:
            if initialised == False:
                # we actually need to grab everything
                # first check to see if we have any data in s3
                saved_bars = utils.load_bars([self.symbol])[self.symbol]

                # this means we got data from s3
                if type(saved_bars) == pd.core.frame.DataFrame:
                    yf_start = saved_bars.index[-1]
                    saved_data = True
                else:
                    # this means we didn't get data from s3
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
                debug_end = "now"
            else:
                debug_end = yf_end

            log_wp.warning(
                f"{self.symbol}: No data returned between {yf_start} til {debug_end}"
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
            log_wp.log(9, f"{self.symbol}: No new data since {from_date}")

    # END  BAR FUNCTIONS

    # START CHECK_STATE FUNCTIONS
    def check_state_no_position_taken(self):
        # get iloc of analyse_index

        # TODO - if the last data is too far in the past, bail out here!
        bars_slice = self.get_data_window()

        if len(bars_slice) < 200:
            log_wp.warning(
                f"{self.symbol}: Less than 200 bar samples - high probability of exception/error so bailing out"
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

            buy_plan = BuyPlan(
                symbol=self.symbol,
                df=bars_slice,
                balance=balance,
                precision=self.precision,
                min_trade_increment=self.min_trade_increment,
                min_order_size=self.min_order_size,
                min_price_increment=self.min_price_increment,
            )

            if not buy_plan.success:
                if buy_plan.error_message == "entry_larger_than_order_size":
                    log_wp.info(
                        f"{self.symbol}: Found buy signal, but max play size is {buy_plan.max_play_value} vs entry price of {buy_plan.entry_unit}"
                    )
                    return False
                elif buy_plan.error_message == "min_order_size":
                    log_wp.info(
                        f"{self.symbol}: Found buy signal, but insufficient balance to purchase at least the minimum order quantity. Balance {balance}, minimum order size {self.min_order_size}, minimum order value {self.min_order_size * buy_plan.entry_unit}"
                    )
                    return False
                elif buy_plan.error_message == "zero_units":
                    log_wp.info(
                        f"{self.symbol}: Found buy signal, but insufficient balance to purchase at least one unit. Balance {balance}, entry unit price {buy_plan.entry_unit}"
                    )
                    return False
                elif buy_plan.error_message == "insufficient_balance":
                    log_wp.info(
                        f"{self.symbol}: Found buy signal, but insufficient balance to purchase at least one unit. Balance {balance}, entry unit price {buy_plan.entry_unit}"
                    )
                    return False
                elif buy_plan.error_message == "stop_unit_too_high":
                    log_wp.info(
                        f"{self.symbol}: Found buy signal, but Stop Loss {buy_plan.stop_unit} would already trigger at last Low price of {buy_plan.last_low}"
                    )
                    return False
                elif buy_plan.error_message == "last_high_too_low":
                    log_wp.info(
                        f"{self.symbol}: Found buy signal, but first Take Profit step {buy_plan.entry_unit * 1.25} would already trigger at last High price of {buy_plan.last_high}"
                    )
                    return False
                else:
                    log_wp.critical(
                        f"{self.symbol}: Found buy signal, but something went wrong in buy_plan. Balance {balance}, entry unit price {buy_plan.entry_unit}"
                    )
                    return False

            self.buy_plan = buy_plan
            log_wp.info(
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
            self.notification_service.send(
                message=f"MACD algo took profit on {self.symbol} | {order.filled_unit_price} sold price | {order.filled_unit_quantity} units sold | {order.filled_unit_quantity * order.filled_unit_price} total sale value",
            )

            # do we have any units left?
            if self.position.quantity == 0:
                # nothing left to sell
                self.notification_service.send(
                    message=f"I don't hold any more units of {self.symbol}. Play complete",
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
                message=f"MACD algo terminated for {self.symbol}. Hold no units but I didn't liquidate them. Did these units get liquidated outside of my workflow?",
            )
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: No units held but liquidated outside of this sell order, next action is trans_externally_liquidated"
            )
            return self.trans_externally_liquidated

        # for some reason there is no rule for this - we're lost, so stop loss and punch out - should never happen
        if not self.active_rule:
            self.notification_service.send(
                message=f"MACD algo terminated for {self.symbol}. The rules for this play (stop loss etc) can't found in state storage. You need to liquidate this holding manually.",
            )
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Can't find rule for this position, next action is trans_take_profit_to_stop_loss"
            )
            return self.trans_take_profit_to_stop_loss

        # stop loss hit?
        stop_loss = self.active_rule["current_stop_loss"]
        if last_close < stop_loss:
            # no notification required here - will be handled in trans_ method
            log_wp.warning(
                f"{self._analyse_date} {self.symbol}: Stop loss hit (last close {round(last_close,2)} < stop loss {round(stop_loss,2)}), next action is trans_take_profit_to_stop_loss"
            )
            return self.trans_take_profit_to_stop_loss

        if order.status_summary == "cancelled":
            # the order got cancelled for some reason. we still have a position, so try to re-raise it
            # no notification required here - will be handled in trans_ method
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
        log_wp.log(9, f"{self.symbol}: Started trans_entering_position")

        self.play_id = "play-" + self.symbol + utils.generate_id()

        order_result = self.api.buy_order_limit(
            symbol=self.symbol,
            units=self.buy_plan.units,
            unit_price=self.buy_plan.entry_unit,
            back_testing_date=self._back_testing_date,
        )

        accepted_statuses = ["open", "filled", "pending"]
        if order_result.status_summary not in accepted_statuses:
            log_wp.error(
                f"{self._analyse_date} {self.symbol}: Failed to submit buy order {order_result.order_id}: {order_result.status_text}"
            )

            return self.trans_buy_order_cancelled

        # hold on to order ID
        self.active_order_id = order_result.order_id

        self.bot_telemetry.add_order(order_result=order_result, play_id=self.play_id)

        self.state_const = BUY_LIMIT_ORDER_ACTIVE

        # write state
        self._write_to_state(order_result)

        # set self.current_check to check_position_taken
        self.current_check = self.check_state_entering_position

        log_wp.warning(
            f"{self._analyse_date} {self.symbol}: Buy order {order_result.order_id} (state {order_result.status_summary}) at unit price {order_result.ordered_unit_price} submitted"
        )
        log_wp.log(9, f"{self.symbol}: Finished trans_entering_position")
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

            self.bot_telemetry.add_order(
                order_result=order_result, play_id=self.play_id
            )

        # clear any variables set at symbol
        self.active_order_id = None
        self.buy_plan = None
        self.play_id = None

        # clear state
        self._remove_from_state()

        # set current check
        self.current_check = self.check_state_no_position_taken

        self.state_const = NO_POSITION_TAKEN

        return True

    def trans_buy_order_cancelled(self):
        # get state
        state = self.get_state()

        if state == False:
            log_wp.warning(
                f"{self._analyse_date} {self.symbol}: Unable to find order for this symbol in state! May be an orphaned buy order!"
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
        self._remove_from_state()

        # set current check
        self.current_check = self.check_state_no_position_taken
        self.state_const = NO_POSITION_TAKEN

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
        self.bot_telemetry.add_order(order_result=order_result, play_id=self.play_id)

        # update active_order_id
        self.active_order_id = None

        # set current check
        self.current_check = self.check_state_position_taken

        self.notification_service.send(
            message=f"MACD algo took position in {self.symbol} | {self.buy_plan.entry_unit} entry price | {self.buy_plan.stop_unit} stop loss price | {self.buy_plan.units} units bought",
        )

        self.state_const = POSITION_TAKEN

        return True

    def trans_take_profit(self):
        # self.active_rule already set in check phase
        # self.position already set in check phase

        # raise sell order
        pct = self.active_rule["risk_point_sell_down_pct"]
        units = self.position.quantity

        # TODO: THIS BIT IS BUSTED AND NEEDS FIXING ASAP
        units_to_sell = floor(pct * units)
        if units_to_sell == 0:
            units_to_sell = 1

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
                f"{self._analyse_date} {self.symbol}: Unable to submit stop loss order for symbol! API returned {order.status_text}"
            )
            return False

        log_wp.warning(
            f"{self.symbol}: Successfully submitted stop_loss order {order.order_id}"
        )
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
                f"{self._analyse_date} {self.symbol}: Unable to submit stop loss order for symbol! API returned {order.status_text}"
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
        order = self.api.get_order(
            order_id=self.active_order_id, back_testing_date=self._analyse_date
        )
        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # already don't hold any, so no need to delete orders
        # just need to clean up the object and delete rules
        self._remove_from_rules()
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
        self._remove_from_rules()

        # TODO add to win/loss

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
        filled_value = (
            filled_order.filled_unit_quantity * filled_order.filled_unit_price
        )
        self.bot_telemetry.add_order(order_result=filled_order, play_id=self.play_id)
        log_wp.warning(
            f"{self._analyse_date} {self.symbol}: Successfully took profit: order ID {filled_order.order_id} sold {filled_order.filled_unit_quantity} at {filled_order.filled_unit_price} for value {filled_value}"
        )

        updated_plan = self.buy_plan.take_profit(
            filled_order=filled_order,
            active_rule=self.active_rule,
            new_position_quantity=self.position.quantity,
        )

        if updated_plan["new_target_unit_price"] == None:
            print("banana")

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

        if not self._replace_rule(new_rule=new_rule):
            log_wp.critical(
                f"{self._analyse_date} {self.symbol}: Failed to update rules with new rule! Likely orphaned order"
            )

        self.notification_service.send(
            message=f"MACD algo took profit on {self.symbol} | {filled_order.filled_unit_price} sold price | {filled_order.filled_unit_quantity} units sold | {filled_order.filled_unit_quantity * filled_order.filled_unit_price} total sale value",
        )
        self.notification_service.send(
            message=f"I still hold {new_units_held} units of {self.symbol} | {new_target_unit_price} is new target price | {new_stop_loss} is new stop loss",
        )

        self.bot_telemetry.add_order(order_result=order, play_id=self.play_id)

        # set current check
        self.current_check = self.check_state_take_profit

        new_value = order.ordered_unit_quantity * order.ordered_unit_price
        log_wp.warning(
            f"{self._analyse_date} {self.symbol}: Successfully lodged new take profit: order ID {order.order_id} (state {order.status_summary}) to sell {order.ordered_unit_quantity} unit at {round(order.ordered_unit_price,2)} for value {round(new_value,2)} with new stop loss {round(new_stop_loss,2)}"
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
