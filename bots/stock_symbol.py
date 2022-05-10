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
        real_money_trading: bool,
        ssm,
        data_source,
        to_date: str = None,
        back_testing: bool = False,
    ):
        self.symbol = symbol
        self.api = api
        self.interval = interval
        self.real_money_trading = real_money_trading
        self.ssm = ssm
        self.data_source = data_source
        self.initialised = False
        self.last_date_processed = None
        self.interval_delta, self.max_range = utils.get_interval_settings(self.interval)

        bars = self._get_bars(
            to_date=to_date,
            initialised=False,
        )
        self.bars = utils.add_signals(bars, interval)
        self.back_testing = back_testing

        # when raising an initial buy order, how long should we wait for it to be filled til killing it?
        self.enter_position_timeout = self.interval_delta

        # pointer to current record to assess
        self.current_record = None

    # returns True/False depending on whether an OrderResult order has passed the configured timeout window
    def enter_position_timed_out(self, now, order):
        cutoff_date = now.astimezone(pytz.utc) - self.enter_position_timeout
        cutoff_date = cutoff_date.astimezone(pytz.utc)
        if order.create_time < cutoff_date:
            return True
        return False

    # writes the symbol to state
    def _write_to_state(self, order):
        stored_state = utils.get_stored_state(ssm=self.ssm, back_testing=self.back_testing)
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
            }
        )
        utils.put_stored_state(
            ssm=self.ssm, new_state=new_state, back_testing=self.back_testing
        )

        log_wp.info(f"{self.symbol}: Successfully wrote order to state")

    # removes this symbol from the state
    def _remove_from_state(self):
        stored_state = utils.get_stored_state(ssm=self.ssm, back_testing=self.back_testing)
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
            ssm=self.ssm, new_state=new_state, back_testing=self.back_testing
        )

        if found_in_state:
            log_wp.debug(f"{self.symbol}: Successfully wrote updated state")
            return True
        else:
            log_wp.warning(
                f"{self.symbol}: Tried to remove symbol from state but did not find it"
            )
            return False

    def _write_to_rules(self, buy_plan, order_result):
        new_rule = {
            "symbol": buy_plan.symbol,
            "original_stop_loss": buy_plan.stop_unit,
            "current_stop_loss": buy_plan.stop_unit,
            "original_target_price": buy_plan.target_price,
            "current_target_price": buy_plan.target_price,
            "steps": 0,
            "original_risk": buy_plan.risk_unit,
            "current_risk": buy_plan.risk_unit,
            "purchase_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "purchase_price": buy_plan.entry_unit,
            "units_held": buy_plan.units,  # TODO
            "units_sold": 0,
            "units_bought": buy_plan.units,
            "order_id": 0,  # TODO
            "sales": [],
            "win_point_sell_down_pct": 0.5,
            "win_point_new_stop_loss_pct": 0.99,
            "risk_point_sell_down_pct": 0.25,
            "risk_point_new_stop_loss_pct": 0.98,
        }
        raise ValueError()

    # removes the symbol from the buy rules in SSM
    def _remove_from_rules(self):
        stored_state = utils.get_rules(ssm=self.ssm, back_testing=self.back_testing)
        found_in_rules = False

        new_rules = []

        for this_rule in stored_state:
            if this_rule["symbol"] == self.symbol:
                found_in_rules = True
            else:
                # not the rule we're looking to remove, so retain it
                new_rules.append(this_rule)

        utils.put_rules(ssm=self.ssm, new_state=new_rules, back_testing=self.back_testing)

        if found_in_rules:
            log_wp.debug(f"{self.symbol}: Successfully wrote updated rules")
        else:
            log_wp.warning(
                f"{self.symbol}: Tried to remove symbol from rules but did not find it"
            )

    # used to clean up state before we actually enter no_position_taken
    def trans_no_position_taken(self, reason: str, order):
        if reason == "timeout":
            # kill the job, remove state
            self.api.delete_order(order_id=order.order_id)
            self._remove_from_state()

        elif reason == "cancelled":
            # job is already killed, but still need to remove state
            self._remove_from_state()
        elif reason == "stop_loss":
            # stop loss hit, remove from state and remove from rules
            self._remove_from_state()
            self._remove_from_rules()
        else:
            raise ValueError(f"Unknown reason: {reason}")

        # now we are done cleaning up, set this symbol to no position taken
        self.load_state_no_position_taken()

    # used to clean up state before we actually enter position_taken
    def trans_position_taken(self):
        # buy order got closed, so clean up reference to it in symbol, remove from state, add to rules
        self._remove_from_state()
        # need to work out what to write in to the rules!
        self.generate_play()

        ...

    # when we have found a signal and raise a buy order
    def trans_buy_limit_order_active(self, buy_plan):
        # buy_plan should tell us everything we need to know about the play
        # first raise the buy request
        # if its accepted, write it to state
        order_result = self.api.buy_order_limit(
            symbol=self.symbol,
            units=buy_plan.units,
            unit_price=buy_plan.entry_unit,
        )

        if not order_result.success:
            raise RuntimeError(f"Buy order was rejected: {order_result.status_text}")

        # add the buy order to state
        self._write_to_state()

        # was it filled already?
        if order_result.status_summary == "filled":
            # skip straight to position taken
            self.trans_position_taken()

    # set this symbol to no position taken
    def load_state_no_position_taken(self):
        # this one is pretty simple - just do it
        self.state = NO_POSITION_TAKEN

    def load_state_buy_limit_order_active(self, order):
        self.state = BUY_LIMIT_ORDER_ACTIVE
        self.active_order_id = order.order_id

    def set_stored_state(self, stored_state):
        requested_state = STATE_MAP[stored_state["state"]]

        if requested_state == NO_POSITION_TAKEN:
            # not much to do here - blank slate
            self.load_state_no_position_taken()

        elif requested_state == BUY_LIMIT_ORDER_ACTIVE:
            # we have previously found a signal and put out a buy order
            # so we need to get that buy order
            order = self.api.get_order(order_id=stored_state["order_id"])

            if order == None:
                raise RuntimeError(
                    f'Buy order in state not found on broker! Symbol {self.symbol}, order ID {stored_state["order_id"]} on broker {self.api.get_broker_name()}'
                )

            # now we have the order object, check whether it's been filled since last run - this means we've actually moved to another state
            if order.status_summary == "open" or order.status_summary == "pending":
                # its still open - now check if it has timed out
                if self.enter_position_timed_out(now=datetime.now(), order=order):
                    # its timed out, so transition back to no position
                    self.trans_no_position_taken(reason="timeout", order=order)
                else:
                    # stored state said we had a buy order active, the order is still open and it hasn't timed out - so its still valid
                    self.load_state_buy_limit_order_active(order=order)
            elif order.status_summary == "cancelled":
                # state had the order open but now its cancelled - so we need to go back to no position taken
                self.trans_no_position_taken(reason="cancelled", order=order)
            elif order.status_summary == "filled":
                # state had the order open but now its filled - move to position taken
                self.trans_position_taken(reason="buy_met")
            else:
                raise ValueError(
                    f'Unknown order status summary for {stored_state["order_id"]} {order.status_summary}'
                )

        elif requested_state == POSITION_TAKEN:
            # do we still hold this number of the position?
            ["order_id"]

        # self.state = requested_state

        ## need to use the stored order ID to query the broker and find out more about the order - is it a buy, stop, or profit?
        # there's some messy logic here
        # NO_POSITION_TAKEN         if there is nothing in rules, no orders open - basically clean slate, nothing to do
        # BUY_LIMIT_ORDER_ACTIVE    if there's a buy order issued, then we are trying to take a position
        # BUY_PRICE_MET             if there's a buy order recently filled but is no rule in ssm, then we just took a position
        # POSITION_TAKEN            if there's a rule in ssm but no

        # TAKING_PROFIT = 4
        # STOP_LOSS_ACTIVE = 5

        print("banana")

    def _get_bars(self, from_date=None, to_date=None, initialised: bool = True):

        if initialised == False:
            # we actually need to grab everything
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

        if len(bars) == 0:
            # something went wrong - usually bad symbol and search parameters
            log_wp.debug(
                f"{self.symbol}: No data returned for start {yf_start} end {yf_end}"
            )

        bars = bars.tz_localize(None)
        # bars = bars.loc[bars.index <= yf_end]

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
            if len(new_bars) < 200:
                new_bars = utils.merge_bars(new_bars=new_bars, bars=self.bars.iloc[-200:])

            new_bars = utils.add_signals(new_bars, interval=self.interval)
            self.bars = utils.merge_bars(self.bars, new_bars)

        else:
            log_wp.debug(f"{self.symbol}: No new data since {from_date}")
