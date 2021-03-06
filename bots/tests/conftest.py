import pytest
from macd_worker import MacdWorker
import pandas as pd
from datetime import datetime
from broker_alpaca import AlpacaAPI
from broker_back_test import BackTestAPI
import boto3
import parameter_stores
import yfinance as yf
from dateutil.relativedelta import relativedelta
import pytz
import utils
from alpaca_trade_api import REST, entity


fixtures_path = "bots/tests/fixtures/"


@pytest.fixture
def f_chris_symbol(monkeypatch):
    def fake_get_bars(from_date=None, to_date=None, initialised: bool = True):
        return

    def fake_add_signals(bars, interval):
        return pd.read_csv(
            f"{fixtures_path}symbol_chris.csv",
            index_col=0,
            parse_dates=True,
            infer_datetime_format=True,
        )

    def fake_list_assets(self):
        return get_pickle("alpaca_assets.txt")
        f = open(f"{fixtures_path}alpaca_assets.txt")
        pickled_assets = f.read()
        return utils.unpickle(pickled_assets)

    monkeypatch.setattr(Symbol, "_get_bars", fake_get_bars)
    monkeypatch.setattr(utils, "add_signals", fake_add_signals)
    monkeypatch.setattr(REST, "list_assets", fake_list_assets)

    ssm = boto3.client("ssm")

    # set up alpaca api
    api_key = (
        ssm.get_parameter(Name="/tabot/alpaca/api_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )
    secret_key = (
        ssm.get_parameter(Name="/tabot/alpaca/security_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )
    api = AlpacaAPI(
        alpaca_key_id=api_key,
        alpaca_secret_key=secret_key,
        back_testing=True,
    )

    api = BackTestAPI(back_testing=True)

    chris = Symbol(
        symbol="CHRIS",
        api=api,
        interval="5m",
        real_money_trading=False,
        store=parameter_stores.back_test_store,
        market_data_source=yf,
    )

    return chris


def make_order_timestamps_current(order):
    order.create_time = pd.Timestamp(
        datetime.now().astimezone(pytz.utc) - relativedelta(minutes=3)
    )
    order.update_time = pd.Timestamp(
        datetime.now().astimezone(pytz.utc) - relativedelta(minutes=4)
    )
    return order


def get_pickle(pickle_file):
    f_file = f"{fixtures_path}{pickle_file}"
    f = open(f_file, "r")
    order_file = f.read()
    order = utils.unpickle(order_file)
    return order


@pytest.fixture
def f_order_buy_timed_out():
    order = get_pickle("order_buy_timed_out.txt")
    return order


@pytest.fixture
def f_order_buy_active():
    order = get_pickle("order_buy_active.txt")
    order = make_order_timestamps_current(order)
    return order


@pytest.fixture
def f_order_buy_filled():
    order = get_pickle("order_buy_filled.txt")
    order = make_order_timestamps_current(order)
    return order


@pytest.fixture
def f_order_buy_cancelled():
    order = get_pickle("order_buy_cancelled.txt")
    order = make_order_timestamps_current(order)
    return order


@pytest.fixture
def f_order_sell_filled():
    order = get_pickle("order_sell_filled.txt")
    order = make_order_timestamps_current(order)
    return order


@pytest.fixture
def f_order_stop_loss_active():
    order = get_pickle("order_stop_loss_active.txt")
    order = make_order_timestamps_current(order)
    return order


@pytest.fixture
def f_order_sell_limit_active():
    order = get_pickle("order_sell_limit_active.txt")
    order = make_order_timestamps_current(order)
    return order


@pytest.fixture(
    params=[
        "order_buy_active",
        "order_buy_cancelled",
        "order_buy_filled",
        "order_buy_timed_out",
    ]
)
def f_order_buys(
    request,
    f_order_buy_active,
    f_order_buy_cancelled,
    f_order_buy_filled,
    f_order_buy_timed_out,
):
    if request.param == "order_buy_active":
        return f_order_buy_active
    elif request.param == "order_buy_cancelled":
        return f_order_buy_cancelled
    elif request.param == "order_buy_filled":
        return f_order_buy_filled
    elif request.param == "order_buy_timed_out":
        return f_order_buy_timed_out


@pytest.fixture(
    params=[
        "order_sell_filled",
        "order_sell_limit_active",
        "order_sell_stop_loss_active",
    ]
)
def f_order_sells(
    request,
    f_order_sell_filled,
    f_order_sell_limit_active,
    f_order_sell_stop_loss_active,
):
    if request.param == "order_sell_filled":
        return f_order_sell_filled
    elif request.param == "order_sell_limit_active":
        return f_order_sell_limit_active
    elif request.param == "order_sell_stop_loss_active":
        return f_order_sell_stop_loss_active


@pytest.fixture(
    params=[
        "order_sell_filled",
        "order_sell_limit_active",
        "order_sell_stop_loss_active",
        "order_buy_active",
        "order_buy_cancelled",
        "order_buy_filled",
        "order_buy_timed_out",
    ]
)
def f_order_all(
    request,
    f_order_buy_active,
    f_order_buy_cancelled,
    f_order_buy_filled,
    f_order_buy_timed_out,
    f_order_sell_filled,
    f_order_sell_limit_active,
    f_order_sell_stop_loss_active,
):
    if request.param == "order_sell_filled":
        return f_order_sell_filled
    elif request.param == "order_sell_limit_active":
        return f_order_sell_limit_active
    elif request.param == "order_sell_stop_loss_active":
        return f_order_sell_stop_loss_active
    if request.param == "order_buy_active":
        return f_order_buy_active
    elif request.param == "order_buy_cancelled":
        return f_order_buy_cancelled
    elif request.param == "order_buy_filled":
        return f_order_buy_filled
    elif request.param == "order_buy_timed_out":
        return f_order_buy_timed_out


@pytest.fixture
def f_state_blank():
    state = get_pickle("state_blank.txt")
    return state


@pytest.fixture
def f_state_buy_active():
    state = get_pickle("state_buy_active.txt")
    return state


@pytest.fixture
def f_state_multiple_symbols():
    state = get_pickle("state_multiple_symbols.txt")
    return state


@pytest.fixture
def f_f_state_no_chris():
    state = get_pickle("state_no_chris.txt")
    return state


@pytest.fixture
def f_state_stop_loss_active():
    state = get_pickle("state_stop_loss_active.txt")
    return state


@pytest.fixture
def f_state_taking_profit_active():
    state = get_pickle("state_taking_profit_active.txt")
    return state


@pytest.fixture(
    params=[
        "state_blank",
        "state_buy_active",
        "state_multiple_symbols",
        "state_no_chris",
        "state_stop_loss_active",
        "state_taking_profit_active",
    ]
)
def f_states(
    request,
    f_state_blank,
    f_state_buy_active,
    f_state_multiple_symbols,
    f_f_state_no_chris,
    f_state_stop_loss_active,
    f_state_taking_profit_active,
):
    if request.param == "state_blank":
        return f_state_blank
    elif request.param == "state_buy_active":
        return f_state_buy_active
    elif request.param == "state_multiple_symbols":
        return f_state_multiple_symbols
    elif request.param == "state_no_chris":
        return f_f_state_no_chris
    elif request.param == "state_stop_loss_active":
        return f_state_stop_loss_active
    elif request.param == "state_taking_profit_active":
        return f_state_taking_profit_active


@pytest.fixture
def f_rule_blank():
    return get_pickle("rule_blank.txt")


@pytest.fixture
def f_rule_multiple():
    return get_pickle("rule_multiple.txt")


@pytest.fixture
def f_rule_no_chris():
    return get_pickle("rule_no_chris.txt")


@pytest.fixture(
    params=[
        "rule_blank",
        "rule_multiple",
        "rule_no_chris",
    ]
)
def f_rules(request, f_rule_blank, f_rule_multiple, f_rule_no_chris):
    if request.param == "rule_blank":
        return f_rule_blank
    elif request.param == "rule_multiple":
        return f_rule_multiple
    elif request.param == "rule_no_chris":
        return f_rule_no_chris
