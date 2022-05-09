import pytest
from stock_symbol import Symbol
from alpaca_wrapper import AlpacaAPI
import boto3
import yfinance as yf
import pandas as pd
from utils import unpickle
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

fixtures_path = "bots/tests/fixtures/"


@pytest.fixture
def f_aapl_symbol(monkeypatch):
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

    def fake_get_bars(from_date=None, to_date=None, initialised: bool = True):
        return pd.read_csv(
            f"{fixtures_path}symbol_aapl.csv",
            index_col=0,
            parse_dates=True,
            infer_datetime_format=True,
        )

    monkeypatch.setattr(Symbol, "_get_bars", fake_get_bars)

    aapl = Symbol(
        symbol="AAPL",
        api=api,
        interval="5m",
        real_money_trading=False,
        ssm=ssm,
        data_source=yf,
    )

    return aapl


def test_symbol(f_aapl_symbol):
    aapl = f_aapl_symbol
    assert len(aapl.bars) == 3121


@pytest.fixture(
    params=[
        "buy_timed_out",  #
        "buy_active",  #
        "buy_filled",  #
        "buy_cancelled",  #
        "sell_filled",  #
        "stop_loss_active",  #
        "sell_limit_active",  #
    ]
)
def f_order(request):
    f_file = f"{fixtures_path}order_{request.param}.txt"

    f = open(f_file, "r")
    order_file = f.read()
    order = unpickle(order_file)

    # set the timestamp on the order to something recent
    if request.param != "timed_out":
        order.create_time = pd.Timestamp(datetime.now().astimezone(pytz.utc) - relativedelta(minutes=15))
        order.update_time = pd.Timestamp(datetime.now().astimezone(pytz.utc) - relativedelta(minutes=5))
        # TODO if i care enough later also overwrite timestmap in _raw_response._raw["updated_at"] etc but its a string and im lazy

    return order


def test_order_expired(request, f_aapl_symbol, f_order):
    if request.node.callspec.id == "buy_timed_out":
        expected_result = True
    else:
        expected_result = False

    actual_result = f_aapl_symbol.enter_position_timed_out(
        datetime.now(), f_order
    )

    assert actual_result == expected_result
