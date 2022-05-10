import pytest
from datetime import datetime
import utils


def test_symbol(f_aapl_symbol):
    aapl = f_aapl_symbol.bars
    assert len(aapl) == 3121


def test_order_expired(request, f_aapl_symbol, f_order_buys):
    if request.node.callspec.id == "order_buy_timed_out":
        expected_result = True
    else:
        expected_result = False

    actual_result = f_aapl_symbol.enter_position_timed_out(datetime.now(), f_order_buys)

    assert actual_result == expected_result

def test_remove_from_state(request, monkeypatch, f_aapl_symbol, f_states):
    def fake_get_stored_state(ssm, back_testing):
        return f_states
    
    def fake_put_stored_state(ssm, new_state, back_testing: bool = False):
        return
    
    monkeypatch.setattr(utils, "get_stored_state", fake_get_stored_state)
    monkeypatch.setattr(utils, "put_stored_state", fake_put_stored_state)
    #monkeypatch.setattr()

    if request.node.callspec.id == "state_no_aapl" or request.node.callspec.id == "state_blank":
        expected_result = False
    else:
        expected_result = True

    actual_result = f_aapl_symbol._remove_from_state()

    assert actual_result == expected_result