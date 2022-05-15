import pytest
from datetime import datetime
import utils
import pandas as pd


def test_symbol(f_chris_symbol):
    chris = f_chris_symbol.bars
    assert len(chris) == 3121


def test_order_expired(request, f_chris_symbol, f_order_buys):
    if request.node.callspec.id == "order_buy_timed_out":
        expected_result = True
    else:
        expected_result = False

    actual_result = f_chris_symbol._is_position_timed_out(
        datetime.now(), f_order_buys
    )

    assert actual_result == expected_result


def test_remove_from_state(request, monkeypatch, f_chris_symbol, f_states):
    def fake_get_stored_state(store, back_testing):
        return f_states

    def fake_put_stored_state(store, new_state, back_testing: bool = False):
        return

    monkeypatch.setattr(utils, "get_stored_state", fake_get_stored_state)
    monkeypatch.setattr(utils, "put_stored_state", fake_put_stored_state)

    if (
        request.node.callspec.id == "state_no_chris"
        or request.node.callspec.id == "state_blank"
    ):
        expected_result = False
    else:
        expected_result = True

    actual_result = f_chris_symbol._remove_from_state()

    assert actual_result == expected_result


def test_remove_from_rules(request, monkeypatch, f_chris_symbol, f_rules):
    def fake_get_rules(store, back_testing):
        return f_rules

    def fake_put_rules(store, new_rules, back_testing):
        return

    monkeypatch.setattr(utils, "get_rules", fake_get_rules)
    monkeypatch.setattr(utils, "put_rules", fake_put_rules)

    if (
        request.node.callspec.id == "rule_blank"
        or request.node.callspec.id == "rule_no_chris"
    ):
        expected_result = False
    else:
        expected_result = True

    actual_result = f_chris_symbol._remove_from_rules()
    assert actual_result == expected_result


def test_trans_enter_position(request, monkeypatch, f_chris_symbol, f_rules):
    ...
