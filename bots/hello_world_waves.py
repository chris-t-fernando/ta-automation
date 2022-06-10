from broker_back_test import BackTestAPI
import utils

starting_holdings = [{"symbol":"BTC-USD","quantity":0.01722374},
    {"symbol":"ETH-USD","quantity":0.281983},
    {"symbol":"ADA-USD","quantity":449.32},
    {"symbol":"XRP-USD","quantity":494.84},
    {"symbol":"DOT-USD","quantity":21.3202},
    {"symbol":"SOL-USD","quantity":4.0831},
    {"symbol":"BNB-USD","quantity":0.52247},
    {"symbol":"MATIC-USD","quantity":119.11},
    {"symbol":"AVAX-USD","quantity":2.9144},
    {"symbol":"AXS-USD","quantity":3.4485},
    {"symbol":"ATOM-USD","quantity":6.1787}
    ]


for this_holding in starting_holdings:
    this_holding["bars"] = utils.load_bars(this_holding["symbol"], bucket="mfers-tabot", key_base="symbol_data/5m/")

print("banana")