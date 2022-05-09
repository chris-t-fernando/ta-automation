# content of a/conftest.py
import pytest
from stock_symbol import Symbol
from alpaca_wrapper import AlpacaAPI
import boto3
import yfinance as yf
import pandas as pd
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)
