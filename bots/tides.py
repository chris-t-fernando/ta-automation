from numpy import NaN
import logging
from dateutil.relativedelta import relativedelta
import time

from portfolio_value import PortfolioValue
import utils
import notification_services
from parameter_stores import Ssm
from broker_alpaca import AlpacaAPI


log_wp = logging.getLogger("tides")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("tides.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)5s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)

_PREFIX = "tabot"
PATH_PAPER_ALPACA_API_KEY = f"/{_PREFIX}/paper/alpaca/api_key"
PATH_PAPER_ALPACA_SECURITY_KEY = f"/{_PREFIX}/paper/alpaca/security_key"
slack_bot_key_path = f"/{_PREFIX}/slack/bot_key"
slack_channel_path = f"/{_PREFIX}/paper/slack/announcements_channel"

record_interval = relativedelta(minutes=5)

benchmark = [
    {"symbol": "ETH-USD", "quantity": 1},
    {"symbol": "ALGO-USD", "quantity": 1014},
    {"symbol": "SHIB-USD", "quantity": 61200000},
    {"symbol": "DOGE-USD", "quantity": 8923},
    {"symbol": "SOL-USD", "quantity": 16},
    {"symbol": "MATIC-USD", "quantity": 1280},
    {"symbol": "AVAX-USD", "quantity": 31},
]

benchmark = {
    "ETH-USD": 1,
    "ALGO-USD": 1014,
    "SHIB-USD": 61200000,
    "DOGE-USD": 8923,
    "SOL-USD": 16,
    "MATIC-USD": 1280,
    "AVAX-USD": 31,
}

benchmark = {
    "YFI-USD": 10,
}


def main(args):
    store = Ssm()
    alpaca_api_key = store.get(path=PATH_PAPER_ALPACA_API_KEY)
    alpaca_security_key = store.get(path=PATH_PAPER_ALPACA_SECURITY_KEY)

    api = AlpacaAPI(alpaca_key_id=alpaca_api_key, alpaca_secret_key=alpaca_security_key)

    slack_bot_key = store.get(path=slack_bot_key_path)
    slack_announcements_channel = store.get(path=slack_channel_path)
    notification_service = notification_services.Slack(
        bot_key=slack_bot_key, channel=slack_announcements_channel
    )

    symbol_collection = PortfolioValue(
        benchmark, [k for k, v in benchmark.items()], log_level=logging.WARNING
    )

    # okay so we've set our starting point, now keep grabbing data and checking if we should buy in
    position_taken = False
    stop_loss_intervals = 0
    stop_loss_threshold = 2
    total_profit = 0
    while True:
        current_portfolio = symbol_collection.portfolio_df
        if not position_taken:
            this_sma = round(current_portfolio.sma.iloc[-1], 3)
            this_close = round(current_portfolio.Close_value.iloc[-1], 3)
            # this_diff_pct = round(current_portfolio.portfolio_diff_pct.iloc[-1], 3)
            # this_sma = porfolio_analysis.sma.iloc[-1]
            # this_diff_pct = porfolio_analysis.portfolio_diff_pct.iloc[-1]
            if this_close > this_sma:
                # the latest diff pct is better than the sma100 diff pct - its getting better, and this is our buy signal
                buy_value = 0
                for asset, quantity in benchmark.items():
                    symbol = asset.replace("-", "")
                    units_to_buy = quantity
                    buy = api.buy_order_market(symbol, units_to_buy)
                    buy_value += buy.filled_total_value
                stop_loss = current_portfolio.Close_value.iloc[-1]
                position_taken = True
                message = f"Took position valued at {buy_value:,}. Last close {this_close:,} > SMA {this_sma:,} value/stop loss of {stop_loss:,.4f}"
                stop_loss_intervals = 0
                log_wp.info(message)
                notification_service.send(message)
            else:
                log_wp.debug(f"No crossover found (last close {this_close:,}, SMA {this_sma:,})")

        else:
            # first check if stop loss has been hit, and if so then liquidate
            current_value = current_portfolio.Close_value.iloc[-1]
            if current_value < stop_loss:
                stop_loss_intervals += 1

                if stop_loss_intervals >= stop_loss_threshold:
                    # stop loss hit
                    sell_value = 0
                    for asset, quantity in benchmark.items():
                        symbol = asset.replace("-", "")
                        units_to_sell = quantity
                        # sell = api.sell_order_market(symbol, units_to_sell)
                        # sell_value += sell.filled_total_value
                        sell = api.sell_order_limit(symbol, units_to_sell, stop_loss)
                        sell_value += stop_loss

                    profit = sell_value - buy_value
                    total_profit += profit
                    message = f"Hit stop loss of {stop_loss:,.4f} vs stop loss of {stop_loss:,.4f}. Buy value was {buy_value:,}, sell value was {sell_value:,}, profit was {profit:,}"
                    log_wp.info(message)
                    notification_service.send(message)
                    del stop_loss
                    position_taken = False
            else:
                stop_loss_intervals = 0
                profit = current_value - stop_loss
                if profit < 0:
                    print("banana")
                profit_50 = profit * 0.5
                new_stop_loss = stop_loss + profit_50
                if new_stop_loss > stop_loss:
                    message = f"Changing stop loss from {stop_loss:,.4f} to {new_stop_loss:,.4f}"
                    log_wp.info(message)
                    notification_service.send(message)
                    stop_loss = new_stop_loss

        pause = utils.get_pause("5m")
        log_wp.info(
            f"Finished analysing. Total profit {total_profit:,}. Sleeping for {round(pause,0)}s"
        )
        time.sleep(pause)

    print("banana")


if __name__ == "__main__":
    # a= MarketData("SOL-USD")
    # a.bars = a.bars.loc[(a.bars.index < pd.Timestamp("2022-06-10 23:00:00-04:00"))]
    # a.get(start=pd.Timestamp("2022-06-14 12:32:39-04:00"))
    args = None

    main(args)
