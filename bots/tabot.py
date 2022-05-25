# TODO
# command line input for:
#   - back testing
#   - back testing broker
#   - symbols
#   - parameter store
#   - notification service to use
#   - interval
#   - real money trading
#   - S3 bucket to store outputs
#   - back testing balance
# recover active plays from state
# better reporting
#  - handle open positions at end of run better - currently these are marked as losses
# backtest wrapper - stop loss issues - either 99% is wrong or i'm calculating it wrong in stop loss
# actually just all of how I iterate over take profit - the logic has seeped out of BuyPlan and in to Stock_Symbol
# 300 df merge update bring down to just changes - faster faster
# swyftx wrapper is busted

# external packages
import boto3
from datetime import datetime
import json
import logging
import time
import yfinance as yf

# my modules
from macd import MacdBot
from bot_telemetry import BotTelemetry
import notification_services
import parameter_stores
import sample_symbols
import utils


log_wp = logging.getLogger("tabot")  # or pass an explicit name here, e.g. "mylogger"
hdlr = logging.StreamHandler()
fhdlr = logging.FileHandler("tabot.log")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(funcName)20s - %(message)s"
)
hdlr.setFormatter(formatter)
log_wp.addHandler(hdlr)
log_wp.addHandler(fhdlr)
log_wp.setLevel(logging.DEBUG)


def main():
    BUCKET = "mfers-tabot"
    back_testing = True
    override_broker = True
    interval = "5m"
    back_testing_balance = 100000
    real_money_trading = False
    bot_telemetry = BotTelemetry(back_testing=back_testing)
    market_data_source = yf
    notification_service_object = notification_services.Slack

    symbols = sample_symbols.everything
    # symbols = sample_symbols.crypto_symbols_all
    # symbols = sample_symbols.crypto_symbol

    run_id = utils.generate_id()
    log_wp.debug(
        f"Starting up run ID {run_id}: interval={interval}, back_testing={back_testing}, real_money_trading={real_money_trading}"
    )

    # write heartbeat to SSM (can't use local for this since the heartbeat reader is Lambda)
    if real_money_trading:
        heartbeat_path = "/tabot/heartbeat/live"
    else:
        # no need for checking backtesting, since I don't heartbeat it
        heartbeat_path = "/tabot/heartbeat/paper"

    ssm = boto3.client("ssm")
    slack_token = (
        ssm.get_parameter(Name="/tabot/slack/bot_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )
    slack_announcements_channel = (
        ssm.get_parameter(
            Name="/tabot/slack/announcements_channel", WithDecryption=False
        )
        .get("Parameter")
        .get("Value")
    )

    # reset back testing rules before we start the run
    if back_testing:
        store = parameter_stores.back_test_store()
        store.put_parameter(
            Name=f"/tabot/rules/backtest/5m",
            Value=json.dumps([]),
            Type="String",
            Overwrite=True,
        )

        # also put the alpaca API keys into the local store
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

        store.put_parameter(
            Name=f"/tabot/alpaca/api_key",
            Value=api_key,
        )
        store.put_parameter(
            Name=f"/tabot/alpaca/security_key",
            Value=secret_key,
        )

        store.put_parameter(Name="/tabot/slack/bot_key", Value=slack_token)
        store.put_parameter(
            Name="/tabot/slack/announcements_channel", Value=slack_announcements_channel
        )

    else:
        store = parameter_stores.ssm()

    notification_service = notification_service_object(
        store=store, back_testing=back_testing
    )

    bot_handler = MacdBot(
        ssm=store,
        market_data_source=market_data_source,
        bot_telemetry=bot_telemetry,
        symbols=symbols,
        real_money_trading=real_money_trading,
        notification_service=notification_service,
        override_broker=override_broker,
        interval=interval,
        back_testing=back_testing,
        back_testing_balance=back_testing_balance,
    )

    if len(symbols) == 0:
        print(f"Nothing to do - no symbols to watch/symbols are invalid/no data")
        return

    if back_testing:
        # no loop needed
        # TODO i think i can nest this into the while, avoid duplicating code
        bot_handler.process_bars()
        bot_handler.bot_telemetry.generate_df()
        utils.save_to_s3(
            bucket=BUCKET,
            key=f"telemetry/backtests/{run_id}_plays.csv",
            pickle=bot_handler.bot_telemetry.plays_df.to_csv(),
        )
        utils.save_to_s3(
            bucket=BUCKET,
            key=f"telemetry/backtests/{run_id}_orders.csv",
            pickle=bot_handler.bot_telemetry.orders_df.to_csv(),
        )
        print("banana")

    else:
        last_orders_df = []
        while True:
            # do heartbeating
            store.put_parameter(
                Name=heartbeat_path,
                Value=str(datetime.now()),
                Type="String",
                Overwrite=True,
            )

            # process data
            bot_handler.process_bars()

            # update report and if its changed upload it to S3
            bot_handler.bot_telemetry.generate_df()
            new_orders_df = bot_handler.bot_telemetry.orders_df
            if len(new_orders_df) != len(last_orders_df):
                utils.save_to_s3(
                    bucket=BUCKET,
                    key=f"telemetry/{run_id}_plays.csv",
                    pickle=bot_handler.bot_telemetry.plays_df.to_json(),
                )
                utils.save_to_s3(
                    bucket=BUCKET,
                    key=f"telemetry/{run_id}_orders.csv",
                    pickle=bot_handler.bot_telemetry.orders_df.to_json(),
                )

            last_orders_df = new_orders_df

            # and now sleep til the next interval
            start, end = bot_handler.get_date_range()
            pause = utils.get_pause(interval)
            log_wp.debug(f"Finished analysing {end}, sleeping for {round(pause,0)}s")
            time.sleep(pause)

    print("banana")


if __name__ == "__main__":
    main()
