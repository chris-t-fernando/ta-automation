# TODO
# recover active plays from state
# better reporting
#  - crypto vs nyse
#  - streaks
# 300 df merge update bring down to just changes - faster faster
# swyftx wrapper is busted
# should we use high/low/close as trigger? its used all through **utils** and buyplan

# external packages
import argparse
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


def main(args):
    s3_bucket = args.bucket
    back_testing = args.back_testing
    back_testing_balance = args.back_testing_balance
    back_testing_override_broker = args.back_testing_override_broker
    back_testing_skip_bar_update = args.back_testing_skip_bar_update
    interval = args.interval
    real_money_trading = args.real_money_trading
    bot_telemetry = BotTelemetry(back_testing=back_testing)
    market_data_source = yf
    if args.notification_service == "pushover":
        notification_service_object = notification_services.Pushover
    elif args.notification_service == "slack":
        notification_service_object = notification_services.Slack
    symbols = sample_symbols.input_symbols[args.symbols]

    # symbols = sample_symbols.everything
    # symbols = sample_symbols.crypto_symbols_all
    # symbols = [{"symbol": "AVAX-USD", "api": "alpaca"}]

    run_id = utils.generate_id()
    log_wp.debug(
        f"Starting up run ID {run_id}: interval={interval}, back_testing={back_testing}, real_money_trading={real_money_trading}"
    )

    # write heartbeat to SSM (can't use local for this since the heartbeat reader is Lambda)
    heartbeat_path = "/tabot/heartbeat/paper"

    ssm = boto3.client("ssm")
    slack_token = (
        ssm.get_parameter(Name="/tabot/slack/bot_key", WithDecryption=True)
        .get("Parameter")
        .get("Value")
    )
    if real_money_trading:
        slack_announcements_path = "/tabot/prod/slack/announcements_channel"
    else:
        slack_announcements_path = "/tabot/paper/slack/announcements_channel"

    slack_announcements_channel = (
        ssm.get_parameter(Name=slack_announcements_path, WithDecryption=False)
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
            ssm.get_parameter(Name="/tabot/paper/alpaca/api_key", WithDecryption=True)
            .get("Parameter")
            .get("Value")
        )
        secret_key = (
            ssm.get_parameter(
                Name="/tabot/paper/alpaca/security_key", WithDecryption=True
            )
            .get("Parameter")
            .get("Value")
        )

        store.put_parameter(
            Name=f"/tabot/paper/alpaca/api_key",
            Value=api_key,
        )
        store.put_parameter(
            Name=f"/tabot/paper/alpaca/security_key",
            Value=secret_key,
        )

        store.put_parameter(Name="/tabot/slack/bot_key", Value=slack_token)
        store.put_parameter(
            Name="/tabot/paper/slack/announcements_channel",
            Value=slack_announcements_channel,
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
        interval=interval,
        back_testing=back_testing,
        back_testing_override_broker=back_testing_override_broker,
        back_testing_balance=back_testing_balance,
        back_testing_skip_bar_update=back_testing_skip_bar_update,
    )

    if len(symbols) == 0:
        print(f"Nothing to do - no symbols to watch/symbols are invalid/no data")
        return

    if back_testing:
        # no loop needed
        # TODO i think i can nest this into the while, avoid duplicating code
        bot_handler.process_bars()

        # liquidate open positions
        bot_handler.liquidate_all(back_testing=back_testing)

        bot_handler.bot_telemetry.generate_df()
        utils.save_to_s3(
            bucket=s3_bucket,
            key=f"telemetry/backtests/{run_id}_plays.csv",
            pickle=bot_handler.bot_telemetry.plays_df.to_csv(),
        )
        utils.save_to_s3(
            bucket=s3_bucket,
            key=f"telemetry/backtests/{run_id}_orders.csv",
            pickle=bot_handler.bot_telemetry.orders_df.to_csv(),
        )
        utils.save_to_s3(
            bucket=s3_bucket,
            key=f"telemetry/backtests/{run_id}_symbols.csv",
            pickle=bot_handler.bot_telemetry.symbols_df.to_csv(),
        )
        print("banana")

    # for idx in bot_handler.bot_telemetry.plays_df.index:
    #    print(f"between: {bot_handler.bot_telemetry.plays_df.start.iloc[idx]} and {bot_handler.bot_telemetry.plays_df.end.iloc[idx]}")
    #    concurrent_orders = bot_handler.bot_telemetry.plays_df.loc[(bot_handler.bot_telemetry.plays_df.start.iloc[idx] < bot_handler.bot_telemetry.plays_df.start) & (bot_handler.bot_telemetry.plays_df.end.iloc[idx] > bot_handler.bot_telemetry.plays_df.end)]
    #    break

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
                    bucket=s3_bucket,
                    key=f"telemetry/{run_id}_plays.csv",
                    pickle=bot_handler.bot_telemetry.plays_df.to_json(),
                )
                utils.save_to_s3(
                    bucket=s3_bucket,
                    key=f"telemetry/{run_id}_orders.csv",
                    pickle=bot_handler.bot_telemetry.orders_df.to_json(),
                )
                utils.save_to_s3(
                    bucket=s3_bucket,
                    key=f"telemetry/backtests/{run_id}_symbols.csv",
                    pickle=bot_handler.bot_telemetry.symbols_df.to_csv(),
                )

            last_orders_df = new_orders_df

            # and now sleep til the next interval
            start, end = bot_handler.get_date_range()
            pause = utils.get_pause(interval)
            log_wp.debug(f"Finished analysing {end}, sleeping for {round(pause,0)}s")
            time.sleep(pause)

    print("banana")


parser = argparse.ArgumentParser(description="TA bot orchestrator")
parser.add_argument(
    "--bucket",
    default="mfers-tabot",
    help="S3 bucket used to hold report CSVs and saved bars",
)
parser.add_argument(
    "--back_testing",
    default=False,
    action=argparse.BooleanOptionalAction,
    help="Back testing toggle",
)
# parser.add_argument("--back_testing", default=True, help="Back testing toggle")
parser.add_argument(
    "--back_testing_balance", default=100000, help="Starting balance when back testing"
)
parser.add_argument(
    "--back_testing_override_broker",
    action=argparse.BooleanOptionalAction,
    default=False,
    help="Use paper API when backtesting. Don't do this",
)
parser.add_argument(
    "--back_testing_skip_bar_update",
    action=argparse.BooleanOptionalAction,
    default=False,
    help="TA bot orchestrator will attempt to download saved bars from S3 and then update them with the latest from Yahoo Finance. Setting this to False will prevent the update",
)
parser.add_argument(
    "--interval",
    default="5m",
    choices=["1m", "5m", "30m"],
    help="Intervals between executions/data resolution",
)
parser.add_argument(
    "--real_money_trading",
    action=argparse.BooleanOptionalAction,
    default=False,
    help="Mutually exclusive with back_trading",
)
parser.add_argument(
    "--notification_service",
    default="slack",
    choices=["pushover", "slack"],
    help="Send notifications using this service",
)
parser.add_argument(
    "--symbols",
    default="crypto_symbols_all",
    choices=list(sample_symbols.input_symbols.keys()),
)
args = parser.parse_args()

if __name__ == "__main__":
    main(args)
