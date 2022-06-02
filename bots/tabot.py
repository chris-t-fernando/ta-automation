# TODO
# print numbers as decimals instead of exponentials
# print numbers rounded to whatever position makes sense
# better reporting/telemetry
# orderresult needs an open/close status
# orderresult needs to be moved in to the I class, and each concrete implementation needs to call super on it
# swyftx wrapper is busted
# recover active plays from state
# better reporting
#  - crypto vs nyse
#  - streaks
# 300 df merge update bring down to just changes - faster faster
# should we use high/low/close as trigger? its used all through **utils** and buyplan
# make it more pythonic
# better testing - especially buyplan
# ability to transition between algos
# ability to pause operation more than every 5 minutes
# sell and stop loss tell me profit


# external packages
import argparse
from datetime import datetime
import logging
import pytz
import time

# my modules
from macd import MacdBot
from macd_config import MacdConfig
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
    # shut this thing down if the user doesn't acknowledge
    if args.run_type == "prod":
        print("****************************")
        print("WARNING!!! This bot is about to spend your REAL MONEY.")
        print(
            "It comes with zero warranty and all transactions are premanent and final."
        )
        print("Only continue if you can afford to lose it ALL.")
        print("****************************")
        confirmation = input("To continue, type Yes (including capitals): ")
        if confirmation != "Yes":
            print("Exiting.")
            exit()

    run_id = utils.generate_id()
    config = MacdConfig(args=args)
    symbols = sample_symbols.input_symbols[args.symbols]

    log_wp.debug(
        f"Starting up run ID {run_id}: interval={config.interval}, run_type={config.run_type}"
    )
    # TODO once you make this stateful, remove this
    config.store.put(path=config.path_state, value="[]")
    config.store.put(path=config.path_rules, value="[]")

    bot_handler = MacdBot(
        config=config,
        symbols=symbols,
    )

    if len(symbols) == 0:
        print(f"Nothing to do - no symbols to watch/symbols are invalid/no data")
        return

    if config.back_testing:
        # no loop needed
        # TODO i think i can nest this into the while, avoid duplicating code
        bot_handler.process_bars()

        bot_handler.bot_telemetry.generate_df()
        utils.save_to_s3(
            bucket=config.telemetry_s3_bucket,
            key_base=f"{config.telemetry_s3_prefix}/",
            key=f"{run_id}_plays.csv",
            pickle=bot_handler.bot_telemetry.plays_df.to_csv(),
        )
        utils.save_to_s3(
            bucket=config.telemetry_s3_bucket,
            key_base=f"{config.telemetry_s3_prefix}/",
            key=f"{run_id}_orders.csv",
            pickle=bot_handler.bot_telemetry.orders_df.to_csv(),
        )
        utils.save_to_s3(
            bucket=config.telemetry_s3_bucket,
            key_base=f"{config.telemetry_s3_prefix}/",
            key=f"{run_id}_symbols.csv",
            pickle=bot_handler.bot_telemetry.symbols_df.to_csv(),
        )
        print("banana")

    else:
        last_orders_df = []
        while True:
            # do heartbeating
            config.store.put(path=config.heartbeat, value=str(datetime.now().astimezone(pytz.utc)))

            # process data
            bot_handler.process_bars()

            # update report and if its changed upload it to S3
            bot_handler.bot_telemetry.generate_df()
            new_orders_df = bot_handler.bot_telemetry.orders_df
            if len(new_orders_df) != len(last_orders_df):
                utils.save_to_s3(
                    bucket=config.telemetry_s3_bucket,
                    key_base=f"{config.telemetry_s3_prefix}/",
                    key=f"{run_id}_plays.csv",
                    pickle=bot_handler.bot_telemetry.plays_df.to_json(),
                )
                utils.save_to_s3(
                    bucket=config.telemetry_s3_bucket,
                    key_base=f"{config.telemetry_s3_prefix}/",
                    key=f"{run_id}_orders.csv",
                    pickle=bot_handler.bot_telemetry.orders_df.to_json(),
                )
                utils.save_to_s3(
                    bucket=config.telemetry_s3_bucket,
                    key_base=f"{config.telemetry_s3_prefix}/",
                    key=f"{run_id}_symbols.csv",
                    pickle=bot_handler.bot_telemetry.symbols_df.to_csv(),
                )

            last_orders_df = new_orders_df

            # and now sleep til the next interval
            start, end = bot_handler.get_date_range()
            pause = utils.get_pause(config.interval)
            log_wp.debug(f"Finished analysing {end}, sleeping for {round(pause,0)}s")
            time.sleep(pause)

    print("banana")


parser = argparse.ArgumentParser(description="TA bot orchestrator")
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
    help="TA bot orchestrator will attempt to download saved bars from S3 and then update them "
    "with the latest from Yahoo Finance. Setting this to False will prevent the update",
)
parser.add_argument(
    "--interval",
    default="5m",
    choices=["1m", "5m", "30m"],
    help="Intervals between executions/data resolution",
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
parser.add_argument(
    "--run_type",
    default="paper",
    choices=list(["prod", "paper", "back_test"]),
)

args = parser.parse_args()

if __name__ == "__main__":
    main(args)
