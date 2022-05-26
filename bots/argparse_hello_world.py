import argparse

parser = argparse.ArgumentParser(description="TA bot orchestrator")
# parser.add_argument('integers', metavar='N', type=int, nargs='+',
#                    help='an integer for the accumulator')
parser.add_argument(
    "--sum", default=True, help="sum the integers (default: find the max)"
)

parser.add_argument(
    "--bucket",
    default="mfers-tabot",
    help="S3 bucket used to hold report CSVs and saved bars",
)
parser.add_argument(
    "--back_testing",
    default=True,
    action=argparse.BooleanOptionalAction,
    help="Back testing toggle",
)
parser.add_argument(
    "--back_testing_balance", default=100000, help="Starting balance when back testing"
)
parser.add_argument(
    "--back_testing_override_broker",
    default=True,
    help="Use paper API when backtesting. Don't do this",
)
parser.add_argument(
    "--back_testing_skip_bar_update",
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
    "--real_money_trading", default=False, help="Mutually exclusive with back_trading"
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
    choices=[
        "mixed_symbols",
        "nyse_symbols_big",
        "nyse_symbols_medium",
        "nyse_symbols",
        "mixed_symbols_small",
        "crypto_symbol",
        "crypto_symbols_all",
        "everything",
    ],
)


args = parser.parse_args()
print(args)
