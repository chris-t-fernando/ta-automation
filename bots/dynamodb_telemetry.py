import boto3

dyn = boto3.resource("dynamodb")
table = dyn.Table("bot_telemetry_macd_cycles")

table.put_item(
    Item={
        "cycle_date": str(self.cycle_timestamp),
        "signal_data": cycle_json,
    }
)
