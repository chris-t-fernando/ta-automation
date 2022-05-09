import boto3

dyn = boto3.resource("dynamodb")
table = dyn.Table("hello-world")
response = table.get_item(Key={"symbol": "AAPL"})
# table.put_item(Item={"symbol": "AAPL", "OHLC": 1})
print("banan")
