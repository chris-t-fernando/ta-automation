import matplotlib.pyplot as plt
import pandas as pd
import json
import boto3
import os
from botocore.exceptions import ClientError


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    try:
        # Upload the file
        # s3_client = boto3.client(
        #   "s3",
        # )
        # response = s3_client.upload_file(
        #    file_name,
        #    bucket,
        #    object_name,
        #    StorageClass="STANDARD_IA",
        #    ExtraArgs={"StorageClass": "STANDARD_IA"},
        #    ContentType="image/png",
        # )

        # response = s3.meta.client.Bucket('<bucket-name>').put_object(Key='folder/{}'.format(filename), Body=file)

        file = open(file_name, "rb")
        s3 = boto3.resource("s3")
        response = s3.meta.client.Bucket(bucket).put_object(
            Key=object_name,
            Body=file,
            ContentType="image/png",
            StorageClass="STANDARD_IA",
        )
    except ClientError as e:
        return False
    return True


def lambda_handler(event, context):
    df_json = json.dumps(event["Payload"]["symbol_data"])
    df = pd.read_json(df_json)

    if list(event["Payload"]["ta_algo"].keys())[0] == "awesome-oscillator":
        ta_data = event["Payload"]["ta_analysis"]["ta_data"]
        buy = ta_data["awesome-oscillator-buy-price"]
        sell = ta_data["awesome-oscillator-sell-price"]
        signal = ta_data["awesome-oscillator-signal"]
        ao = ta_data["awesome-oscillator"]

        ax1 = plt.subplot2grid((10, 1), (0, 0), rowspan=5, colspan=1)
        ax2 = plt.subplot2grid((10, 1), (6, 0), rowspan=4, colspan=1)
        ax1.plot(df["Close"], label=event["Payload"]["symbol"], color="skyblue")
        ax1.plot(
            df.index,
            ta_data["awesome-oscillator-buy-price"],
            marker="^",
            markersize=12,
            color="#26a69a",
            linewidth=0,
            label="BUY SIGNAL",
        )
        ax1.plot(
            df.index,
            ta_data["awesome-oscillator-sell-price"],
            marker="v",
            markersize=12,
            color="#f44336",
            linewidth=0,
            label="SELL SIGNAL",
        )
        ax1.legend()
        ax1.set_title(f'{event["Payload"]["symbol"]} CLOSING PRICE')
        for i in range(len(df)):
            if ta_data["awesome-oscillator"][i - 1] > ta_data["awesome-oscillator"][i]:
                ax2.bar(df.index[i], ta_data["awesome-oscillator"][i], color="#f44336")
            else:
                ax2.bar(df.index[i], ta_data["awesome-oscillator"][i], color="#26a69a")
        ax2.set_title(f'{event["Payload"]["symbol"]} AWESOME OSCILLATOR 5,34')

        # make a hash out of the things that make this query unique - symbol, dates and algo used
        graph_file = (
            str(
                hash(
                    event["Payload"]["symbol"]
                    + str(event["Payload"]["date_from"])
                    + str(event["Payload"]["date_to"])
                    + list(event["Payload"]["ta_algo"].keys())[0]
                )
            )
            + ".png"
        )

        graph_path = "/tmp"
        save_success = plt.savefig(graph_path + "/" + graph_file)

        s3 = boto3.client("s3")
        with open(graph_path + "/" + graph_file, "rb") as f:
            s3.upload_fileobj(f, "mfers-graphs", graph_file)

        return f"http://s3-ap-southeast-2.amazonaws.com/mfers-graphs/{graph_file}"
    else:
        return "Algo not implemented"
