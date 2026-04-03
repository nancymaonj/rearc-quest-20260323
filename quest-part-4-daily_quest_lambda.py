import boto3
import os
import requests

s3 = boto3.client("s3")

TODAY = os.environ["TODAY_BUCKET"]
YESTERDAY = os.environ["YESTERDAY_BUCKET"]

BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
DATAUSA_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"


def move_files():
    resp = s3.list_objects_v2(Bucket=TODAY)
    if "Contents" not in resp:
        return

    for obj in resp["Contents"]:
        key = obj["Key"]

        s3.copy_object(
            Bucket=YESTERDAY,
            CopySource={"Bucket": TODAY, "Key": key},
            Key=key
        )

        s3.delete_object(Bucket=TODAY, Key=key)


def fetch_data():
    # DataUSA JSON
    r = requests.get(DATAUSA_URL)
    s3.put_object(Bucket=TODAY, Key="datausa.json", Body=r.content)

    # BLS (raw page)
    r = requests.get(BLS_URL)
    s3.put_object(Bucket=TODAY, Key="bls.html", Body=r.content)


def lambda_handler(event, context):
    move_files()
    fetch_data()