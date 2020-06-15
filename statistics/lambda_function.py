import requests
import os
import json
import atexit
import boto3
import gzip
import logging
from datetime import date, timedelta

URI = "https://mixpanel.com/api/2.0/events"
S3_BUCKET = os.environ["S3_BUCKET"]
CLOUDFLARE_TOKEN = os.environ["CLOUDFLARE_TOKEN"]
CLOUDFLARE_BASE_URL = os.environ["CLOUDFLARE_BASE_URL"]
CLOUDFLARE_ZONE = os.environ["CLOUDFLARE_ZONE"]

client = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def purge_cloudflare_cache():
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + CLOUDFLARE_TOKEN,
    }
    data = {"files": [f"https://www.sthlmlunch.se/statistics.json"]}
    url = f"{CLOUDFLARE_BASE_URL}/zones/{CLOUDFLARE_ZONE}/purge_cache"
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        logger.error(f"{response.status_code}: {response.text}")


class Client:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = (os.environ["AWS_SECRET"], "")
        atexit.register(self.session.close)

    def top_events(self):
        response = self.session.get(f"{URI}/top", data={"limit": 10, "type": "general"})
        response.raise_for_status()
        return response.json()["events"]

    def events_series(self, event_names):
        data = {
            "event": json.dumps(event_names),
            "type": "general",
            "unit": "day",
            "interval": 7,
            "from_date": (date.today() - timedelta(days=7)).isoformat(),
            "to_date": date.today().isoformat(),
            "format": "json",
        }
        response = self.session.get(URI, data=data)
        response.raise_for_status()
        return response.json()["data"]


def write_file(data):
    data = gzip.compress(json.dumps(data).encode("utf8"))
    client.put_object(
        Body=data, ContentEncoding="gzip", Bucket=S3_BUCKET, Key="statistics.json"
    )


def lambda_handler(event, context):
    client = Client()
    events = client.top_events()
    event_names = [event["event"] for event in events]
    series = client.events_series(event_names)
    statistics = {"events": events, "series": series}
    write_file(statistics)
    purge_cloudflare_cache()
    return {"statusCode": 201}
