import os
import re
import json
import boto3
import logging
import requests
from io import BytesIO
from gzip import GzipFile, compress
from datetime import date, timedelta
from collections import defaultdict

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
    data = {"files": [f"https://www.sthlmlunch.se/userMeta.json"]}
    url = f"{CLOUDFLARE_BASE_URL}/zones/{CLOUDFLARE_ZONE}/purge_cache"
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        logger.error(f"{response.status_code}: {response.text}")


def write_file(data):
    data = compress(json.dumps(data).encode("utf8"))
    client.put_object(
        Body=data, ContentEncoding="gzip", Bucket=S3_BUCKET, Key="userMeta.json"
    )


def read_file(key):
    old_file = client.get_object(Bucket=S3_BUCKET, Key=key)
    bytestream = BytesIO(old_file["Body"].read())
    return json.loads(GzipFile(None, "rb", fileobj=bytestream).read().decode("utf-8"))


def lambda_handler(event, context):
    user_meta = defaultdict(list)
    restaurant_meta = read_file("restaurants/meta.json")
    for key in client.list_objects(Bucket=S3_BUCKET, Prefix="restaurants")["Contents"]:
        if key["Key"].endswith("meta.json"):
            continue
        for review in read_file(key["Key"]):
            pointer = re.split(r"/|\.", key["Key"])[1]
            user_meta[review["reviewer"]].append(
                {
                    "tasteScore": review["tasteScore"],
                    "timestamp": review.get("timestamp"),
                    "restaurant": restaurant_meta[pointer]["name"],
                    "pointer": pointer,
                }
            )
    write_file(user_meta)
    return {"statusCode": 201}
