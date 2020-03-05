import json
import boto3, json, uuid
from http import HTTPStatus
from datetime import datetime

BUCKET_NAME = "hackathon-iti"


def lambda_handler(event, context):

    try:
        if isinstance(event["queryStringParameters"], dict) and "action" in event["queryStringParameters"]:
            action = event["queryStringParameters"]["action"]

            pld_str = event["body"]

            key = get_key_path(action, pld_str)

            pld_str_cleaned = clean_str_pld(pld_str)

            save_pld_s3(key, pld_str_cleaned)

        return {
            "statusCode": HTTPStatus.OK,
            "body": json.dumps({"message": "ok"})
        }
    except:
        return {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR,
            "body": json.dumps({"message": "error"})
        }


def clean_str_pld(pld_str):
    return pld_str.replace("\n", "").replace("\r", "").replace("\t", "")


def get_key_path(action, pld_str):
    pld_json = json.loads(pld_str)
    partition = get_partition(pld_json)
    key = "analytics/" + action + "/" + partition + "/" + str(uuid.uuid1()) + ".json"
    return key


def get_partition(pld_json):
    if "timestamp" in pld_json:
        partition = "dt=" + pld_json['timestamp'].split(" ")[0]
    else:
        partition = "dt=" + datetime.now().strftime("%Y-%m-%d")
    return partition


def save_pld_s3(key, pld_str):

    s3 = boto3.client("s3")
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=pld_str)


