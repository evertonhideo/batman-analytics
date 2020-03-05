import json
import boto3, json, uuid
from http import HTTPStatus


def lambda_handler(event, context):
    if isinstance(event["queryStringParameters"], dict) and "action" in event["queryStringParameters"]:
        action = event["queryStringParameters"]["action"]
        pld_str = event["body"].replace("\n", "").replace("\r", "").replace("\t", "")
        pld_json = json.loads(pld_str)
        partition = "dt=" + pld_json['timestamp']
        key = "analytics/" + action + "/" + partition + "/" + str(uuid.uuid1()) + ".json"

        s3 = boto3.client("s3")
        s3.put_object(Bucket="hackathon-iti", Key=key, Body=pld_str)

    return {
        "statusCode": HTTPStatus.OK,
        "body": json.dumps({"message": "ok"})

    }


