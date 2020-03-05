import pytest
import lambdas.analytics as analytics
import boto3
from moto import mock_s3

def test_get_key_path():

    action = "test"
    pld_str = "{\"timestamp\":\"2020-03-05 00:00:00\"}"

    actual_key_path = analytics.get_key_path(action, pld_str)

    excptected_key_path = "analytics/" + action + "/dt=2020-03-05/"

    assert excptected_key_path in actual_key_path


def test_get_partition_with_timestamp():
    pld_json = {"test": "teste"}
    actual_partition = analytics.get_partition(pld_json)
    expected_partition = "dt=20"
    assert expected_partition in actual_partition

def test_get_partition_without_timestamp():
    pld_json = {"timestamp": "2020-03-05 00:00:00"}
    actual_partition = analytics.get_partition(pld_json)
    expected_partition = "dt=2020-03-05"
    assert expected_partition in actual_partition

def test_clean_str_pld():
    pld = "{\r\t\"test\":\"test\"\r\t}"
    actual_pld = analytics.clean_str_pld(pld)
    expected_pld = "{\"test\":\"test\"}"

    assert expected_pld == actual_pld

@mock_s3
def test_save_pld_s3():
    key = "test/test.json"
    pld = "{\"test\":\"test\"}"

    conn = boto3.resource("s3")
    conn.create_bucket(Bucket=analytics.BUCKET_NAME)

    analytics.save_pld_s3(key, pld)

    s3 = boto3.client("s3")

    response = s3.list_objects(Bucket=analytics.BUCKET_NAME)
    assert response['Contents'][0]["Key"] == key

