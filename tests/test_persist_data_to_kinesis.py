from moto import mock_events, mock_kinesis, mock_s3
import sys
import os
import json
import boto3
import pytest
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from kinesis_data_persist_handler.kinesis_event_lambda_handler import HandleBucketEvent


@mock_kinesis
def test_send_data_to_kinesis():
    stream_name = "dev-dot-sdc-persist-curated-data"
    bucket_name = "dev-dot-sdc-curated-911061262852-us-east-1"
    s3_key = "waze/version=20180720/content/state=AK/table=alert/projection=redshift/year=2019/month=05/day=01/hour=00/minute=01/3e9a9c59-8eb7-41e4-92a0-ee4993428e5a.csv.gz"
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
    os.environ["KINESIS_STREAM"] = stream_name
    metadata_object = dict()
    metadata_object["bucket-name"] = bucket_name
    metadata_object["s3-key"] = s3_key
    metadata_object["is-historical"] = "false"
    persist_data_to_kinesis_obj = HandleBucketEvent()
    persist_data_to_kinesis_obj.sendDatatoKinesis(metadata_object)
    assert True


@mock_s3
def test_get_s3_head_object():
    file_name = "data/3e9a9c59-8eb7-41e4-92a0-ee4993428e5a.csv.gz"
    bucket_name = "dev-dot-sdc-curated-911061262852-us-east-1"
    s3_key = "waze/version=20180720/content/state=AK/table=alert/projection=redshift/year=2019/month=05/day=01/hour=00/minute=01/3e9a9c59-8eb7-41e4-92a0-ee4993428e5a.csv.gz"
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Body=file_name, Key=s3_key)
    persist_data_to_kinesis_obj = HandleBucketEvent()
    persist_data_to_kinesis_obj.getS3HeadObject(bucket_name, s3_key)
    assert True


# @mock_s3
# def test_fetch_s3_details_from_event():
#     input_file_name = "tests/data/s3event_input.json"
#     bucket_name = "dev-dot-sdc-curated-911061262852-us-east-1"
#     s3_key = "waze/version=20180720/content/state=AK/table=alert/projection=redshift/year=2019/month=05/day=01/hour=00/minute=01/3e9a9c59-8eb7-41e4-92a0-ee4993428e5a.csv.gz"
#
#     with open(input_file_name) as input_file:
#         event_data = json.load(input_file)
#     persist_data_to_kinesis_obj = HandleBucketEvent()
#     bucket, key = persist_data_to_kinesis_obj.fetchS3DetailsFromEvent(event_data)
#     assert bucket == bucket_name
#     assert key == s3_key


@mock_events
def test_handle_bucket_event():
    with pytest.raises(Exception):
        assert HandleBucketEvent.handleBucketEvent(None, None) is None