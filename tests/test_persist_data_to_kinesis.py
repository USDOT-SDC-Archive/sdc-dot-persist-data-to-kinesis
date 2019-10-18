import json
import os
import sys
from unittest import mock

import boto3
import pytest
from moto import mock_kinesis, mock_s3

from common.logger_utility import LoggerUtility
from lambdas.kinesis_event_lambda_handler import HandleBucketEvent

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# setup

s3_key = "waze/version=20180720/content/state=AK/table=alert/projection=redshift/year=2019/month=05/day=01/hour=00/minute=01/3e9a9c59-8eb7-41e4-92a0-ee4993428e5a.csv.gz"
bucket_name = "dev-dot-sdc-curated-911061262852-us-east-1"


def mock_fetch_s3_details_from_event(*args, **kwargs):
    return "bucket_name", "waze/object_key"


def mock_fetch_s3_details_from_event_not_waze(*args, **kwargs):
    return "bucket_name", "none/object_key"


mock_s3_head_object = {
    "Metadata": {
        "bucket-name": "bucket-name",
        "s3-key": "s3-key",
        "is-historical": "True"
    }
}


def mock_get_s3_head_object(*args, **kwargs):
    return mock_s3_head_object


@mock_kinesis
def test_send_data_to_kinesis():
    stream_name = "dev-dot-sdc-persist-curated-data"
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
    os.environ["KINESIS_STREAM"] = stream_name
    metadata_object = dict()
    metadata_object["bucket-name"] = bucket_name
    metadata_object["s3-key"] = s3_key
    metadata_object["is-historical"] = "false"
    persist_data_to_kinesis_obj = HandleBucketEvent()
    persist_data_to_kinesis_obj.send_data_to_kinesis(metadata_object)
    assert True


@mock_s3
def test_get_s3_head_object():
    file_name = "data/3e9a9c59-8eb7-41e4-92a0-ee4993428e5a.csv.gz"
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Body=file_name, Key=s3_key)
    persist_data_to_kinesis_obj = HandleBucketEvent()
    persist_data_to_kinesis_obj.get_s3_head_object(bucket_name, s3_key)
    assert True


def test_fetch_s3_details_from_event():
    """
    Test that fetch_s3_details_from_event returns the correct values from an event.
    """
    message = {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "bucket"
                    },
                    "object": {
                        "key": "key"
                    }
                }
            }
        ]
    }

    event = {
        "Records": [
            {
                "Sns": {
                    "Message": json.dumps(message)
                }
            }

        ]
    }

    persist_data_to_kinesis_obj = HandleBucketEvent()
    bucket, key = persist_data_to_kinesis_obj.fetch_s3_details_from_event(event)

    assert bucket == "bucket"
    assert key == "key"


def test_fetch_s3_details_from_event_exception():
    """
    Tests that LoggerUtility.log_error is called when the event's Message is None.
    """
    LoggerUtility.log_error = mock.MagicMock()
    persist_data_to_kinesis_obj = HandleBucketEvent()

    with pytest.raises(TypeError):
        event = {
            "Records": [
                {
                    "Sns": {
                        "Message": None
                    }
                }

            ]
        }
        persist_data_to_kinesis_obj.fetch_s3_details_from_event(event)

    LoggerUtility.log_error.assert_called()


def test_handle_bucket_event_historical():
    """
    Verify that handle_bucket_event doesn't call send_data_to_kinesis when historical data passed in.
    """

    persist_data_to_kinesis_obj = HandleBucketEvent()
    persist_data_to_kinesis_obj.fetch_s3_details_from_event = mock_fetch_s3_details_from_event
    persist_data_to_kinesis_obj.get_s3_head_object = mock_get_s3_head_object
    persist_data_to_kinesis_obj.send_data_to_kinesis = mock.MagicMock()

    persist_data_to_kinesis_obj.handle_bucket_event("event")

    assert not persist_data_to_kinesis_obj.send_data_to_kinesis.called


def test_handle_bucket_event_not_historical():
    """
    Verify that handle_bucket_event calls send_data_to_kinesis when the metadata isn't historical.
    """

    mock_s3_head_object["Metadata"]["is-historical"] = "False"
    persist_data_to_kinesis_obj = HandleBucketEvent()
    persist_data_to_kinesis_obj.fetch_s3_details_from_event = mock_fetch_s3_details_from_event
    persist_data_to_kinesis_obj.get_s3_head_object = mock_get_s3_head_object
    persist_data_to_kinesis_obj.send_data_to_kinesis = mock.MagicMock()

    persist_data_to_kinesis_obj.handle_bucket_event("event")

    persist_data_to_kinesis_obj.send_data_to_kinesis.assert_called_once_with(mock_s3_head_object["Metadata"])


def test_handle_bucket_event_not_waze():
    """
    Verify that handle_bucket_event doesn't call send_data_to_kinesis when the data_set != "waze"
    """

    mock_s3_head_object["Metadata"]["is-historical"] = "False"
    persist_data_to_kinesis_obj = HandleBucketEvent()
    persist_data_to_kinesis_obj.fetch_s3_details_from_event = mock_fetch_s3_details_from_event_not_waze
    persist_data_to_kinesis_obj.get_s3_head_object = mock_get_s3_head_object
    persist_data_to_kinesis_obj.send_data_to_kinesis = mock.MagicMock()

    persist_data_to_kinesis_obj.handle_bucket_event("event")

    assert not persist_data_to_kinesis_obj.send_data_to_kinesis.called
