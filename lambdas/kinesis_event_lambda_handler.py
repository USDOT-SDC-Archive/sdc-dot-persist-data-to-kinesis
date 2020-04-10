import datetime
import json
import os
import urllib.parse

import boto3
from botocore.exceptions import ClientError

from common.logger_utility import LoggerUtility


class HandleBucketEvent:

    def fetch_s3_details_from_event(self, event):
        """
        Grab sns_message, bucket, and key from the event
        :param event: A dictionary with a json object inside
        :return:
        """
        try:
            sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
            bucket = sns_message["Records"][0]["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(sns_message["Records"][0]["s3"]["object"]["key"])
        except Exception as e:
            LoggerUtility.log_error(str(e))
            LoggerUtility.log_error("Failed to process the event")
            raise e
        else:
            LoggerUtility.log_info("Bucket name: " + bucket)
            LoggerUtility.log_info("Object key: " + key)
            return bucket, key

    def get_s3_head_object(self, bucket_name, object_key):
        s3_client = boto3.client('s3', region_name='us-east-1')
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        except ClientError as e:
            LoggerUtility.log_error(e)
            LoggerUtility.log_error('Error getting object {} from bucket {}. Make sure they exist, '
                                    'your bucket is in the same region as this function and necessary permissions '
                                    'have been granted.'.format(object_key, bucket_name))
            raise e
        else:
            return response

    def send_data_to_kinesis(self, metadata_object):
        kinesis_client = boto3.client('kinesis', region_name='us-east-1')
        kinesis_stream = os.environ["KINESIS_STREAM"]
        put_response = kinesis_client.put_record(
            StreamName=kinesis_stream,
            Data=json.dumps(metadata_object),
            PartitionKey=str(datetime.datetime.utcnow())
        )
        LoggerUtility.log_info("Response of Put record from kinesis:" + str(put_response))

    def handle_bucket_event(self, event):
        LoggerUtility.set_level()
        bucket_name, object_key = self.fetch_s3_details_from_event(event)
        s3_head_object = self.get_s3_head_object(bucket_name, object_key)
        data_set = object_key.split("/")[0]
        if data_set == "waze":
            table = object_key.split("/")[4]
            if table == "table=alert":
                metadata_object = s3_head_object["Metadata"]
                metadata_object["bucket-name"] = bucket_name
                metadata_object["s3-key"] = object_key
                LoggerUtility.log_info("S3 METADATA" + str(metadata_object))
                LoggerUtility.log_info("Is historical:" + metadata_object["is-historical"])
                if metadata_object["is-historical"] == "True":
                    LoggerUtility.log_info("Historical Data found ,hence skipping sending it to kinesis")
                else:
                    self.sendDatatoKinesis(metadata_object)
                    LoggerUtility.log_info("Sent data to kinesis data stream")
            else:
                LoggerUtility.log_info("Skipping sending to Kinesis for table " + table)
        else:
            LoggerUtility.log_info("Skipping sending data to kinesis for the data set:" + data_set)