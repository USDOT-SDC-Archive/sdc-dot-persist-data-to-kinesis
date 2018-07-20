import os
import boto3
import json
import datetime
import urllib.parse
from botocore.exceptions import ClientError
from common.constants import *
from common.logger_utility import *


class HandleBucketEvent:

    def __fetchS3DetailsFromEvent(self, event):
        try:
            sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
            bucket = sns_message["Records"][0]["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(sns_message["Records"][0]["s3"]["object"]["key"])
        except Exception as e:
            LoggerUtility.logError(str(e))
            LoggerUtility.logError("Failed to process the event")
            raise e
        else:
            LoggerUtility.logInfo("Bucket name: " + bucket)
            LoggerUtility.logInfo("Object key: " + key)
            return bucket, key

    def __getS3HeadObject(self, bucket_name, object_key):
        s3_client = boto3.client(Constants.S3_SERVICE_CLIENT)
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        except ClientError as e:
            LoggerUtility.logError(e)
            LoggerUtility.logError('Error getting object {} from bucket {}. Make sure they exist, '
                                   'your bucket is in the same region as this function and necessary permissions '
                                   'have been granted.'.format(object_key, bucket_name))
            raise e
        else:
            return response

    def __sendDatatoKinesis(self, metadata_object):
        kinesis_client = boto3.client('kinesis', region_name='us-east-1')
        put_response = kinesis_client.put_record(
            StreamName='persist-curated-data',
            Data=json.dumps(metadata_object),
            PartitionKey=str(datetime.datetime.utcnow())
        )
        LoggerUtility.logInfo("Response of Put record from kinesis:"+str(put_response))

    def handleBucketEvent(self, event, context):
        LoggerUtility.setLevel()
        bucket_name, object_key = self.__fetchS3DetailsFromEvent(event)
        s3_head_object = self.__getS3HeadObject(bucket_name, object_key)
        metadata_object = s3_head_object["Metadata"]
        metadata_object["bucket-name"] = bucket_name
        metadata_object["s3-key"] = object_key
        LoggerUtility.logInfo("S3 METADATA"+ str(metadata_object))
        self.__sendDatatoKinesis(metadata_object)