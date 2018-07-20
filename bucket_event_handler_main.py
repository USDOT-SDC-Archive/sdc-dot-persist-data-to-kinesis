from kinesis_data_persist_handler.kinesis_event_lambda_handler import *


def lambda_handler(event, context):
    handle_bucket_event = HandleBucketEvent()
    handle_bucket_event.handleBucketEvent(event, context)