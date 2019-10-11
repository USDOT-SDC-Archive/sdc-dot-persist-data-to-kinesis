from lambdas.kinesis_event_lambda_handler import *


def lambda_handler(event, *args, **kwargs):
    handle_bucket_event = HandleBucketEvent()
    handle_bucket_event.handle_bucket_event(event)
