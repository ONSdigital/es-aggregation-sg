import json
import traceback
import boto3
import pandas as pd
import os
import random

# Set up clients
s3 = boto3.resource('s3')
sqs = boto3.client('sqs')
sns = boto3.client('sns')
lambda_client = boto3.client('lambda')

# S3
bucket_name = os.environ['bucket_name']
file_name = os.environ['file_name']

# SQS
queue_url = os.environ['queue_url']

# SNS
checkpoint = os.environ['checkpoint']
sns_topic_arn = os.environ['sns_topic_arn']

error_handler_arn = os.environ['error_handler_arn']
sqs_messageid_name = os.environ['sqs_messageid_name']
method_name = os.environ['method_name']
period = os.environ['period']


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """

    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def send_sns_message():
    sns_message = {
        "success": True,
        "module": "AggByRegion",
        "checkpoint": checkpoint,
        "anomalies": "PLACE HOLDER",
        "message": ""
    }

    sns.publish(
        TargetArn=sns_topic_arn,
        Message=json.dumps(sns_message)
    )


def lambda_handler(event, context):
    try:
        # Reads in Data from SQS Queue
        # response = sqs.receive_message(QueueUrl=queue_url)
        # message = response['Messages'][0]
        # message_json = json.loads(message['Body'])

        # TESTING - Read from S3
        object = s3.Object(bucket_name, file_name)
        input_file = object.get()['Body'].read()

        data = pd.DataFrame(json.loads(input_file))

        disaggregated_data = data[data.period == int(period)]

        formatted_data = disaggregated_data.to_json(orient='records')

        # Invoke by region method
        by_region = lambda_client.invoke(FunctionName=method_name, Payload=formatted_data)
        json_response = by_region.get('Payload').read().decode("utf-8")

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.loads(json_response),
                         MessageGroupId=sqs_messageid_name,
                         MessageDeduplicationId=str(random.getrandbits(128)))

        send_sns_message()

    except Exception as exc:
        # Invoke error handler lambda
        # lambda_client.invoke(
        #     FunctionName=error_handler_arn,
        #     InvocationType='Event',
        #     Payload=json.loads(_get_traceback(exc))
        # )

        return {
            "success": False,
            "error": "Unexpected Wrangler exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": checkpoint
    }
