import json
import logging
import os
import random

import boto3
import marshmallow
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError

# Clients
sqs = boto3.client('sqs', region_name="eu-west-2")
sns = boto3.client('sns', region_name="eu-west-2")
s3 = boto3.resource('s3', region_name="eu-west-2")


class InputSchema(marshmallow.Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    bucket_name = marshmallow.fields.Str(required=True)
    file_name = marshmallow.fields.Str(required=True)
    queue_url = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    sns_topic_arn = marshmallow.fields.Str(required=True)
    sqs_messageid_name = marshmallow.fields.Str(required=True)
    method_name = marshmallow.fields.Str(required=True)
    period = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method is used to prepare data for the calculation of county totals.
    :param event: N/A
    :param context: N/A
    :return: Success - True/False & Checkpoint
    """
    current_module = "Aggregation County - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Aggregation")
    logger.setLevel(10)
    try:
        logger.info("Aggregation county wrangler begun.")

        # Needs to be declared inside the lambda_handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        # ENV vars
        config, errors = InputSchema().load(os.environ)
        bucket_name = config['bucket_name']
        file_name = config['file_name']
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Vaildated params.")

        input_file = read_from_s3(bucket_name, file_name)

        data = pd.DataFrame(input_file)

        disaggregated_data = data[data.period == int(config['period'])]

        formatted_data = disaggregated_data.to_json(orient='records')

        # Invoke by county method
        by_county = lambda_client.invoke(
            FunctionName=config['method_name'],
            Payload=formatted_data
        )
        json_response = by_county.get('Payload').read().decode("utf-8")

        send_sqs_message(config['queue_url'], json_response, config['sqs_messageid_name'])

        logger.info("Successfully sent data to sqs.")

        send_sns_message()

    except AttributeError as e:
        error_message = (
            "Bad data encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(
            e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
            "Parameter validation error"
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(
            e.__traceback__.tb_lineno)
    except ClientError as e:
        error_message = (
            "AWS Error ("
            + str(e.response["Error"]["Code"])
            + ") "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(
            e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
            "Key Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(
            e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = (
            "Incomplete Lambda response encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(
            e.__traceback__.tb_lineno)
    except Exception as e:
        error_message = (
            "General Error in "
            + current_module
            + " ("
            + str(type(e))
            + ") |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(
            e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": 0}


def send_sns_message(checkpoint, sns_topic_arn):
    """
    This method is responsible for sending a notification to the specified arn,
    so that it can be used to relay information for the BPM to use and handle.
    :param checkpoint: The current checkpoint location - Type: String.
    :param sns_topic_arn: The arn of the sns topic you are directing the message at -
                          Type: String.
    :return: None
    """
    sns_message = {
        "success": True,
        "module": "Aggregation",
        "checkpoint": checkpoint,
        "message": ""
    }

    return sns.publish(TargetArn=sns_topic_arn, Message=json.dumps(sns_message))


def send_sqs_message(queue_url, message, output_message_id):
    """
    This method is responsible for sending data to the SQS queue and deleting the
    left-over data.
    :param queue_url: The url of the SQS queue. - Type: String
    :param message: The message/data you wish to send to the SQS queue - Type: String
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :return: None
    """
    # MessageDeduplicationId is set to a random hash to overcome de-duplication,
    # otherwise modules could not be re-run in the space of 5 Minutes.
    return sqs.send_message(QueueUrl=queue_url,
                            MessageBody=message,
                            MessageGroupId=output_message_id,
                            MessageDeduplicationId=str(random.getrandbits(128))
                            )


def read_from_s3(bucket_name, file_name):
    """
    Given the name of the bucket and the filename(key), this function will
    return a file. File is JSON format.
    :param bucket_name: Name of the S3 bucket - Type: String
    :param file_name: Name of the file - Type: String
    :return: input_file: The JSON file in S3 - Type: JSON
    """
    object = s3.Object(bucket_name, file_name)
    input_file = object.get()['Body'].read()

    return input_file
