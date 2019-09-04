import json
import logging
import os
import random

import boto3
import marshmallow
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError


class InputSchema(marshmallow.Schema):
    bucket_name = marshmallow.fields.Str(required=True)
    file_name = marshmallow.fields.Str(required=True)
    queue_url = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    sns_topic_arn = marshmallow.fields.Str(required=True)
    error_handler_arn = marshmallow.fields.Str(required=True)
    sqs_messageid_name = marshmallow.fields.Str(required=True)
    method_name = marshmallow.fields.Str(required=True)
    period = marshmallow.fields.Str(required=True)


class NoDataInQueueError(Exception):
    pass


def lambda_handler(event, context):
    current_module = "Aggregation County - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Aggregation")
    logger.setLevel(10)
    try:
        logger.info("Aggregation county wrangler begun.")

        # Set up clients
        s3 = boto3.resource('s3')
        sqs = boto3.client('sqs')
        lambda_client = boto3.client('lambda')

        # ENV vars
        config, errors = InputSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Vaildated params.")

        # Read from S3
        object = s3.Object(config['bucket_name'], config['file_name'])
        input_file = object.get()['Body'].read()

        data = pd.DataFrame(json.loads(input_file))

        disaggregated_data = data[data.period == int(config['period'])]

        formatted_data = disaggregated_data.to_json(orient='records')

        # Invoke by county method
        by_county = lambda_client.invoke(
            FunctionName=config['method_name'],
            Payload=formatted_data
        )
        json_response = by_county.get('Payload').read().decode("utf-8")

        sqs.send_message(
            QueueUrl=config['queue_url'],
            MessageBody=json.loads(json_response),
            MessageGroupId=config['sqs_messageid_name'],
            MessageDeduplicationId=str(random.getrandbits(128))
        )

        logger.info("Successfully sent data to sqs.")

        send_sns_message()

    except NoDataInQueueError as e:
        error_message = (
            "There was no data in sqs queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(
            e.__traceback__.tb_lineno)
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
            return {"success": True, "checkpoint": config['checkpoint']}


def send_sns_message(aggregation_run_type, checkpoint, sns, arn):
    """
    This function is responsible for sending notifications to the SNS Topic.
    Notifications will be used to relay information to the BPM.
    :param aggregation_run_type:
    Message indicating status of run - Type: String.
    :param checkpoint: Location of process - Type: String.
    :param sns: boto3 SNS client - Type: boto3.client
    :param arn: The Address of the SNS topic - Type: String.
    :return: None.
    """
    sns_message = {
        "success": True,
        "module": "Aggregation",
        "checkpoint": checkpoint,
        "message": aggregation_run_type,
    }

    sns.publish(TargetArn=arn, Message=json.dumps(sns_message))


def get_sqs_message(queue_url):
    """
    Retrieves message from the SQS queue.
    :param queue_url: The url of the SQS queue. - Type: String.
    :return: Message from queue - Type: String.
    """
    sqs = boto3.client("sqs", region_name="eu-west-2")
    return sqs.receive_message(QueueUrl=queue_url)
