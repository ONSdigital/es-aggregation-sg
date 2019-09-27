import json
import logging
import os
import random

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from marshmallow import Schema, fields

# Set up clients
sqs = boto3.client('sqs', region_name='eu-west-2')
sns = boto3.client('sns', region_name='eu-west-2')


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    bucket_name = fields.Str(required=True)
    file_name = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    period = fields.Str(required=True)


class NoDataInQueueError(Exception):
    pass


def lambda_handler(event, context):
    """
    This method ia used to prepare data for the calculation of enterprise reference count.
    :param event: N/A
    :param context: N/A
    :return: Success - True/False & Checkpoint
    """
    current_module = "Aggregation CalculateEnterpriseRef - Wrangler"
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger()
    error_message = ''
    log_message = ''
    checkpoint = 0

    try:

        logger.info("Starting " + current_module)

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)

        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # Needs to be declared inside of the lambda handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        bucket_name = config['bucket_name']
        file_name = config['file_name']
        queue_url = config['queue_url']
        checkpoint = config['checkpoint']
        sns_topic_arn = config['sns_topic_arn']
        sqs_messageid_name = config['sqs_messageid_name']
        method_name = config['method_name']
        period = event['RuntimeVariables']['period']

        logger.info("Set-up environment configs")

        data = get_from_s3(bucket_name, file_name)
        logger.info("Retrieved data from S3")

        disaggregated_data = data[data.period == int(period)]
        formatted_data = disaggregated_data.to_json(orient='records')
        logger.info("Filtered disaggregated_data")

        by_region = lambda_client.invoke(FunctionName=method_name, Payload=formatted_data)
        logger.info("Successfully invoked the method lambda")

        json_response = json.loads(by_region.get('Payload').read().decode("utf-8"))

        send_sqs_message(queue_url, json_response, sqs_messageid_name)
        logger.info("Successfully sent the data to SQS")

        send_sns_message(checkpoint, sns_topic_arn)
        logger.info("Successfully sent the SNS message")

    except AttributeError as e:
        error_message = "Bad data encountered in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = "Parameter validation error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = "AWS Error in (" \
                        + str(e.response['Error']['Code']) + ") " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = "Key Error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = "Incomplete Lambda response encountered in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = "General Error in " \
                        + current_module + " (" \
                        + str(type(e)) + ") |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:

        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}


def get_from_s3(bucket_name, file_name):
    """
    Given the name of the bucket and the filename(key), this function will
    return a DataFrame. File to get MUST be json format.
    :param bucket_name: Name of the s3 bucket - Type: String
    :param file_name: Name of the file - Type: String
    :return: data: DataFrame created from the json file in s3 - Type: DataFrame
    """
    s3 = boto3.resource('s3', region_name="eu-west-2")
    content_object = s3.Object(bucket_name, file_name)
    file_content = content_object.get()['Body'].read()
    data = pd.DataFrame(json.loads(file_content))

    return data


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
