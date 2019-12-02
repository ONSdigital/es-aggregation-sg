import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class InputSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)


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
    logger.setLevel(0)
    try:
        logger.info("Started Aggregation County - Wrangler.")

        # Needs to be declared inside the lambda_handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        # ENV vars
        config, errors = InputSchema().load(os.environ)

        period = event['RuntimeVariables']['period']
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        checkpoint = config['checkpoint']
        bucket_name = config['bucket_name']
        in_file_name = config['in_file_name']
        method_name = config['method_name']
        out_file_name = config['out_file_name']
        sns_topic_arn = config['sns_topic_arn']
        sqs_message_group_id = config['sqs_message_group_id']
        sqs_queue_url = config['sqs_queue_url']

        logger.info("Validated params.")

        # Read from S3 bucket
        data = funk.read_dataframe_from_s3(bucket_name, in_file_name)
        logger.info("Completed reading data from s3")

        disaggregated_data = data[data.period == int(period)]

        formatted_data = disaggregated_data.to_json(orient='records')

        # Invoke by county method
        by_county = lambda_client.invoke(
            FunctionName=method_name,
            Payload=formatted_data
        )
        json_response = json.loads(by_county.get('Payload').read().decode("utf-8"))

        if str(type(json_response)) != "<class 'list'>":
            raise funk.MethodFailure(json_response['error'])

        funk.save_data(bucket_name, out_file_name,
                       json.dumps(json_response), sqs_queue_url, sqs_message_group_id)

        logger.info("Successfully sent the data to SQS")
        logger.info("Successfully sent data to sqs.")

        funk.send_sns_message(checkpoint, sns_topic_arn, "Aggregation - County.")
        logger.info("Successfully sent the SNS message")

    except AttributeError as e:
        error_message = ("Bad data encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = ("Parameter validation error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = ("AWS Error in ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except funk.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "."

    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": 0}
