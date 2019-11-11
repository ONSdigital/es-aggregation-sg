import logging
import os
import json

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class InputSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    bucket_name = fields.Str(required=True)
    s3_file = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    arn = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    file_name = fields.Str(required=True)


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
        placeholder = context.aws_request_id
        context={}
        context['aws_request_id'] = placeholder

        logger.info("Aggregation county wrangler begun.")

        # Needs to be declared inside the lambda_handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        # ENV vars
        config, errors = InputSchema().load(os.environ)

        period = event['RuntimeVariables']['period']
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        sqs_messageid_name = config['sqs_messageid_name']
        bucket_name = config['bucket_name']
        s3_file = config['s3_file']
        queue_url = config['queue_url']
        file_name = config['file_name']
        checkpoint = config['checkpoint']
        arn = config['arn']

        logger.info("Vaildated params.")

        # Read from S3 bucket
        data = funk.read_dataframe_from_s3(bucket_name, s3_file)
        logger.info("Completed reading data from s3")

        disaggregated_data = data[data.period == int(period)]

        formatted_data = disaggregated_data.to_json(orient='records')

        # Invoke by county method
        by_county = lambda_client.invoke(
            FunctionName=config['method_name'],
            Payload=formatted_data
        )
        json_response = json.loads(by_county.get('Payload').read().decode("utf-8"))

        funk.save_data(bucket_name, file_name,
                       json_response, queue_url, sqs_messageid_name)

        logger.info("Successfully sent the data to SQS")
        logger.info("Successfully sent data to sqs.")

        funk.send_sns_message(checkpoint, arn, "County Total completed successfully")
        logger.info("Successfully sent the SNS message")

    except AttributeError as e:
        error_message = ("Bad data encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = ("Parameter validation error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = ("AWS Error in ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context["aws_request_id"]))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": 0}
