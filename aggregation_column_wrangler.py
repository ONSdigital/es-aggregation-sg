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
    period_column = fields.Str(required=True)
    region_column = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method is used to prepare data for the calculation of column totals.
    :param event: N/A
    :param context: N/A
    :return: Success - True/False & Checkpoint
    """
    current_module = "Aggregation by column - Wrangler"
    error_message = ""
    log_message = ""
    checkpoint = 4
    logger = logging.getLogger("Aggregation")
    logger.setLevel(0)
    try:
        logger.info("Started Aggregation - Wrangler.")

        # Needs to be declared inside the lambda_handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        period = event['RuntimeVariables']['period']
        aggregation_type = event['RuntimeVariables']['aggregation_type']
        aggregated_column = event['RuntimeVariables']['aggregated_column']
        cell_total_column = event['RuntimeVariables']['cell_total_column']
        total_column = event['RuntimeVariables']['total_column']

        # ENV vars
        config, errors = InputSchema().load(os.environ)

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
        period_column = config['period_column']
        region_column = config['region_column']

        logger.info("Validated params.")

        out_file_name = cell_total_column + "_" + out_file_name

        # Read from S3 bucket
        data = funk.read_dataframe_from_s3(bucket_name, in_file_name)
        logger.info("Completed reading data from s3")

        disaggregated_data = data[data.period == int(period)]

        formatted_data = disaggregated_data.to_json(orient='records')
        logger.info("Filtered disaggregated_data")

        json_payload = {
            "input_json": formatted_data,
            "total_column": total_column,
            "period_column": period_column,
            "region_column": region_column,
            "aggregated_column": aggregated_column,
            "cell_total_column": cell_total_column,
            "aggregation_type": aggregation_type
        }

        by_column = lambda_client.invoke(FunctionName=method_name,
                                         Payload=json.dumps(json_payload))

        json_response = json.loads(by_column.get('Payload').read().decode("utf-8"))

        logger.info("Successfully invoked the method lambda")

        if str(type(json_response)) != "<class 'list'>":
            raise funk.MethodFailure(json_response['error'])

        funk.save_data(bucket_name, out_file_name, json.dumps(json_response),
                       sqs_queue_url, sqs_message_group_id)
        logger.info("Successfully sent the data to SQS")

        funk.send_sns_message(checkpoint,
                              sns_topic_arn,
                              "Aggregation - " + aggregated_column + ".")

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
            return {"success": True, "checkpoint": checkpoint}