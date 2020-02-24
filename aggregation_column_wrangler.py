import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields


class InputSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    bucket_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method is used to prepare data for the calculation of column totals.

    :param event: {"RuntimeVariables":{
        aggregated_column - A column to aggregate by. e.g. Enterprise_Reference.
        additional_aggregated_column - A column to aggregate by. e.g. Region.
        aggregation_type - How we wish to do the aggregation. e.g. sum, count, nunique.
        total_columns - The names of the columns to produce aggregations for.
        cell_total_column - Name of column to rename each total_column.
                        Is concatenated to the front of the total_column name.
    }}

    :param context: N/A
    :return: {"success": True, "checkpoint":4}
            or LambdaFailure exception
    """
    current_module = "Aggregation by column - Wrangler"
    error_message = ""
    log_message = ""
    checkpoint = 4
    logger = logging.getLogger("Aggregation")
    logger.setLevel(0)

    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Started Aggregation - Wrangler.")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']

        # Needs to be declared inside the lambda_handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        # ENV vars
        config, errors = InputSchema().load(os.environ)

        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params")

        # Environment Variables
        bucket_name = config['bucket_name']
        checkpoint = config['checkpoint']
        method_name = config['method_name']
        sns_topic_arn = config['sns_topic_arn']

        # Runtime Variables
        additional_aggregated_column = \
            event['RuntimeVariables']['additional_aggregated_column']
        aggregated_column = event['RuntimeVariables']['aggregated_column']
        aggregation_type = event['RuntimeVariables']['aggregation_type']
        cell_total_column = event['RuntimeVariables']['cell_total_column']
        in_file_name = event['RuntimeVariables']['in_file_name']
        location = event['RuntimeVariables']['location']
        out_file_name = event['RuntimeVariables']['out_file_name']
        outgoing_message_group_id = event['RuntimeVariables']["outgoing_message_group_id"]
        sqs_queue_url = event['RuntimeVariables']["queue_url"]
        total_columns = event['RuntimeVariables']['total_columns']

        logger.info("Retrieved configuration variables.")

        out_file_name = cell_total_column + "_" + out_file_name

        # Read from S3 bucket
        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name, location)
        logger.info("Completed reading data from s3")

        formatted_data = data.to_json(orient='records')
        logger.info("Formated disaggregated_data")

        json_payload = {
            "input_json": formatted_data,
            "total_columns": total_columns,
            "additional_aggregated_column": additional_aggregated_column,
            "aggregated_column": aggregated_column,
            "cell_total_column": cell_total_column,
            "aggregation_type": aggregation_type
        }

        by_column = lambda_client.invoke(FunctionName=method_name,
                                         Payload=json.dumps(json_payload))

        json_response = json.loads(by_column.get('Payload').read().decode("utf-8"))

        logger.info("Successfully invoked the method lambda")

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        aws_functions.save_data(bucket_name, out_file_name, json_response["data"],
                                sqs_queue_url, outgoing_message_group_id, location)
        logger.info("Successfully sent the data to SQS")

        aws_functions.send_sns_message(checkpoint,
                                       sns_topic_arn,
                                       "Aggregation - " + aggregated_column + ".")

        logger.info("Successfully sent the SNS message")

    except ValueError as e:
        error_message = ("Parameter validation error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = ("AWS Error in ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except exception_classes.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "." \
                      + " | Run_id: " + str(run_id)

    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}
