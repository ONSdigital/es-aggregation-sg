import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the
    correct format.
    :return: None
    """
    bucket_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the calculate top two
    statistical method.
    The method requires a dataframe which must contain the columns specified by:
    - aggregated column
    - additional aggregated column
    - total columns

    :param event: {"RuntimeVariables":{
        aggregated_column - A column to aggregate by. e.g. Enterprise_Reference.
        additional_aggregated_column - A column to aggregate by. e.g. Region.
        total_columns - The names of the columns to produce aggregations for.
        top1_column - The prefix for the largest_contibutor column
        top2_column - The prefix for the second_largest_contibutor column
    }}
    :param context: N/A
    :return: {"success": True, "checkpoint": 4}
            or LambdaFailure exception
    """
    current_module = "Aggregation Calc Top Two - Wrangler."
    logger = logging.getLogger()
    logger.setLevel(0)
    error_message = ''
    log_message = ''
    checkpoint = 4
    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Starting " + current_module)
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']

        # Needs to be declared inside of the lambda handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")
        logger.info("Setting-up environment configs")

        # Import environment variables using marshmallow validation
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")
        logger.info("Vaildated params")

        # Environment Variables
        bucket_name = config['bucket_name']
        checkpoint = config['checkpoint']
        method_name = config['method_name']
        sns_topic_arn = config['sns_topic_arn']

        # Runtime Variables
        aggregated_column = event['RuntimeVariables']['aggregated_column']
        additional_aggregated_column = \
            event['RuntimeVariables']['additional_aggregated_column']
        in_file_name = event['RuntimeVariables']['in_file_name']
        out_file_name = event['RuntimeVariables']['out_file_name']
        outgoing_message_group_id = event['RuntimeVariables']["outgoing_message_group_id"]
        sqs_queue_url = event['RuntimeVariables']["queue_url"]
        top1_column = event['RuntimeVariables']['top1_column']
        top2_column = event['RuntimeVariables']['top2_column']
        total_columns = event['RuntimeVariables']['total_columns']

        logger.info("Retrieved configuration variables.")

        # Read from S3 bucket
        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name, run_id)
        logger.info("Completed reading data from s3")

        # Add output columns
        logger.info("Appending two further required columns.")
        data[top1_column] = 0
        data[top2_column] = 0

        # Serialise data
        logger.info("Converting dataframe to json.")
        prepared_data = data.to_json(orient='records')

        # Invoke aggregation top2 method
        logger.info("Invoking the statistical method.")

        json_payload = {
            "input_json": prepared_data,
            "total_columns": total_columns,
            "additional_aggregated_column": additional_aggregated_column,
            "aggregated_column": aggregated_column,
            "top1_column": top1_column,
            "top2_column": top2_column
        }

        top2 = lambda_client.invoke(FunctionName=method_name,
                                    Payload=json.dumps(json_payload))

        json_response = json.loads(top2.get('Payload').read().decode("utf-8"))

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        # Sending output to SQS, notice to SNS
        logger.info("Sending function response downstream.")
        aws_functions.save_data(bucket_name, out_file_name,
                                json_response["data"], sqs_queue_url,
                                outgoing_message_group_id, run_id)
        logger.info("Successfully sent the data to S3")
        aws_functions.send_sns_message(checkpoint, sns_topic_arn, "Aggregation - Top 2.")
        logger.info("Successfully sent the SNS message")

    except IndexError as e:
        error_message = ("Required columns missing from input data in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except TypeError as e:
        error_message = ("Bad data encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = ("Parameter validation error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = ("AWS Error ("
                         + str(e.response['Error']['Code']) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id)
                         + " | Run_id: " + str(run_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

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

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if(len(error_message)) > 0:
            logger.error(log_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}
