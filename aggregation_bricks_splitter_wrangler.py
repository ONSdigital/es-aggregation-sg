import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)


def lambda_handler(event, context):
    """
    The wrangler converts the data from JSON format into a dataframe and then edits data.
    This process consolidates 36 columns of data down to 12 and adds brick_type, then
    creates two outputs. One with the GB region added and one with a
    consolidated brick_type.

    :param event: Contains all the variables which are required for the specific run.
    :param context: N/A

    :return:  Success & Checkpoint/Error - Type: JSON
    """
    current_module = "Pre Aggregation Data Wrangler."
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Pre Aggregation Data Wrangler")
    logger.setLevel(10)
    # Define run_id outside of try block
    run_id = 0
    try:

        logger.info("Starting " + current_module)

        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']

        # Environment Variables.
        sqs = boto3.client('sqs', region_name="eu-west-2")
        lambda_client = boto3.client('lambda', region_name="eu-west-2")
        config, errors = EnvironSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Vaildated params")

        # Environment Variables
        checkpoint = config["checkpoint"]
        bucket_name = config["bucket_name"]
        method_name = config["method_name"]
        sns_topic_arn = config["sns_topic_arn"]

        # Runtime Variables
        column_list = event['RuntimeVariables']['total_columns']
        factors_parameters = event['RuntimeVariables']["factors_parameters"]
        in_file_name = event['RuntimeVariables']['in_file_name']
        incoming_message_group_id = event['RuntimeVariables']['incoming_message_group_id']
        out_file_name_bricks = event['RuntimeVariables']['out_file_name_bricks']
        out_file_name_region = event['RuntimeVariables']['out_file_name_region']
        region_column = factors_parameters['RuntimeVariables']['region_column']
        regionless_code = factors_parameters['RuntimeVariables']['regionless_code']
        sqs_queue_url = event['RuntimeVariables']["queue_url"]
        unique_identifier = event['RuntimeVariables']['unique_identifier']

        logger.info("Retrieved configuration variables.")

        # Pulls In Data.
        data, receipt_handler = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                            in_file_name,
                                                            incoming_message_group_id,
                                                            run_id)

        logger.info("Succesfully retrieved data.")

        brick_type = {
            "clay": 3,
            "concrete": 2,
            "sandlime": 4
        }

        new_type = 1  # This number represents Clay & Sandlime Combined

        # Identify The Brick Type Of The Row.
        data[unique_identifier[0]] = data.apply(
            lambda x: calculate_row_type(x, brick_type, column_list), axis=1)

        # Collate Each Rows 12 Good Brick Type Columns And 24 Empty Columns Down
        # Into 12 With The Same Name.
        data = data.apply(lambda x: sum_columns(x, brick_type, column_list,
                                                unique_identifier), axis=1)

        # Old Columns With Brick Type In The Name Are Dropped.
        for check_type in brick_type.keys():
            for current_column in column_list:
                data.drop([check_type + "_" + current_column], axis=1, inplace=True)

        # Add GB Region For Aggregation By Region.
        logger.info("Creating File For Aggregation By Region.")
        data_region = data.to_json(orient="records")

        payload = {
            "json_data": json.loads(data_region),
            "regionless_code": regionless_code,
            "region_column": region_column
        }

        # Pass the data for processing (adding of the regionless region.
        imputed_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload),
        )
        logger.info("Succesfully invoked method.")

        json_response = json.loads(imputed_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        region_dataframe = pd.DataFrame(json.loads(json_response["data"]))

        totals_dict = {total_column: "sum" for total_column in column_list}

        data_region = region_dataframe.groupby(
            unique_identifier[1:]).agg(
            totals_dict).reset_index()

        region_output = data_region.to_json(orient='records')

        aws_functions.save_to_s3(bucket_name, out_file_name_region,
                                 region_output, run_id)

        logger.info("Successfully sent data to s3")

        # Collate Brick Types Clay And Sand Lime Into A Single Type And Add To Data
        # For Aggregation By Brick Type.
        logger.info("Creating File For Aggregation By Brick Type.")
        data_brick = data.copy()

        data = data[data[unique_identifier[0]] != brick_type["concrete"]]
        data[unique_identifier[0]] = new_type

        data_brick = pd.concat([data_brick, data])

        brick_dataframe = data_brick.groupby(unique_identifier[0:2]
                                             ).agg(totals_dict).reset_index()

        brick_output = brick_dataframe.to_json(orient='records')
        aws_functions.save_to_s3(bucket_name, out_file_name_bricks, brick_output, run_id)

        logger.info("Successfully sent data to s3")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        logger.info(aws_functions.send_sns_message(checkpoint, sns_topic_arn,
                                                   "Pre Aggregation."))

        logger.info("Succesfully sent message to sns")

    except AttributeError as e:
        error_message = (
                "Bad data encountered in "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
                + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
                "Parameter validation error in "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
                + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ClientError as e:
        error_message = (
                "AWS Error ("
                + str(e.response["Error"]["Code"])
                + ") "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
                + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
                "Key Error in "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
                + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = (
                "Incomplete Lambda response encountered in "
                + current_module
                + " |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
                + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except exception_classes.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "." \
                      + " | Run_id: " + str(run_id)
    except Exception as e:
        error_message = (
                "General Error in "
                + current_module
                + " ("
                + str(type(e))
                + ") |- "
                + str(e.args)
                + " | Request ID: "
                + str(context.aws_request_id)
                + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}


def calculate_row_type(row, brick_type, column_list):
    """
    Takes a row and adds up all columns of the current type.
    If there is data we know it is the current type.

    :param row: Contains all data. - Row.
    :param brick_type: Dictionary of the possible brick types. - Dict.
    :param column_list: List of the columns that need to be added. - List.

    :return:  brick_type - Int.
    """

    for check_type in brick_type.keys():
        total_for_type = 0

        for current_column in column_list:
            total_for_type += row[check_type + "_" + current_column]

        if total_for_type > 0:
            return brick_type[check_type]


def sum_columns(row, brick_type, column_list, unique_identifier):
    """
    Takes a row and the columns with data, then adds that data to the generically
    named columns.

    :param row: Contains all data. - Row.
    :param brick_type: Dictionary of the possible brick types. - Dict.
    :param column_list: List of the columns that need to be added to. - List.

    :return:  Updated row. - Row.
    """

    for check_type in brick_type.keys():
        if row[unique_identifier[0]] == brick_type[check_type]:
            for current_column in column_list:
                row[current_column] = row[check_type + "_" + current_column]

    return row
