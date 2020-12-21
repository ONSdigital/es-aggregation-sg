import json
import logging
import os

import boto3
import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields
from marshmallow.validate import Equal


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)


class FactorsSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    region_column = fields.Str(required=True)
    regionless_code = fields.Int(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    total_columns = fields.List(fields.String, required=True)
    environment = fields.Str(Required=True)
    bpm_queue_url = fields.Str(required=True)
    factors_parameters = fields.Dict(
        keys=fields.String(validate=Equal(comparable="RuntimeVariables")),
        values=fields.Nested(FactorsSchema, required=True))
    in_file_name = fields.Str(required=True)
    out_file_name_bricks = fields.Str(required=True)
    out_file_name_region = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    survey = fields.Str(required=True)
    unique_identifier = fields.List(fields.String, required=True)


def lambda_handler(event, context):
    """
    The wrangler converts the data from JSON format into a dataframe and then edits data.
    This process consolidates 36 columns of data down to 12 and adds brick_type, then
    creates two outputs. One with the GB region added and one with a
    consolidated brick_type.

    :param event: Contains all the variables which are required for the specific run.
    :param context: N/A

    :return:  Success & None/Error - Type: JSON
    """
    current_module = "Pre Aggregation Data Wrangler."
    error_message = ""

    # Define run_id outside of try block
    run_id = 0

    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        column_list = runtime_variables["total_columns"]
        environment = runtime_variables["environment"]
        factors_parameters = runtime_variables["factors_parameters"]["RuntimeVariables"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name_bricks = runtime_variables["out_file_name_bricks"]
        out_file_name_region = runtime_variables["out_file_name_region"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        survey = runtime_variables["survey"]
        unique_identifier = runtime_variables["unique_identifier"]

        # Factors Parameters
        region_column = factors_parameters["region_column"]
        regionless_code = factors_parameters["regionless_code"]
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module, run_id,
                                                           context=context)
        raise exception_classes.LambdaFailure(error_message)
    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context)

        raise exception_classes.LambdaFailure(error_message)
    try:

        # Send start of module status to BPM.
        # (NB: Current step and total steps omitted to display as "-" in bpm.)
        status = "IN PROGRESS"
        aws_functions.send_bpm_status(bpm_queue_url, current_module, status, run_id)

        # Pulls In Data.
        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

        logger.info("retrieved data from s3")
        new_type = 1  # This number represents Clay & Sandlime Combined
        brick_type = {
            "clay": 3,
            "concrete": 2,
            "sandlime": 4
        }

        # Prune rows that contain no data
        questions_list = [brick + "_" + column
                          for column in column_list
                          for brick in brick_type.keys()]
        data["zero_data"] = data.apply(
            lambda x: do_check(x, questions_list), axis=1)
        data = data[~data["zero_data"]]
        data.drop(["zero_data"], axis=1, inplace=True)

        # Identify The Brick Type Of The Row.
        data[unique_identifier[0]] = data.apply(
            lambda x: calculate_row_type(x, brick_type, column_list), axis=1)

        # Collate Each Rows 12 Good Brick Type Columns And 24 Empty Columns Down
        # Into 12 With The Same Name.
        data = data.apply(lambda x: sum_columns(x, brick_type, column_list,
                                                unique_identifier), axis=1)

        # Old Columns With Brick Type In The Name Are Dropped.
        for question in questions_list:
            data.drop([question], axis=1, inplace=True)

        # Add GB Region For Aggregation By Region.
        logger.info("Creating File For Aggregation By Region.")
        data_region = data.to_json(orient="records")

        payload = {
            "RuntimeVariables": {
                "data": json.loads(data_region),
                "environment": environment,
                "regionless_code": regionless_code,
                "region_column": region_column,
                "run_id": run_id,
                "survey": survey,
                "bpm_queue_url": bpm_queue_url
            }
        }

        # Pass the data for processing (adding of the regionless region.
        gb_region_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload)
        )
        logger.info("Succesfully invoked method.")

        json_response = json.loads(gb_region_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        region_dataframe = pd.DataFrame(json.loads(json_response["data"]))

        totals_dict = {total_column: "sum" for total_column in column_list}

        data_region = region_dataframe.groupby(
            unique_identifier[1:]).agg(
            totals_dict).reset_index()

        region_output = data_region.to_json(orient="records")

        aws_functions.save_to_s3(bucket_name, out_file_name_region, region_output)

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

        brick_output = brick_dataframe.to_json(orient="records")
        aws_functions.save_to_s3(bucket_name, out_file_name_bricks, brick_output)

        logger.info("Successfully sent data to s3")

        logger.info(aws_functions.send_sns_message(sns_topic_arn,
                                                   "Pre Aggregation."))

        logger.info("Succesfully sent message to sns")

    except Exception as e:
        error_message = general_functions.handle_exception(e,
                                                           current_module,
                                                           run_id,
                                                           context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    # Send end of module status to BPM.
    # (NB: Current step and total steps omitted to display as "-" in bpm.)
    status = "DONE"
    aws_functions.send_bpm_status(bpm_queue_url, current_module, status, run_id)

    return {"success": True}


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
    :param unique_identifier: List of columns to make each row unique. - List.

    :return:  Updated row. - Row.
    """

    for check_type in brick_type.keys():
        if row[unique_identifier[0]] == brick_type[check_type]:
            for current_column in column_list:
                row[current_column] = row[check_type + "_" + current_column]

    return row


def do_check(row, questions_list):
    """
    Prunes rows that contain 0 for all question values.
    Returns true if all of the cols are == 0

    :param row: Contains all data. - Row.
    :param questions_list: List of question columns

    :return:  Bool. False if any question col had a value
    """
    total_data = 0
    for question in questions_list:
        total_data += row[question]
    if total_data == 0:
        return True
    else:
        return False
