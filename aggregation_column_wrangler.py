import json
import logging
import os

import boto3
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    additional_aggregated_column = fields.Str(required=True)
    aggregated_column = fields.Str(required=True)
    aggregation_type = fields.Str(required=True)
    cell_total_column = fields.Str(required=True)
    environment = fields.Str(Required=True)
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    survey = fields.Str(required=True)
    total_columns = fields.List(fields.String, required=True)


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
    :return: {"success": True}
            or LambdaFailure exception
    """
    current_module = "Aggregation by column - Wrangler"
    error_message = ""

    # Define run_id outside of try block
    run_id = 0
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Needs to be declared inside the lambda_handler
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]

        # Runtime Variables
        additional_aggregated_column = runtime_variables["additional_aggregated_column"]
        aggregated_column = runtime_variables["aggregated_column"]
        aggregation_type = runtime_variables["aggregation_type"]
        cell_total_column = runtime_variables["cell_total_column"]
        environment = runtime_variables["environment"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        survey = runtime_variables["survey"]
        total_columns = runtime_variables["total_columns"]

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
        # Read from S3 bucket
        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)
        logger.info("Started - retrieved data from s3")

        formatted_data = data.to_json(orient="records")
        logger.info("Formatted disaggregated_data")

        json_payload = {
            "RuntimeVariables": {
                "additional_aggregated_column": additional_aggregated_column,
                "aggregated_column": aggregated_column,
                "aggregation_type": aggregation_type,
                "cell_total_column": cell_total_column,
                "data": formatted_data,
                "environment": environment,
                "run_id": run_id,
                "survey": survey,
                "total_columns": total_columns
            }
        }

        by_column = lambda_client.invoke(FunctionName=method_name,
                                         Payload=json.dumps(json_payload))

        json_response = json.loads(by_column.get("Payload").read().decode("utf-8"))

        logger.info("Successfully invoked the method lambda")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        aws_functions.save_to_s3(bucket_name, out_file_name, json_response["data"])
        logger.info("Successfully sent the data to S3")

        aws_functions.send_sns_message(sns_topic_arn,
                                       "Aggregation - " + aggregated_column + ".")

        logger.info("Successfully sent the SNS message")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    return {"success": True}
