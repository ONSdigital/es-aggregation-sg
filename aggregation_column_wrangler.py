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
    checkpoint = fields.Str(required=True)
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
    in_file_name = fields.Str(required=True)
    location = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    outgoing_message_group_id = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    queue_url = fields.Str(required=True)
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
    :return: {"success": True, "checkpoint":4}
            or LambdaFailure exception
    """
    current_module = "Aggregation by column - Wrangler"
    error_message = ""
    checkpoint = 4
    logger = logging.getLogger("Aggregation")
    logger.setLevel(0)

    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Started Aggregation - Wrangler.")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Needs to be declared inside the lambda_handler
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        checkpoint = environment_variables["checkpoint"]
        method_name = environment_variables["method_name"]

        # Runtime Variables
        additional_aggregated_column = runtime_variables["additional_aggregated_column"]
        aggregated_column = runtime_variables["aggregated_column"]
        aggregation_type = runtime_variables["aggregation_type"]
        cell_total_column = runtime_variables["cell_total_column"]
        in_file_name = runtime_variables["in_file_name"]
        location = runtime_variables["location"]
        out_file_name = runtime_variables["out_file_name"]
        outgoing_message_group_id = runtime_variables["outgoing_message_group_id"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        sqs_queue_url = runtime_variables["queue_url"]
        total_columns = runtime_variables["total_columns"]

        logger.info("Retrieved configuration variables.")

        # Read from S3 bucket
        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name, location)
        logger.info("Completed reading data from s3")

        formatted_data = data.to_json(orient="records")
        logger.info("Formated disaggregated_data")

        json_payload = {
            "RuntimeVariables": {
                "data": formatted_data,
                "total_columns": total_columns,
                "additional_aggregated_column": additional_aggregated_column,
                "aggregated_column": aggregated_column,
                "cell_total_column": cell_total_column,
                "aggregation_type": aggregation_type,
                "run_id": run_id
            }
        }

        by_column = lambda_client.invoke(FunctionName=method_name,
                                         Payload=json.dumps(json_payload))

        json_response = json.loads(by_column.get("Payload").read().decode("utf-8"))

        logger.info("Successfully invoked the method lambda")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        aws_functions.save_data(bucket_name, out_file_name, json_response["data"],
                                sqs_queue_url, outgoing_message_group_id, location)
        logger.info("Successfully sent the data to SQS")

        aws_functions.send_sns_message(checkpoint,
                                       sns_topic_arn,
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

    return {"success": True, "checkpoint": checkpoint}
