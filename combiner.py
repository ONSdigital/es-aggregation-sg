import json
import logging
import os

import boto3
import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    run_environment = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    additional_aggregated_column = fields.Str(required=True)
    aggregated_column = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    aggregation_files = fields.List(fields.Str(required=True))
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method takes the new columns and adds them all onto the main dataset.

    :param event: { "RuntimeVariables": {
        aggregated_column - A column to aggregate by. e.g. Enterprise_Reference.
        additional_aggregated_column - A column to aggregate by. e.g. Region.
    }}
    :param context:
    :return:
    """
    logger = logging.getLogger("Combininator")
    logger.setLevel(logging.INFO)
    current_module = "Aggregation_Combiner"
    error_message = ""
    checkpoint = 4
    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Starting Aggregation Combiner.")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        checkpoint = environment_variables["checkpoint"]
        bucket_name = environment_variables["bucket_name"]
        run_environment = environment_variables["run_environment"]

        # Runtime Variables
        additional_aggregated_column = runtime_variables["additional_aggregated_column"]
        aggregated_column = runtime_variables["aggregated_column"]
        in_file_name = runtime_variables["in_file_name"]
        aggregation_files = runtime_variables["aggregation_files"]
        out_file_name = runtime_variables["out_file_name"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]

        logger.info("Retrieved configuration variables.")

        # Get file from s3
        imp_df = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

        logger.info("Successfully retrieved data from s3")
        data = []

        # Receive the 3 aggregation outputs.
        for aggregation_file in aggregation_files:
            data.append(aggregation_file)

        if len(data) != 3:
            raise exception_classes.LambdaFailure("Wrong amount of file names provided")
        else:
            # Convert the 3 outputs into dataframes.
            first_agg = json.loads(data[0])
            second_agg = json.loads(data[1])
            third_agg = json.loads(data[2])

        # Load file content.
        first_agg_df = aws_functions.read_dataframe_from_s3(first_agg["bucket"],
                                                            first_agg["key"])
        second_agg_df = aws_functions.read_dataframe_from_s3(second_agg["bucket"],
                                                             second_agg["key"])
        third_agg_df = aws_functions.read_dataframe_from_s3(third_agg["bucket"],
                                                            third_agg["key"])
        logger.info("Successfully retrievied the aggragation files for combination")

        to_aggregate = [aggregated_column]
        if additional_aggregated_column != "":
            to_aggregate.append(additional_aggregated_column)

        # merge the imputation output from s3 with the 3 aggregation outputs
        first_merge = pd.merge(
            imp_df, first_agg_df, on=to_aggregate, how="left")

        second_merge = pd.merge(
            first_merge, second_agg_df, on=to_aggregate, how="left")

        third_merge = pd.merge(
            second_merge, third_agg_df, on=to_aggregate, how="left")

        logger.info("Successfully merged dataframes")

        # convert output to json ready to return
        final_output = third_merge.to_json(orient="records")

        # send output onwards
        aws_functions.save_to_s3(bucket_name, out_file_name, final_output)
        logger.info("Successfully sent data to s3.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(first_agg["bucket"], first_agg["key"]))
            logger.info(aws_functions.delete_data(second_agg["bucket"], second_agg["key"])) # noqa
            logger.info(aws_functions.delete_data(third_agg["bucket"], third_agg["key"]))
            logger.info("Successfully deleted input data.")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn,
                                       "Aggregation - Combiner.")
        logger.info("Successfully sent data to sns.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    return {"success": True, "checkpoint": checkpoint}
