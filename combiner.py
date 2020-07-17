import logging
import os

import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

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
    aggregation_files = fields.Dict(required=True)
    in_file_name = fields.Str(required=True)
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
    logger = logging.getLogger("Combiner")
    logger.setLevel(10)
    current_module = "Aggregation_Combiner"
    error_message = ""

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
        bucket_name = environment_variables["bucket_name"]
        run_environment = environment_variables["run_environment"]

        # Runtime Variables
        additional_aggregated_column = runtime_variables["additional_aggregated_column"]
        aggregated_column = runtime_variables["aggregated_column"]
        aggregation_files = runtime_variables["aggregation_files"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]

        logger.info("Retrieved configuration variables.")

        # Get file from s3
        imp_df = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

        logger.info("Successfully retrieved imputation data from s3")

        # Receive the 3 aggregation outputs.
        ent_ref_agg = aggregation_files["ent_ref_agg"]
        cell_agg = aggregation_files["cell_agg"]
        top2_agg = aggregation_files["top2_agg"]

        # Load file content.
        ent_ref_agg_df = aws_functions.read_dataframe_from_s3(bucket_name, ent_ref_agg)
        cell_agg_df = aws_functions.read_dataframe_from_s3(bucket_name, cell_agg)
        top2_agg_df = aws_functions.read_dataframe_from_s3(bucket_name, top2_agg)
        logger.info("Successfully retrievied aggragation data from s3")

        to_aggregate = [aggregated_column]
        if additional_aggregated_column != "":
            to_aggregate.append(additional_aggregated_column)

        # merge the imputation output from s3 with the 3 aggregation outputs
        first_merge = pd.merge(
            imp_df, ent_ref_agg_df, on=to_aggregate, how="left")

        second_merge = pd.merge(
            first_merge, cell_agg_df, on=to_aggregate, how="left")

        third_merge = pd.merge(
            second_merge, top2_agg_df, on=to_aggregate, how="left")

        logger.info("Successfully merged dataframes")

        # convert output to json ready to return
        final_output = third_merge.to_json(orient="records")

        # send output onwards
        aws_functions.save_to_s3(bucket_name, out_file_name, final_output)
        logger.info("Successfully sent data to s3.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(bucket_name, ent_ref_agg))
            logger.info(aws_functions.delete_data(bucket_name, cell_agg))
            logger.info(aws_functions.delete_data(bucket_name, top2_agg))
            logger.info("Successfully deleted input data.")

        aws_functions.send_sns_message(sns_topic_arn, "Aggregation - Combiner.")
        logger.info("Successfully sent data to sns.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    return {"success": True}
