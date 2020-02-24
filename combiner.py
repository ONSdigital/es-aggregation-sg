import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Class to set up the environment variables schema.
    """

    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    run_environment = fields.Str(required=True)
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
    log_message = ""
    checkpoint = 4
    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Starting Aggregation Combiner.")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']
        # Set up Environment variables Schema.
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        logger.info("Validated params")

        # Environment Variables
        checkpoint = config["checkpoint"]
        bucket_name = config["bucket_name"]
        run_environment = config['run_environment']
        sns_topic_arn = config["sns_topic_arn"]

        # Runtime Variables
        additional_aggregated_column =\
            event['RuntimeVariables']['additional_aggregated_column']
        aggregated_column = event['RuntimeVariables']['aggregated_column']
        in_file_name = event['RuntimeVariables']['in_file_name']
        location = event['RuntimeVariables']['location']
        out_file_name = event['RuntimeVariables']['out_file_name']
        outgoing_message_group_id = event['RuntimeVariables']["outgoing_message_group_id"]
        sqs_queue_url = event['RuntimeVariables']["queue_url"]

        logger.info("Retrieved configuration variables.")

        # Clients
        sqs = boto3.client("sqs", "eu-west-2")

        # Get file from s3
        imp_df = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name, location)

        logger.info("Successfully retrieved data from s3")
        data = []

        # Receive the 3 aggregation outputs
        response = aws_functions.get_sqs_messages(sqs_queue_url, 3, 'aggregation')

        receipt_handles = []
        logger.info("Successfully retrieved message from sqs")
        for message in response["Messages"]:
            receipt_handles.append(message['ReceiptHandle'])
            data.append(message["Body"])

        for handle in receipt_handles:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=handle)

        logger.info("Successfully deleted message from sqs")
        # convert the 3 outputs into dataframes
        first_agg = json.loads(data[0])
        second_agg = json.loads(data[1])
        third_agg = json.loads(data[2])

        first_agg_df = aws_functions.read_dataframe_from_s3(first_agg['bucket'],
                                                            first_agg['key'],
                                                            location)
        second_agg_df = aws_functions.read_dataframe_from_s3(second_agg['bucket'],
                                                             second_agg['key'],
                                                             location)
        third_agg_df = aws_functions.read_dataframe_from_s3(third_agg['bucket'],
                                                            third_agg['key'],
                                                            location)

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
        aws_functions.save_data(bucket_name, out_file_name, final_output,
                                sqs_queue_url, outgoing_message_group_id,
                                location)
        logger.info("Successfully sent data to s3.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(first_agg['bucket'], first_agg['key'],
                                                  location))
            logger.info(aws_functions.delete_data(second_agg['bucket'], second_agg['key'],
                                                  location))
            logger.info(aws_functions.delete_data(third_agg['bucket'], third_agg['key'],
                                                  location))
            logger.info("Successfully deleted input data.")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn,
                                       "Aggregation - Combiner.")
        logger.info("Successfully sent data to sns.")

    except exception_classes.NoDataInQueueError as e:
        error_message = (
            "There was no data in sqs queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context.aws_request_id)
            + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except exception_classes.DoNotHaveAllDataError as e:
        error_message = (
            "Did not recieve all 3 messages from queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context.aws_request_id)
            + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
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
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
