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
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)


class DoNotHaveAllDataError(Exception):
    pass


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

    try:
        logger.info("Starting Aggregation Combiner.")

        # Set up Environment variables Schema.
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        logger.info("Validated params")

        # Enviroment variables
        checkpoint = config["checkpoint"]
        bucket_name = config["bucket_name"]
        in_file_name = config["in_file_name"]
        out_file_name = config['out_file_name']
        sns_topic_arn = config["sns_topic_arn"]
        sqs_message_group_id = config["sqs_message_group_id"]
        sqs_queue_url = config["sqs_queue_url"]

        aggregated_column = event['RuntimeVariables']['aggregated_column']
        additional_aggregated_column =\
            event['RuntimeVariables']['additional_aggregated_column']

        # Clients
        sqs = boto3.client("sqs", "eu-west-2")

        # Get file from s3
        imp_df = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

        logger.info("Successfully retrieved data from s3")
        data = []

        # Receive the 3 aggregation outputs
        response = aws_functions.get_sqs_message(sqs_queue_url, 3)
        if "Messages" not in response:
            raise exception_classes.NoDataInQueueError("No Messages in queue")
        if len(response["Messages"]) < 3:
            raise DoNotHaveAllDataError(
                "Only " + str(len(response["Messages"])) + " recieved"
            )
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
                                                            first_agg['key'])
        second_agg_df = aws_functions.read_dataframe_from_s3(second_agg['bucket'],
                                                             second_agg['key'])
        third_agg_df = aws_functions.read_dataframe_from_s3(third_agg['bucket'],
                                                            third_agg['key'])

        # merge the imputation output from s3 with the 3 aggregation outputs
        first_merge = pd.merge(
            imp_df, first_agg_df, on=[additional_aggregated_column,
                                      aggregated_column], how="left")

        second_merge = pd.merge(
            first_merge, second_agg_df, on=[additional_aggregated_column,
                                            aggregated_column], how="left")

        third_merge = pd.merge(
            second_merge, third_agg_df, on=[additional_aggregated_column,
                                            aggregated_column], how="left")

        logger.info("Successfully merged dataframes")

        # !temporary due to the size of our test data.
        # This means that cells that didn't have any responders
        # to produce aggregations from, then the aggregations are not null
        # (breaking things)
        third_merge.fillna(1, inplace=True, axis=1)
        # convert output to json ready to return
        final_output = third_merge.to_json(orient="records")

        # send output onwards
        aws_functions.save_data(bucket_name, out_file_name, final_output,
                                sqs_queue_url, sqs_message_group_id)
        logger.info("Successfully sent message to sqs")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn,
                                       "Aggregation - Combiner.")
        logger.info("Successfully sent data to sns")

    except exception_classes.NoDataInQueueError as e:
        error_message = (
            "There was no data in sqs queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except DoNotHaveAllDataError as e:
        error_message = (
            "Did not recieve all 3 messages from queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context.aws_request_id)
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
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
