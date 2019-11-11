import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Class to set up the environment variables schema.
    """

    arn = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    s3_file = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    file_name = fields.Str(required=True)


class NoDataInQueueError(Exception):
    pass


class DoNotHaveAllDataError(Exception):
    pass


def lambda_handler(event, context):
    logger = logging.getLogger("Combininator")
    logger.setLevel(logging.INFO)
    current_module = "Aggregation_Combiner"
    error_message = ""
    log_message = ""
    checkpoint = 0
    try:
        placeholder = context.aws_request_id
        context={}
        context['aws_request_id'] = placeholder

        logger.info("Combiner Begun")

        # Set up Environment variables Schema.
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        logger.info("Validated params")
        # Enviroment variables
        queue_url = config["queue_url"]
        bucket_name = config["bucket_name"]
        s3_file = config["file_name"]
        sqs_messageid_name = config["sqs_messageid_name"]
        checkpoint = config["checkpoint"]
        arn = config["arn"]
        file_name = config['file_name']
        # Clients

        sqs = boto3.client("sqs", "eu-west-2")

        # Get file from s3
        imp_df = funk.read_dataframe_from_s3(bucket_name, s3_file)

        logger.info("Successfully retrieved data from s3")
        data = []

        # Receive the 3 aggregation outputs
        response = funk.get_sqs_message(queue_url, 3)
        if "Messages" not in response:
            raise NoDataInQueueError("No Messages in queue")
        if len(response["Messages"]) < 3:
            raise DoNotHaveAllDataError(
                "Only " + str(len(response["Messages"])) + " recieved"
            )
        logger.info("Successfully retrieved data from sqs")
        for message in response["Messages"]:
            data.append(message["Body"])

        sqs.purge_queue(QueueUrl=queue_url)
        logger.info("Successfully deleted input data from sqs")
        # convert the 3 outputs into dataframes
        first_agg = json.loads(data[0])
        second_agg = json.loads(data[1])
        third_agg = json.loads(data[2])

        first_agg_df = pd.DataFrame(first_agg)
        second_agg_df = pd.DataFrame(second_agg)
        third_agg_df = pd.DataFrame(third_agg)

        # merge the imputation output from s3 with the 3 aggregation outputs
        first_merge = pd.merge(
            imp_df, first_agg_df, on=["region", "county", "period"], how="left"
        )
        second_merge = pd.merge(
            first_merge, second_agg_df, on=["region", "county", "period"], how="left"
        )
        third_merge = pd.merge(
            second_merge, third_agg_df, on=["region", "county", "period"], how="left"
        )
        logger.info("Successfully merged dataframes")
        # convert output to json ready to return
        final_output = third_merge.to_json(orient="records")

        # send output onwards
        funk.save_data(bucket_name, file_name,
                       final_output, queue_url, sqs_messageid_name)
        logger.info("Successfully sent data to sqs")

        funk.send_sns_message(checkpoint, arn, "Combiner completed succesfully")
        logger.info("Successfully sent data to sns")

    except NoDataInQueueError as e:
        error_message = (
            "There was no data in sqs queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except DoNotHaveAllDataError as e:
        error_message = (
            "Did not recieve all 3 messages from queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except AttributeError as e:
        error_message = (
            "Bad data encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
            "Parameter validation error"
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
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
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
            "Key Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
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
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            print(type(logger))
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
