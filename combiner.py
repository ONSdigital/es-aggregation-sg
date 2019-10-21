import json
import logging
import os
import random

import boto3
import marshmallow
import pandas as pd
from botocore.exceptions import ClientError


class EnvironSchema(marshmallow.Schema):
    """
    Class to set up the environment variables schema.
    """

    arn = marshmallow.fields.Str(required=True)
    queue_url = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    file_name = marshmallow.fields.Str(required=True)
    bucket_name = marshmallow.fields.Str(required=True)
    sqs_messageid_name = marshmallow.fields.Str(required=True)


class NoDataInQueueError(Exception):
    pass


class DoNotHaveAllDataError(Exception):
    pass


def get_sqs_message(sqs, queue_url, num_messages):
    """
    Retrieves messages from the SQS queue.
    :param sqs: SQS object
    :param queue_url: The url of the SQS queue. - Type: String.
    :param num_messages: The number of messages to receive(usually 3)
                        - Type: Int.
    :return: Message from queue - Type: String.
    """
    return sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=num_messages)


def get_from_s3(bucket_name, file_name):
    """
    This method is used to retrieve data from an s3 bucket.
    :param bucket_name: The name of the bucket you are accessing.
    :param file_name: The file you wish to import.
    :return: imp_file: - JSON.
    """
    s3 = boto3.resource("s3", "eu-west-2")
    object = s3.Object(bucket_name, file_name)
    imp_file = object.get()["Body"].read()
    return imp_file


def lambda_handler(event, context):
    logger = logging.getLogger("Combininator")
    logger.setLevel(logging.INFO)
    current_module = "Aggregation_Combiner"
    error_message = ""
    log_message = ""
    try:
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
        file_name = config["file_name"]
        sqs_messageid_name = config["sqs_messageid_name"]
        checkpoint = config["checkpoint"]
        arn = config["arn"]

        # Clients

        sqs = boto3.client("sqs", "eu-west-2")

        # Get file from s3
        imp_file = get_from_s3(bucket_name, file_name)
        imp_df = pd.DataFrame(json.loads(imp_file))
        logger.info("Successfully retrieved data from s3")
        data = []

        # Receive the 3 aggregation outputs
        response = get_sqs_message(sqs, queue_url, 3)
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
        send_sqs_message(sqs, queue_url, final_output, sqs_messageid_name)
        logger.info("Successfully sent data to sqs")

        send_sns_message(arn, checkpoint)
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


def send_sns_message(arn, checkpoint):
    """
    This function is responsible for sending notifications to the SNS Topic.
    Notifications will be used to relay information to the BPM.

    :param arn: The Address of the SNS topic - Type: String.
    :param checkpoint: Location of process - Type: String.
    :return: None.
    """
    sns = boto3.client("sns", "eu-west-2")

    sns_message = {
        "success": True,
        "module": "Aggregation Combiner",
        "checkpoint": checkpoint,
        "anomalies": "",
        "message": "Completed Aggregation Combiner",
    }

    return sns.publish(TargetArn=arn, Message=json.dumps(sns_message))


def send_sqs_message(sqs, queue_url, message, output_message_id):
    """
    This method is responsible for sending data to the SQS queue.
    :param sqs: SQS client for use in interacting with sqs  - Boto3 client(SQS)
    :param queue_url: The url of the SQS queue. - String.
    :param message: The message/data you wish to send to the SQS queue - String.
    :param output_message_id: The label of the record in the SQS queue - String.
    :return: None
    """

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
        MessageGroupId=output_message_id,
        MessageDeduplicationId=str(random.getrandbits(128)),
    )
