import logging
import os

import json
import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the
    correct format.
    :return: None
    """
    s3_file = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    arn = fields.Str(required=True)
    method_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    file_name = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the calculate top two
    statistical method.
    The method requires a dataframe which must contain the input columns:
     - period
     - county
     - Q608_total
    ... and the two output columns...
     - largest_contributor
     - second_largest contributor

    The wrangler:
      - converts the data from json to dataframe,
      - ensures the mandatory columns are present and correctly typed
      - appends the output columns
      - sends the dataframe to the function
      - ensures the new columns are present in the returned dataframe
      - sends the data on via SQS
      - Notifies via SNS

    :param event: N/A
    :param context: N/A
    :return: Success - dataframe, checkpoint
    """
    current_module = "Aggregation Calc Top Two - Wrangler"
    logger = logging.getLogger()
    error_message = ''
    log_message = ''
    checkpoint = 0

    try:
        placeholder = context.aws_request_id
        context={}
        context['aws_request_id'] = placeholder

        logger.info("Starting " + current_module)

        # Import environment variables using marshmallow validation
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # Needs to be declared inside of the lambda handler
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        logger.info("Setting-up environment configs")

        s3_file = config['s3_file']
        bucket_name = config['bucket_name']
        queue_url = config['queue_url']
        sqs_messageid_name = config['sqs_messageid_name']
        checkpoint = config['checkpoint']
        arn = config['arn']
        method_name = config['method_name']
        file_name = config['file_name']
        # Read from S3 bucket
        data = funk.read_dataframe_from_s3(bucket_name, s3_file)
        logger.info("Completed reading data from s3")

        # Ensure mandatory columns are present and have the correct
        # type of content
        msg = "Checking required data columns are present and correctly typed."
        logger.info(msg)
        req_col_list = ['period', 'county', 'Q608_total']
        for req_col in req_col_list:
            if req_col not in data.columns:
                err_msg = 'Required column "' + req_col + '" not found in dataframe.'
                raise IndexError(err_msg)
            row_index = 0
            for row in data.to_records():
                if not isinstance(row[req_col], np.int64):
                    err_msg = 'Required column "' + req_col
                    err_msg += '" has wrong data type (' + str(type(row[req_col]))
                    err_msg += ' at row index ' + str(row_index) + '.'
                    raise TypeError(err_msg)
                row_index += 1

        # Add output columns
        logger.info("Appending two further required columns.")
        data['largest_contributor'] = 0
        data['second_largest contributor'] = 0

        # Serialise data
        logger.info("Converting dataframe to json.")
        prepared_data = data.to_json(orient='records')

        # Invoke aggregation top2 method
        logger.info("Invoking the statistical method.")
        top2 = lambda_client.invoke(FunctionName=method_name, Payload=prepared_data)
        json_response = json.loads(top2.get('Payload').read().decode("utf-8"))

        # Ensure appended columns are present in output and have the
        # correct type of content
        msg = "Checking required output columns are present and correctly typed."
        logger.info(msg)
        ret_data = pd.read_json(json_response, orient='records')
        req_col_list = ['largest_contributor', 'second_largest_contributor']
        for req_col in req_col_list:
            if req_col not in ret_data.columns:
                err_msg = 'Required column "' + req_col + '" not found in output data.'
                raise IndexError(err_msg)
            row_index = 0
            for row in ret_data.to_records():
                if not isinstance(row[req_col], np.int64):
                    err_msg = 'Output column "' + req_col
                    err_msg += '" has wrong data type (' + str(type(row[req_col]))
                    err_msg += ' at row index ' + str(row_index) + '.'
                    raise TypeError(err_msg)
                row_index += 1

        # Sending output to SQS, notice to SNS
        logger.info("Sending function response downstream.")
        funk.save_data(bucket_name, file_name,
                       json_response, queue_url, sqs_messageid_name)
        logger.info("Successfully sent the data to SQS")
        funk.send_sns_message(checkpoint, arn, "Top 2 completed successfully")
        logger.info("Successfully sent the SNS message")

    except IndexError as e:
        error_message = ("Required columns missing from input data in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except TypeError as e:
        error_message = ("Bad data encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = ("Parameter validation error"
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = ("AWS Error ("
                         + str(e.response['Error']['Code']) + ") "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = ("Incomplete Lambda response encountered in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message
        log_message += " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
