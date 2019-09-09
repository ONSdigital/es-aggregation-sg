import os
import json
import random

import boto3
import pandas as pd
import logging

from marshmallow import Schema, fields
from botocore.exceptions import IncompleteReadError, ClientError


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    s3_file = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    sqs_message_id_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    arn = fields.Str(required=True)
    method_name = fields.Str(required=True)
    time = fields.Str(required=True)
    response_type = fields.Str(required=True)
    questions_list = fields.Str(required=True)
    output_file = fields.Str(required=True)
    reference = fields.Str(required=True)
    segmentation = fields.Str(required=True)
    stored_segmentation = fields.Str(required=True)
    current_time = fields.Str(required=True)
    previous_time = fields.Str(required=True)
    current_segmentation = fields.Str(required=True)
    previous_segmentation = fields.Str(required=True)


class NoDataInQueueError(Exception):
    pass


# Set up clients
s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name='eu-west-2')
sns = boto3.client('sns', region_name='eu-west-2')


def lambda_handler(event, context):
    """
        This wrangler is used to prepare data for the calculate top two statistical method.
        The method requires a dataframe which must contain the input columns...
         - period
         - county
         - Q608_total
        ... and the two output columns...
         - largest_contributor
         - second_largest contributor

        The wrangler:
          - converts the data from json to dataframe,
          - ensures the mandatory columns are present
          - appends the output columns
          - sends the dataframe to the function
          - ensures the new columns are present in the returned dataframe
          - sends the data on ??????

        :param event: N/A
        :param context: N/A
        :return: Success - Dataframe, checkpoint
    """
    current_module = "Aggregation Calc Top Two - Wrangler"
    logger = logging.getLogger()
    error_message = ''
    log_message = ''
    checkpoint = 0

    logger.info("Starting " + current_module)

    try:
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
        sqs_message_id_name = config['sqs_messageid_name']
        checkpoint = config['checkpoint']
        arn = config['arn']
        period = event['RuntimeVariables']['period']
        method_name = config['method_name']
        time = config['time']  # Set as "period"
        response_type = config['response_type']  # Set as "response_type"
        questions_list = config['questions_list']
        output_file = config['output_file']

        # Read from S3 bucket
        data_json = read_data_from_s3(bucket_name, s3_file)
        logger.info("Completed reading data from s3")

        # Convert to dataframe
        data = pd.DataFrame(data_json)

        # Ensure mandatory columns are present
        req_col_list = ['period', 'county', 'Q608_total']
        for req_col in req_col_list:
            if req_col not in data.columns:
                err_msg = 'Required column ' + req_col + ' not found in dataframe.'
                raise IndexError(err_msg)

        # Add output columns
        data['largest_contributor'] = 0
        data['second_largest contributor'] = 0

        # Send wrangled data on to method.
        prepared_data = data.to_json(orient='records')

        # Invoke aggregation top2 method
        top2 = lambda_client.invoke(FunctionName=method_name, Payload=prepared_data)
        json_response = top2.get('Payload').read().decode("utf-8")

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.loads(json_response), MessageGroupId=sqs_message_id_name,
                         MessageDeduplicationId=str(random.getrandbits(128)))

        send_sns_message(checkpoint, arn)

    except NoDataInQueueError as e:
        error_message = "There was no data in sqs queue in:  " \
                        + current_module + " |-  | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except AttributeError as e:
        error_message = "Bad data encountered in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IndexError as e:
        error_message = "Required columns missing from input data in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = "Parameter validation error" \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = "AWS Error (" \
                        + str(e.response['Error']['Code']) + ") " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = "Key Error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = "Incomplete Lambda response encountered in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = "General Error in " \
                        + current_module + " (" \
                        + str(type(e)) + ") |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}


def send_sns_message(checkpoint, arn):
    sns_message = {
        "success": True,
        "module": "Calculate Top Two Wrangler",
        "checkpoint": checkpoint,
        "message": "Completed Top Two"
    }

    return sns.publish(TargetArn=arn, Message=json.dumps(sns_message))


def read_data_from_s3(bucket_name, s3_file):
    """
    This method is used to retrieve data from an s3 bucket.
    :param bucket_name: The name of the bucket you are accessing.
    :param s3_file: The file you wish to import.
    :return: data_json - Type: JSON.
    """
    data_object = s3.Object(bucket_name, s3_file)
    data_content = data_object.get()['Body'].read()
    data_json = json.loads(data_content)

    return data_json
