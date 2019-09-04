import json
import traceback
import boto3
import pandas as pd
import os
import random

# Set up clients
sqs = boto3.client('sqs')
lambda_client = boto3.client('lambda')

# SQS
queue_url = os.environ['queue_url']

# Env vars
error_handler_arn = os.environ['error_handler_arn']


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """

    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def lambda_handler(event, context):
    try:
        input_json = event

        input_dataframe = pd.DataFrame(input_json)

        region_agg = input_dataframe.groupby(['county', 'region', 'period'])
        agg_by_region_output = region_agg.agg({'enterprise_ref': 'nunique'}).reset_index()
        agg_by_region_output.rename(columns={'enterprise_ref': 'ent_ref_count'},
                                    inplace=True)

        output_json = agg_by_region_output.to_json(orient='records')

    except Exception as exc:
        # Invoke error handler lambda
        # lambda_client.invoke(
        #     FunctionName=error_handler_arn,
        #     InvocationType='Event',
        #     Payload=json.loads(_get_traceback(exc))
        # )

        return {
            "success": False,
            "error": "Unexpected Wrangler exception {}".format(_get_traceback(exc))
        }

    return output_json