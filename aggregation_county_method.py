import traceback
import boto3
import pandas as pd
import os

# Set up clients
sqs = boto3.client('sqs')

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

        county_agg = input_dataframe.groupby(['region', 'county', 'period'])
        agg_by_county_output = county_agg.agg(
            {'Q608_total': 'sum'}
        ).reset_index()
        agg_by_county_output.rename(
            columns={'Q608_total': 'county_total'},
            inplace=True
        )

        output_json = agg_by_county_output.to_json(orient='records')

    except Exception as exc:

        return {
            "success": False,
            "error": "Unexpected Wrangler exception {}"
            .format(_get_traceback(exc))
        }

    return output_json
