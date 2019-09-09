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


def calc_top_two(data):
    data['largest_contributor'] = 0
    data['second_largest contributor'] = 0

    secondary_value = 0

    period_list = list(data.period.unique())

    # Get unique periods
    for period in period_list:
        county_list = []
        temp_county_list = data.loc[(data['period'] == period)]['county'].tolist()

        # Make County unique
        for temp_county in temp_county_list:
            if temp_county not in county_list:
                county_list.append(temp_county)

            # Loop through each county (by period) and update largest & second largest value
            for county in county_list:

                tot = data.loc[(data['period'] == period)][['Q608_total', 'county']]

                tot2 = tot.loc[(tot['county'] == county)]

                sorted_dataframe = tot2.sort_values(by=['Q608_total'], ascending=False)

                sorted_dataframe = sorted_dataframe.reset_index(drop=True)

                top_two = sorted_dataframe.head(2)

                primary_value = top_two['Q608_total'].iloc[0]
                if top_two.shape[0] >= 2:
                    secondary_value = top_two['Q608_total'].iloc[1]

                data.loc[(data['county'] == county) & (data['period'] == period),
                         'largest_contributor'] = primary_value
                data.loc[(data['county'] == county) & (data['period'] == period),
                         'second_largest_contributor'] = secondary_value
    return data[['county', 'region', 'period', 'largest_contributor', 'second_largest_contributor']]


def lambda_handler(event, context):
    try:
        input_json = event

        input_dataframe = pd.DataFrame(input_json)

        response = calc_top_two(input_dataframe)

        response_json = response.to_json(orient='records')

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

    return response_json
