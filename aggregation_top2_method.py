import json
import logging

import marshmallow
import numpy as np
import pandas as pd


class EnvironSchema(marshmallow.Schema):
    """
    Class to set up the environment variables schema.
    """
    input_json = marshmallow.fields.Str(required=True)
    total_column = marshmallow.fields.Str(required=True)
    period_column = marshmallow.fields.Str(required=True)
    additional_aggregated_column = marshmallow.fields.Str(required=True)
    aggregated_column = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method requires a dataframe which must contain the input columns:
     - 'period' by default, can be changed in environment variables.
     - 'county' by default, can be changed in environment variables.
     - 'region' by default, can be changed in environment variables.
     - 'Q608_total' by default, can be changed in environment variables.
    ... and the two output columns...
     - largest_contributor, created inside top2 wrangler.
     - second_largest contributor, created inside top2 wrangler.

    It loops through each county (by period) and records largest & second
    largest value against each record in the group.

    :param event: N/A
    :param context: N/A
    :return:    Success - response_json (json serialised pandas dataframe)
                Failure - success (string, bool), error (string)
    """
    current_module = "Aggregation Calc Top Two - Method"
    logger = logging.getLogger()
    error_message = ''
    log_message = ''
    response_json = None

    logger.info("Starting " + current_module)

    try:
        # Set up Environment variables Schema.
        schema = EnvironSchema(strict=False)
        config, errors = schema.load(event)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        logger.info("Converting input json to dataframe")
        input_json = json.loads(config["input_json"])
        total_column = config["total_column"]
        period_column = config["period_column"]
        additional_aggregated_column = config["additional_aggregated_column"]
        aggregated_column = config["aggregated_column"]

        input_dataframe = pd.DataFrame(input_json)

        logger.info("Invoking calc_top_two function on input dataframe")

        response = calc_top_two(input_dataframe, total_column,
                                period_column, aggregated_column)

        response = response[[additional_aggregated_column,
                             aggregated_column,
                             period_column,
                             "largest_contributor",
                             "second_largest_contributor"]]

        logger.info("Converting output dataframe to json")
        response_json = response.to_json(orient='records')
        final_output = {"data": response_json}

    except Exception as e:
        # Catch anything unforseen that wrangler has missed.
        error_message = ("There was an error processing the method itself: "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = (error_message + " | Line: "
                       + str(e.__traceback__.tb_lineno))

    finally:
        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output['success'] = True
    return final_output


def calc_top_two(data, total_column, period_column, aggregated_column,):
    # NB: No need for try/except as called from inside try clause in lambda_handler.

    logger = logging.getLogger()
    logger.info("Executing function: calc_top_two")

    # Ensure additional columns are zeroed (Belt n Braces)
    data['largest_contributor'] = 0
    data['second_largest_contributor'] = 0
    secondary_value = 0

    # Create unique list of periods in data
    period_list = list(data.period.unique())

    # Get unique periods
    for period in period_list:
        column_list = []
        temp_column_list = data.loc[
            (data[period_column] == period)][aggregated_column].tolist()

        logger.info("Processing period " + str(period))

        # Make column unique
        for temp_column in temp_column_list:
            if temp_column not in column_list:
                column_list.append(temp_column)

            # Loop through each column (by period) updating largest & second largest value
            for column in column_list:
                logger.info("...Processing column " + str(column))

                tot = data.loc[(data[period_column] == period)][[total_column,
                                                                 aggregated_column]]

                tot2 = tot.loc[(tot[aggregated_column] == column)]

                sorted_dataframe = tot2.sort_values(by=[total_column], ascending=False)

                sorted_dataframe = sorted_dataframe.reset_index(drop=True)

                top_two = sorted_dataframe.head(2)

                primary_value = top_two[total_column].iloc[0]

                if top_two.shape[0] >= 2:
                    secondary_value = top_two[total_column].iloc[1]

                data.loc[(data[aggregated_column] == column) &
                         (data[period_column] == period),
                         'largest_contributor'] = primary_value

                data.loc[(data[aggregated_column] == column) &
                         (data[period_column] == period),
                         'second_largest_contributor'] = secondary_value

    # Ensure additional columns are type cast correctly (Belt n Braces)
    data['largest_contributor'] = (data['largest_contributor'].astype(np.int64))

    data['second_largest_contributor'] = (data['second_largest_contributor']
                                          .astype(np.int64))

    logger.info("Returning the output data")
    logger.info("Successfully completed function: calc_top_two")

    return data
