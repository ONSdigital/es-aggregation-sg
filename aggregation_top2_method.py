import json
import logging

import marshmallow
import pandas as pd


class EnvironSchema(marshmallow.Schema):
    """
    Class to set up the environment variables schema.
    """
    input_json = marshmallow.fields.Str(required=True)
    total_columns = marshmallow.fields.List(marshmallow.fields.Str(), required=True)
    additional_aggregated_column = marshmallow.fields.Str(required=True)
    aggregated_column = marshmallow.fields.Str(required=True)
    top1_column = marshmallow.fields.Str(required=True)
    top2_column = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method loops through each county and records largest & second largest value
     against each record in the group.


    :param event: {
        input_json - JSON String of the data.
        aggregated_column - A column to aggregate by. e.g. Enterprise_Reference.
        additional_aggregated_column - A column to aggregate by. e.g. Region.
        total_columns - The names of the columns to produce aggregations for.
        top1_column - The prefix for the largest_contibutor column
        top2_column - The prefix for the second_largest_contibutor column
    }
    :param context: N/A
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Aggregation Calc Top Two - Method"
    logger = logging.getLogger()
    logger.setLevel(0)
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
        total_columns = config["total_columns"]
        additional_aggregated_column = config["additional_aggregated_column"]
        aggregated_column = config["aggregated_column"]
        top1_column = config['top1_column']
        top2_column = config['top2_column']

        input_dataframe = pd.DataFrame(input_json)
        top_two_output = pd.DataFrame()
        logger.info("Invoking calc_top_two function on input dataframe")
        counter = 0
        for total_column in total_columns:
            response = calc_top_two(input_dataframe, total_column,
                                    aggregated_column, additional_aggregated_column,
                                    top1_column, top2_column)

            response = response.drop_duplicates()
            if (counter == 0):
                top_two_output = response
            else:
                top_two_output = top_two_output.merge(response,
                                                      on=[additional_aggregated_column,
                                                          aggregated_column], how="left")
            counter += 1

        response = top_two_output
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
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output['success'] = True
    return final_output


def calc_top_two(data, total_column, aggregated_column, additional_aggregated_column,
                 top1_column, top2_column):
    '''

    :param data: Input Dataframe
    :param total_column - The name of the column to produce aggregation for.
    :param aggregated_column: A column to aggregate by. e.g. Enterprise_Reference.
    :param additional_aggregated_column: A column to aggregate by. e.g. Region.
    :param top1_column: top1_column - Prefix for the largest_contributor column.
    :param top2_column: top2_column - Prefix for the second_largest_contributor column.

    :return: data: input dataframe with the addition of top2 calulations for total_column
    '''
    logger = logging.getLogger()
    logger.info("Executing function: calc_top_two")
    top1_column = total_column + "_" + top1_column
    top2_column = total_column + "_" + top2_column
    # Ensure additional columns are zeroed (Belt n Braces)
    data[top1_column] = 0
    data[top2_column] = 0

    # Organise the unique groups to be used for top2 lookup
    aggregations = data[[aggregated_column,
                         additional_aggregated_column]].drop_duplicates()
    aggregations_list = json.loads(aggregations.to_json(orient='records'))

    # Find top 2 in each unique group
    for aggregation in aggregations_list:
        logger.info("Looking for top 2 in: " + str(aggregation))

        # Extract and sort the data
        current_data = data[
            (data[aggregated_column] == aggregation[aggregated_column]) &
            (data[additional_aggregated_column] == aggregation[additional_aggregated_column])]  # noqa
        sorted_data = current_data.sort_values(by=[total_column], ascending=False)
        sorted_data = sorted_data[total_column].reset_index(drop=True)

        # Get the top 2 records
        top_one = sorted_data.iloc[0]
        if len(sorted_data.index) > 1:
            top_two = sorted_data.iloc[1]
        else:
            top_two = 0

        # Save to the output data
        data[[top1_column, top2_column]] = data.apply(
            lambda x: pd.Series([top_one, top_two])
            if (x[aggregated_column] == aggregation[aggregated_column]) &
               (x[additional_aggregated_column] == aggregation[additional_aggregated_column])  # noqa
            else pd.Series([x[top1_column], x[top2_column]]),
            axis=1)

    logger.info("Returning the output data")
    logger.info("Successfully completed function: calc_top_two")
    data = data[[additional_aggregated_column,
                 aggregated_column,
                 top1_column,
                 top2_column]]
    return data
