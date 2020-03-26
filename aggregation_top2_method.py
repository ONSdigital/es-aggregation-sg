import json
import logging

import marshmallow
import pandas as pd
from es_aws_functions import general_functions


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
    response_json = None
    run_id = 0
    logger.info("Starting " + current_module)
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']
        # Set up Environment variables Schema.
        schema = EnvironSchema(strict=False)
        config, errors = schema.load(event["RuntimeVariables"])
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
            if counter == 0:
                top_two_output = response
            else:
                to_aggregate = [aggregated_column]
                if additional_aggregated_column != "":
                    to_aggregate.append(additional_aggregated_column)

                top_two_output = top_two_output.merge(response,
                                                      on=to_aggregate, how="left")
            counter += 1

        response = top_two_output
        logger.info("Converting output dataframe to json")
        response_json = response.to_json(orient='records')
        final_output = {"data": response_json}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output['success'] = True
    return final_output


def calc_top_two(data, total_column, aggregated_column, additional_aggregated_column,
                 top1_column, top2_column):
    """
    :param data: Input Dataframe
    :param total_column - The name of the column to produce aggregation for.
    :param aggregated_column: A column to aggregate by. e.g. Enterprise_Reference.
    :param additional_aggregated_column: A column to aggregate by. e.g. Region.
    :param top1_column: top1_column - Prefix for the largest_contributor column.
    :param top2_column: top2_column - Prefix for the second_largest_contributor column.

    :return: data: input dataframe with the addition of top2 calulations for total_column
    """
    logger = logging.getLogger()
    logger.info("Executing function: calc_top_two")
    top1_column = total_column + "_" + top1_column
    top2_column = total_column + "_" + top2_column
    # Ensure additional columns are zeroed (Belt n Braces)
    data[top1_column] = 0
    data[top2_column] = 0

    to_aggregate = [aggregated_column]
    if additional_aggregated_column != "":
        to_aggregate.append(additional_aggregated_column)

    # Organise the unique groups to be used for top2 lookup
    aggregations = data[to_aggregate].drop_duplicates()
    aggregations_list = json.loads(aggregations.to_json(orient='records'))

    # Find top 2 in each unique group
    for aggregation in aggregations_list:
        logger.info("Looking for top 2 in: " + str(aggregation))

        # Extract and sort the data
        current_data = data[data[aggregated_column] == aggregation[aggregated_column]]
        if additional_aggregated_column != "":
            data[data[additional_aggregated_column] == aggregation[additional_aggregated_column]]  # noqa
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
            lambda x: update_columns(x, aggregation, aggregated_column,
                                     additional_aggregated_column,
                                     top1_column, top2_column,
                                     top_one, top_two),
            axis=1)

    logger.info("Returning the output data")
    logger.info("Successfully completed function: calc_top_two")
    filter_output = [
        aggregated_column,
        top1_column,
        top2_column
    ]

    if additional_aggregated_column != "":
        filter_output.append(additional_aggregated_column)

    data = data[filter_output]
    return data


def update_columns(data, aggregation, aggregated_column, additional_aggregated_column,
                   top1_column, top2_column, top_one, top_two):
    """
    Used to check if for the current cell the responder needs to update what it contains
    for top2 data or if it should overwrite data with its currently held value.
    :param data: Input Dataframe.
    :param aggregation: Dict containing the values to identify the current unique cell.
    :param aggregated_column: A column to aggregate by. e.g. Enterprise_Reference.
    :param additional_aggregated_column: A column to aggregate by. e.g. Region.
    :param top1_column: top1_column - Prefix for the largest_contributor column.
    :param top2_column: top2_column - Prefix for the second_largest_contributor column.
    :param top_one: Top value for the current cell.
    :param top_two: Second top value for the current cell.

    :return: data: Series containing two elements. The chosen top and second top data.
    """

    if data[aggregated_column] != aggregation[aggregated_column]:
        return pd.Series([data[top1_column], data[top2_column]])
    elif additional_aggregated_column != "":
        if data[additional_aggregated_column] != aggregation[
           additional_aggregated_column]:
            return pd.Series([data[top1_column], data[top2_column]])
        else:
            return pd.Series([top_one, top_two])
    else:
        return pd.Series([top_one, top_two])
