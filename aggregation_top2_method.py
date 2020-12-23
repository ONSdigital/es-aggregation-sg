import json
import logging

import pandas as pd
from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    additional_aggregated_column = fields.Str(required=True)
    aggregated_column = fields.Str(required=True)
    bpm_queue_url = fields.Str(required=True)
    data = fields.Str(required=True)
    environment = fields.Str(required=True)
    survey = fields.Str(required=True)
    top1_column = fields.Str(required=True)
    top2_column = fields.Str(required=True)
    total_columns = fields.List(fields.String, required=True)


def lambda_handler(event, context):
    """
    This method loops through each county and records largest & second largest value
     against each record in the group.


    :param event: {
        data - JSON String of the data.
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

    error_message = ""
    run_id = 0
    bpm_queue_url = None

    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        # Runtime Variables
        additional_aggregated_column = runtime_variables["additional_aggregated_column"]
        aggregated_column = runtime_variables["aggregated_column"]
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        data = json.loads(runtime_variables["data"])
        environment = runtime_variables["environment"]
        survey = runtime_variables["survey"]
        top1_column = runtime_variables["top1_column"]
        top2_column = runtime_variables["top2_column"]
        total_columns = runtime_variables["total_columns"]

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module, run_id,
                                                           context=context)
        return {"success": False, "error": error_message}

    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context)
        return {"success": False, "error": error_message}

    try:
        logger.info("Started - retrieved configuration variables from wrangler.")
        input_dataframe = pd.DataFrame(data)
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
        response_json = response.to_json(orient="records")
        final_output = {"data": response_json}
    except Exception as e:
        error_message = general_functions.handle_exception(e,
                                                           current_module,
                                                           run_id,
                                                           context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}
    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
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

    to_aggregate = [aggregated_column]
    if additional_aggregated_column != "":
        to_aggregate.append(additional_aggregated_column)

    # Group data on groupby columns and collect list of total column.

    grouped_data = data.groupby(to_aggregate, as_index=False)\
        .agg({total_column: col_to_list})

    grouped_data = grouped_data.apply(
        lambda x: do_top_two(x, total_column, top1_column, top2_column), axis=1)

    grouped_data = grouped_data.drop(total_column, axis=1)

    logger.info("Returning the output data")
    filter_output = [
        aggregated_column,
        top1_column,
        top2_column
    ]

    if additional_aggregated_column != "":
        filter_output.append(additional_aggregated_column)

    grouped_data = grouped_data[filter_output]
    logger.info("Successfully completed function: calc_top_two")

    return grouped_data


def col_to_list(series):
    """
    Aggregates values in a series into a single list
    :param series:
    :return: List: values in series
    """
    return series.tolist()


def do_top_two(row, column, top1, top2):
    """
    Apply Method.
    Calculates and appends top two data on row.
    Assumes that the column has been aggregated to a list.
    :param row: Row of grouped dataframe
    :param column: String - Name of the column which holds list of values
    :param top1: String - Name of top1 column
    :param top2: String - Name of top2 column
    :return row:
    """
    value_list = row[column]
    value_list.sort(reverse=True)
    row[top1] = value_list[0]
    if(len(value_list) > 1):
        row[top2] = value_list[1]
    else:
        row[top2] = 0

    return row
