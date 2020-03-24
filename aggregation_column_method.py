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
    cell_total_column = marshmallow.fields.Str(required=True)
    aggregation_type = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    Generates a JSON dataset, grouped by the given aggregated_column(e.g.county) with
     the given total_column(e.g.Q608_total) aggregated by the given aggregation_type
     (e.g.Sum) as a new column called cell_total_column(e.g.county_total).

    :param event: {
        input_json - JSON String of the data.
        aggregated_column - A column to aggregate by. e.g. Enterprise_Reference.
        additional_aggregated_column - A column to aggregate by. e.g. Region.
        aggregation_type - How we wish to do the aggregation. e.g. sum, count, nunique.
        total_columns - The names of the columns to produce aggregations for.
        cell_total_column - Name of column to rename total_column.
    }

    :param context: N/A
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Aggregation by column - Method"
    error_message = ""
    logger = logging.getLogger("Aggregation")
    logger.setLevel(0)
    run_id = 0
    try:
        logger.info("Aggregation by column - Method begun.")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']
        # Set up Environment variables Schema.
        schema = EnvironSchema(strict=False)
        config, errors = schema.load(event["RuntimeVariables"])
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        input_json = json.loads(config["input_json"])

        additional_aggregated_column = config["additional_aggregated_column"]
        aggregated_column = config["aggregated_column"]
        cell_total_column = config["cell_total_column"]
        aggregation_type = config["aggregation_type"]
        # Total columns can go through as a list
        total_columns = config['total_columns']

        input_dataframe = pd.DataFrame(input_json)
        totals_dict = {total_column: aggregation_type for total_column in total_columns}

        logger.info("JSON data converted to DataFrame.")

        to_aggregate = [aggregated_column]
        if additional_aggregated_column != "":
            to_aggregate.append(additional_aggregated_column)

        county_agg = input_dataframe.groupby(to_aggregate)

        agg_by_county_output = county_agg.agg(totals_dict) \
            .reset_index()

        for total_column in total_columns:
            if "total" not in cell_total_column:
                column_cell_total_column = cell_total_column
            else:
                column_cell_total_column = cell_total_column + "_" + total_column
            agg_by_county_output = agg_by_county_output.rename(
                columns={total_column: column_cell_total_column},
                inplace=False
            )

        logger.info("Column totals successfully calculated.")

        output_json = agg_by_county_output.to_json(orient='records')
        final_output = {"data": output_json}
        logger.info("DataFrame converted to JSON for output.")

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
