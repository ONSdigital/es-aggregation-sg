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

    data = fields.Str(required=True)
    total_columns = fields.List(fields.String, required=True)
    additional_aggregated_column = fields.Str(required=True)
    aggregated_column = fields.Str(required=True)
    cell_total_column = fields.Str(required=True)
    aggregation_type = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Generates a JSON dataset, grouped by the given aggregated_column(e.g.county) with
     the given total_column(e.g.Q608_total) aggregated by the given aggregation_type
     (e.g.Sum) as a new column called cell_total_column(e.g.county_total).

    :param event: {
        data - JSON String of the data.
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
    logger.setLevel(10)
    run_id = 0
    try:
        logger.info("Aggregation by column - Method begun.")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Runtime Variables
        additional_aggregated_column = runtime_variables["additional_aggregated_column"]
        aggregated_column = runtime_variables["aggregated_column"]
        aggregation_type = runtime_variables["aggregation_type"]
        cell_total_column = runtime_variables["cell_total_column"]
        data = json.loads(runtime_variables["data"])
        total_columns = runtime_variables["total_columns"]

        input_dataframe = pd.DataFrame(data)
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

        output_json = agg_by_county_output.to_json(orient="records")
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
    final_output["success"] = True
    return final_output
