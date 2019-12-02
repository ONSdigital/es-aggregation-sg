import json
import logging

import marshmallow
import boto3
import pandas as pd

lambda_client = boto3.client('lambda', region_name='eu-west-2')

class EnvironSchema(marshmallow.Schema):
    """
    Class to set up the environment variables schema.
    """
    input_json = marshmallow.fields.Str(required=True)
    ent_ref_column = marshmallow.fields.Str(required=True)
    period_column = marshmallow.fields.Str(required=True)
    region_column = marshmallow.fields.Str(required=True)
    county_column = marshmallow.fields.Str(required=True)
    cell_total_column = marshmallow.fields.Str(required=True)

def lambda_handler(event, context):
    """
    This method is responsible for grouping the data by county, region
    and period. It then aggregates on enterprise_reference creating a
    count and then renames the column accordingly.
    :param event: The data you wish to perform this method on.
    :param context:
    :return: output_json: The grouped and aggregated data. - Type: JSON
    """
    current_module = "Aggregation CalculateEnterpriseRef - Method"
    logger = logging.getLogger("Entref")
    error_message = ""
    log_message = ""

    try:
        logger.info("Starting Method " + current_module)

        # Set up Environment variables Schema.
        schema = EnvironSchema(strict=False)
        config, errors = schema.load(event)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        input_json = json.loads(config["input_json"])
        period_column = config["period_column"]
        region_column = config["region_column"]
        county_column = config["county_column"]
        ent_ref_column = config["ent_ref_column"]
        cell_total_column = config["cell_total_column"]

        input_dataframe = pd.DataFrame(input_json)

        region_agg = input_dataframe.groupby([county_column, region_column, period_column])
        agg_by_region_output = region_agg.agg({ent_ref_column: 'nunique'}).reset_index()
        agg_by_region_output.rename(columns={ent_ref_column: cell_total_column},
                                    inplace=True)

        output_json = agg_by_region_output.to_json(orient='records')

    except KeyError as e:
        error_message = "Key Error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = "General Error in " \
                        + current_module + " (" \
                        + str(type(e)) + ") |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:

        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return json.loads(output_json)
