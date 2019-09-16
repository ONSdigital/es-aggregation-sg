import logging

import boto3
import pandas as pd

lambda_client = boto3.client('lambda', region_name='eu-west-2')


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

        input_json = event

        input_dataframe = pd.DataFrame(input_json)

        region_agg = input_dataframe.groupby(['county', 'region', 'period'])
        agg_by_region_output = region_agg.agg({'enterprise_ref': 'nunique'}).reset_index()
        agg_by_region_output.rename(columns={'enterprise_ref': 'ent_ref_count'},
                                    inplace=True)

        output_json = agg_by_region_output.to_json(orient='records')

    except KeyError as e:
        error_message = "Key Error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = "General Error in " \
                        + current_module + " (" \
                        + str(type(e)) + ") |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:

        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return output_json
