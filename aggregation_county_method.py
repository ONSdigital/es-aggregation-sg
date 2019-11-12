import logging

import pandas as pd


def lambda_handler(event, context):
    """
    Generates a JSON dataset, grouped by region, county and period,
    with the Q608 totals (sum) as a new column called 'county_total'.
    :param event: Event object
    :param context: Context object
    :return: JSON string
    """
    current_module = "Aggregation County - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Aggregation")
    output_json = ""
    try:
        logger.info("Aggregation county method begun.")

        input_json = event

        input_dataframe = pd.DataFrame(input_json)

        logger.info("JSON data converted to DataFrame.")

        county_agg = input_dataframe.groupby(['region', 'county', 'period'])

        agg_by_county_output = county_agg.agg({'Q608_total': 'sum'}).reset_index()

        agg_by_county_output.rename(
            columns={'Q608_total': 'county_total'},
            inplace=True
        )

        logger.info("County totals successfully calculated.")

        output_json = agg_by_county_output.to_json(orient='records')

        logger.info("DataFrame converted to JSON for output.")

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args) + " | Request ID: "
                         + str(context.aws_request_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return output_json
