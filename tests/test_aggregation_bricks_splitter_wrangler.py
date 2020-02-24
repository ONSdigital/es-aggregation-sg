import json
from unittest import mock

import pandas as pd
from es_aws_functions import test_generic_library
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

import aggregation_bricks_splitter_wrangler as lambda_wrangler_function

example_brick_type = {
    "clay": 3,
    "concrete": 2,
    "sandlime": 4
}

wrangler_environment_variables = {
    "bucket_name": "test_bucket",
    "checkpoint": "mock-point",
    "sns_topic_arn": "fake_sns_arn",
    "method_name": "mock-method"
}

wrangler_runtime_variables = {
    "RuntimeVariables": {
        "factors_parameters": {
            "RuntimeVariables": {
                "regionless_code": "99",
                "region_column": "region"
            }
        },
        "run_id": "001",
        "queue_url": "test_queue",
        "in_file_name": "test_splitter_input",
        "incoming_message_group_id": "mock-id",
        "location": "Here",
        "out_file_name_bricks": "test_splitter_bricks_output.json",
        "out_file_name_region": "test_splitter_region_output.json",
        "unique_identifier": [
            "brick_type",
            "enterprise_reference",
            "region"
        ],
        "total_columns": [
            "opening_stock_commons",
            "opening_stock_facings",
            "opening_stock_engineering",
            "produced_commons",
            "produced_facings",
            "produced_engineering",
            "deliveries_commons",
            "deliveries_facings",
            "deliveries_engineering",
            "closing_stock_commons",
            "closing_stock_facings",
            "closing_stock_engineering"
        ],
    }
}


##########################################################################################
#                                     Generic                                            #
##########################################################################################

def test_client_error():
    test_generic_library.client_error(
        lambda_wrangler_function,
        wrangler_runtime_variables,
        wrangler_environment_variables, None,
        "AWS Error", test_generic_library.wrangler_assert)


def test_general_error():
    test_generic_library.general_error(
        lambda_wrangler_function, wrangler_runtime_variables,
        wrangler_environment_variables,
        "aggregation_bricks_splitter_wrangler.EnvironSchema", "General Error",
        test_generic_library.wrangler_assert)


@mock_s3
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.get_dataframe',
            side_effect=test_generic_library.replacement_get_dataframe)
def test_incomplete_read_error(mock_s3_get):
    file_list = ["test_splitter_input.json"]

    test_generic_library.incomplete_read_error(
        lambda_wrangler_function,
        wrangler_runtime_variables,
        wrangler_environment_variables,
        file_list,
        "aggregation_bricks_splitter_wrangler")


def test_key_error():
    test_generic_library.key_error(
        lambda_wrangler_function,
        wrangler_environment_variables, "Key Error",
        test_generic_library.wrangler_assert)


@mock_s3
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.get_dataframe',
            side_effect=test_generic_library.replacement_get_dataframe)
def test_method_error(mock_s3_get):
    file_list = ["test_splitter_input.json"]

    test_generic_library.wrangler_method_error(
        lambda_wrangler_function,
        wrangler_runtime_variables,
        wrangler_environment_variables,
        file_list,
        "aggregation_bricks_splitter_wrangler")


def test_value_error():
    test_generic_library.value_error(
        lambda_wrangler_function, "Error validating environment params",
        test_generic_library.wrangler_assert)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_s3
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.get_dataframe',
            side_effect=test_generic_library.replacement_get_dataframe)
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
def test_wrangler_success(mock_s3_get, mock_s3_put):
    """
    Runs the wrangler function.
    :param None
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_splitter_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with open("tests/fixtures/test_splitter_region_gb_return.json", "r") as file_2:
        test_data_out = file_2.read()

    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         wrangler_environment_variables):
        with mock.patch("aggregation_bricks_splitter_wrangler.boto3.client")\
                as mock_client:
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object

            mock_client_object.invoke.return_value.get.return_value.read \
                .return_value.decode.return_value = json.dumps({
                 "data": test_data_out,
                 "success": True,
                 "anomalies": []
                })

            output = lambda_wrangler_function.lambda_handler(
                wrangler_runtime_variables, test_generic_library.context_object
            )

    with open("tests/fixtures/test_splitter_bricks_prepared_output.json", "r") as file_3:
        test_data_bricks_prepared = file_3.read()
    prepared_data_bricks = pd.DataFrame(json.loads(test_data_bricks_prepared))

    with open("tests/fixtures/" + wrangler_runtime_variables["RuntimeVariables"][
              "out_file_name_bricks"], "r") as file_4:
        test_data_bricks_produced = file_4.read()
    produced_data_bricks = pd.DataFrame(json.loads(test_data_bricks_produced))

    with open("tests/fixtures/test_splitter_region_prepared_output.json", "r") as file_5:
        test_data_region_prepared = file_5.read()
    prepared_data_region = pd.DataFrame(json.loads(test_data_region_prepared))

    with open("tests/fixtures/" + wrangler_runtime_variables["RuntimeVariables"][
              "out_file_name_region"], "r") as file_6:
        produced_data_region = file_6.read()
    produced_data_region = pd.DataFrame(json.loads(produced_data_region))

    assert output
    assert_frame_equal(prepared_data_bricks, produced_data_bricks)
    assert_frame_equal(prepared_data_region, produced_data_region)


def test_calculate_row_type():
    column_list = wrangler_runtime_variables["RuntimeVariables"]["total_columns"]

    with open("tests/fixtures/test_splitter_calculate_row_type_input.json", "r") as file:
        data = file.read()
    dataframe = pd.read_json(data)

    dataframe['brick_type'] = dataframe.apply(
        lambda x: lambda_wrangler_function.calculate_row_type(
            x, example_brick_type, column_list
        ), axis=1
    )

    assert(dataframe['brick_type'][0] == 3)


def test_sum_columns():
    column_list = wrangler_runtime_variables["RuntimeVariables"]["total_columns"]
    unique_identifier = wrangler_runtime_variables[
        "RuntimeVariables"]["unique_identifier"]

    with open("tests/fixtures/test_sum_columns_input.json", "r") as file_1:
        test_data_in = file_1.read()
    input_data = pd.DataFrame(json.loads(test_data_in))

    input_data = input_data.apply(lambda x: lambda_wrangler_function.sum_columns(
        x, example_brick_type, column_list, unique_identifier), axis=1)

    with open("tests/fixtures/test_sum_columns_prepared_output.json", "r") as file_2:
        test_data_prepared = file_2.read()
    output_data = pd.DataFrame(json.loads(test_data_prepared))

    input_data.sort_index(axis=1, inplace=True)
    output_data.sort_index(axis=1, inplace=True)

    assert_frame_equal(input_data, output_data)
