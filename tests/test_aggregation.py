import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import test_generic_library
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

import aggregation_column_method as lambda_method_col_function
import aggregation_top2_method as lambda_method_top2_function
import aggregation_bricks_splitter_wrangler as lambda_pre_wrangler_function
import aggregation_column_wrangler as lambda_wrangler_col_function
import aggregation_top2_wrangler as lambda_wrangler_top2_function
import combiner as lambda_combiner_function

combiner_runtime_variables = {
    "RuntimeVariables":
        {
            "run_id": "bob",
            "additional_aggregated_column": "",
            "aggregated_column": "",
            "in_file_name": "test_wrangler_agg_input",
            "location": "",
            "out_file_name": "test_wrangler_agg_output.json",
            "outgoing_message_group_id": "test_id",
            "queue_url": "Earl",
            "sns_topic_arn": "fake_sns_arn"
        }
}

generic_environment_variables = {
    "bucket_name": "test_bucket",
    "checkpoint": "999",
    "method_name": "aggregation",
    "run_environment": "something"
}

method_cell_runtime_variables = {
    "RuntimeVariables": {
        "run_id": "bob",
        "input_json": None,
        "total_columns": ["Q608_total"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "cell_total_column": "Q608_total",
        "aggregation_type": "sum"
    }
}


method_ent_runtime_variables = {
    "RuntimeVariables": {
        "run_id": "bob",
        "input_json": None,
        "total_columns": ["enterprise_reference"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "cell_total_column": "enterprise_reference",
        "aggregation_type": "nunique"
    }
}

method_top2_runtime_variables = {
    "RuntimeVariables": {
        "input_json": None,
        "run_id": "bob",
        "total_columns": ["Q608_total"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "top1_column": "largest_contributor",
        "top2_column": "second_largest_contributor"
    }
}

pre_wrangler_runtime_variables = {
    "RuntimeVariables":
        {
            "run_id": "bob",
            "in_file_name": "test_wrangler_splitter_input",
            "location": "",
            "out_file_name_bricks": "test_wrangler_bricks_output.json",
            "out_file_name_region": "test_wrangler_region_output.json",
            "outgoing_message_group_id": "test_id",
            "queue_url": "Earl",
            "sns_topic_arn": "fake_sns_arn",
            "total_columns":  ["opening_stock_commons",
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
                               "closing_stock_engineering"],
            "factors_parameters": {
                "RuntimeVariables": {
                    "region_column": "region",
                    "regionless_code": "14"
                }
            },
            "incoming_message_group_id": "",
            "unique_identifier": ["", ""]
        }
}

wrangler_cell_runtime_variables = {
    "RuntimeVariables":
        {
            "run_id": "bob",
            "additional_aggregated_column": "strata",
            "aggregated_column": "region",
            "aggregation_type": "sum",
            "cell_total_column": "Q608_total",
            "in_file_name": "test_wrangler_agg_input",
            "location": "",
            "out_file_name": "test_wrangler_cell_output.json",
            "outgoing_message_group_id": "test_id",
            "queue_url": "Earl",
            "sns_topic_arn": "fake_sns_arn",
            "total_columns": ["Q608_total"]
        }
}

wrangler_ent_runtime_variables = {
    "RuntimeVariables": {
        "run_id": "bob",
        "in_file_name": "test_wrangler_agg_input",
        "total_columns": ["enterprise_reference"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "cell_total_column": "enterprise_reference",
        "aggregation_type": "nunique",
        "location": "",
        "out_file_name": "test_wrangler_ent_output.json",
        "outgoing_message_group_id": "test_id",
        "queue_url": "Earl",
        "sns_topic_arn": "fake_sns_arn"
    }
}

wrangler_top2_runtime_variables = {
    "RuntimeVariables":
        {
            "run_id": "bob",
            "additional_aggregated_column": "a",
            "aggregated_column": "a",
            "in_file_name": "test_wrangler_agg_input",
            "location": "",
            "out_file_name": "test_wrangler_top2_output.json",
            "outgoing_message_group_id": "test_id",
            "queue_url": "Earl",
            "sns_topic_arn": "fake_sns_arn",
            "top1_column": "largest_contributor",
            "top2_column": "second_largest_contributor",
            "total_columns": ["Q608_total"]
        }
}

##########################################################################################
#                                     Generic                                            #
##########################################################################################


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_wrangler_col_function, wrangler_cell_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_wrangler_top2_function, wrangler_top2_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_pre_wrangler_function, pre_wrangler_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert),
        (lambda_combiner_function, combiner_runtime_variables,
         generic_environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
def test_client_error(which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):

    bucket_name = which_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)
    file_list = ["test_wrangler_agg_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_wrangler_col_function, wrangler_cell_runtime_variables,
         generic_environment_variables, "aggregation_column_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_wrangler_top2_function, wrangler_top2_runtime_variables,
         generic_environment_variables, "aggregation_top2_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_pre_wrangler_function, pre_wrangler_runtime_variables,
         generic_environment_variables,
         "aggregation_bricks_splitter_wrangler.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_combiner_function, combiner_runtime_variables,
         generic_environment_variables, "combiner.EnvironSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_method_col_function, method_cell_runtime_variables,
         False, "aggregation_column_method.EnvironSchema",
         "Exception", test_generic_library.method_assert),
        (lambda_method_top2_function, method_top2_runtime_variables,
         False, "aggregation_top2_method.EnvironSchema",
         "Exception", test_generic_library.method_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@mock_s3
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.get_dataframe',
            side_effect=test_generic_library.replacement_get_dataframe)
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,file_list," +
    "lambda_name,expected_message",
    [
        (lambda_wrangler_col_function, wrangler_cell_runtime_variables,
         generic_environment_variables, ["test_wrangler_agg_input.json"],
         "aggregation_column_wrangler", "IncompleteReadError"),
        (lambda_wrangler_top2_function, wrangler_top2_runtime_variables,
         generic_environment_variables, ["test_wrangler_agg_input.json"],
         "aggregation_top2_wrangler", "IncompleteReadError"),
        (lambda_pre_wrangler_function, pre_wrangler_runtime_variables,
         generic_environment_variables, ["test_wrangler_splitter_input.json"],
         "aggregation_bricks_splitter_wrangler", "IncompleteReadError"),
    ])
def test_incomplete_read_error(mock_get_s3, which_lambda, which_runtime_variables,
                               which_environment_variables, file_list, lambda_name,
                               expected_message):

    test_generic_library.incomplete_read_error(which_lambda,
                                               which_runtime_variables,
                                               which_environment_variables,
                                               file_list,
                                               lambda_name,
                                               expected_message)


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion," +
    "which_runtime_variables",
    [
        (lambda_wrangler_col_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert, False),
        (lambda_wrangler_top2_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert, False),
        (lambda_pre_wrangler_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert, False),
        (lambda_combiner_function, generic_environment_variables,
         "KeyError", test_generic_library.wrangler_assert, False),
        (lambda_method_col_function, False,
         "KeyError", test_generic_library.method_assert, method_cell_runtime_variables),
        (lambda_method_top2_function, False,
         "KeyError", test_generic_library.method_assert, method_top2_runtime_variables)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion, which_runtime_variables):
    if not which_runtime_variables:
        test_generic_library.key_error(which_lambda, which_environment_variables,
                                       expected_message, assertion)
    else:
        which_runtime_variables["RuntimeVariables"]["input_json"] = '[{"Test": 0}]'
        test_generic_library.key_error(which_lambda, which_environment_variables,
                                       expected_message, assertion,
                                       which_runtime_variables)


@mock_s3
@mock.patch('aggregation_column_wrangler.aws_functions.get_dataframe',
            side_effect=test_generic_library.replacement_get_dataframe)
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables," +
    "file_list,lambda_name",
    [
        (lambda_wrangler_col_function, wrangler_cell_runtime_variables,
         generic_environment_variables, ["test_wrangler_agg_input.json"],
         "aggregation_column_wrangler"),
        (lambda_wrangler_top2_function, wrangler_top2_runtime_variables,
         generic_environment_variables, ["test_wrangler_agg_input.json"],
         "aggregation_top2_wrangler")
    ])
def test_method_error(mock_s3_get, which_lambda, which_runtime_variables,
                      which_environment_variables, file_list, lambda_name):
    test_generic_library.wrangler_method_error(which_lambda,
                                               which_runtime_variables,
                                               which_environment_variables,
                                               file_list,
                                               lambda_name)


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion",
    [
        (lambda_wrangler_col_function,
         "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_wrangler_top2_function,
         "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_pre_wrangler_function,
         "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_combiner_function,
         "Error validating environment param",
         test_generic_library.wrangler_assert),
        (lambda_method_col_function,
         "Error validating environment param",
         test_generic_library.method_assert),
        (lambda_method_top2_function,
         "Error validating environment param",
         test_generic_library.method_assert)
    ])
def test_value_error(which_lambda, expected_message, assertion):
    test_generic_library.value_error(
        which_lambda, expected_message, assertion)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_s3
def test_calc_top_two():
    """
    Runs the method function.
    :param None.
    :return Test Pass/Fail
    """
    runtime = method_top2_runtime_variables["RuntimeVariables"]

    with open("tests/fixtures/test_calc_top_two_prepared_output.json", "r") as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    with open("tests/fixtures/test_calc_top_two_input.json", "r") as file_2:
        test_data = file_2.read()
    input_data = pd.DataFrame(json.loads(test_data))

    output = lambda_method_top2_function.calc_top_two(
        input_data, runtime["total_columns"][0], runtime["aggregated_column"],
        runtime["additional_aggregated_column"], runtime["top1_column"],
        runtime["top2_column"])

    produced_data = output.sort_index(axis=1)

    assert_frame_equal(produced_data, prepared_data)


@mock_s3
def test_calculate_row_type():
    """
    Runs the method function.
    :param None.
    :return Test Pass/Fail
    """
    brick_type = {
        "clay": 3,
        "concrete": 2,
        "sandlime": 4
    }

    runtime = pre_wrangler_runtime_variables["RuntimeVariables"]

    with open("tests/fixtures/test_calculate_row_type_prepared_output.json", "r")\
            as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    with open("tests/fixtures/test_calculate_row_type_input.json", "r") as file_2:
        test_data = file_2.read()
    input_data = pd.DataFrame(json.loads(test_data))

    input_data[runtime["unique_identifier"][0]] = input_data.apply(
        lambda x: lambda_pre_wrangler_function.calculate_row_type(
            x, brick_type, runtime["total_columns"]),
        axis=1)
    produced_data = input_data.sort_index(axis=1)

    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,input_data,prepared_data",
    [
        (lambda_method_col_function, method_cell_runtime_variables,
         "tests/fixtures/test_method_cell_input.json",
         "tests/fixtures/test_method_cell_prepared_output.json"),
        (lambda_method_col_function, method_ent_runtime_variables,
         "tests/fixtures/test_method_ent_input.json",
         "tests/fixtures/test_method_ent_prepared_output.json"),
        (lambda_method_top2_function, method_top2_runtime_variables,
         "tests/fixtures/test_method_top2_input.json",
         "tests/fixtures/test_method_top2_prepared_output.json")
    ])
def test_method_success(which_lambda, which_runtime_variables, input_data, prepared_data):
    """
    Runs the method function.
    :param None.
    :return Test Pass/Fail
    """
    with open(prepared_data, "r") as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    with open(input_data, "r") as file_2:
        test_data = file_2.read()
    which_runtime_variables["RuntimeVariables"]["input_json"] = test_data

    output = which_lambda.lambda_handler(
        which_runtime_variables, test_generic_library.context_object)

    produced_data = pd.DataFrame(json.loads(output["data"]))

    assert output["success"]
    assert_frame_equal(produced_data, prepared_data)


@mock_s3
def test_sum_columns():
    """
    Runs the method function.
    :param None.
    :return Test Pass/Fail
    """
    brick_type = {
        "clay": 3,
        "concrete": 2,
        "sandlime": 4
    }

    runtime = pre_wrangler_runtime_variables["RuntimeVariables"]

    with open("tests/fixtures/test_sum_columns_prepared_output.json", "r") as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    with open("tests/fixtures/test_sum_columns_input.json", "r") as file_2:
        test_data = file_2.read()
    input_data = pd.DataFrame(json.loads(test_data))

    input_data = input_data.apply(lambda x: lambda_pre_wrangler_function.sum_columns(
        x, brick_type, runtime["total_columns"], runtime["unique_identifier"]),
        axis=1)
    produced_data = input_data.sort_index(axis=1)

    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,which_runtime_variables," +
    "lambda_name,file_list,method_data,prepared_data",
    [
        (lambda_wrangler_col_function, generic_environment_variables,
         wrangler_cell_runtime_variables, "aggregation_column_wrangler",
         ["test_wrangler_agg_input.json"],
         "tests/fixtures/test_method_cell_prepared_output.json",
         "tests/fixtures/test_wrangler_cell_prepared_output.json"),
        (lambda_wrangler_col_function, generic_environment_variables,
         wrangler_ent_runtime_variables, "aggregation_column_wrangler",
         ["test_wrangler_agg_input.json"],
         "tests/fixtures/test_method_ent_prepared_output.json",
         "tests/fixtures/test_wrangler_ent_prepared_output.json"),
        (lambda_wrangler_top2_function, generic_environment_variables,
         wrangler_top2_runtime_variables, "aggregation_top2_wrangler",
         ["test_wrangler_agg_input.json"],
         "tests/fixtures/test_method_top2_prepared_output.json",
         "tests/fixtures/test_wrangler_top2_prepared_output.json")
    ])
def test_wrangler_success(which_lambda, which_environment_variables,
                          which_runtime_variables, lambda_name,
                          file_list, method_data, prepared_data):
    """
    Runs the wrangler function.
    :param mock_s3_get - Replacement Function For The Data Retrieval AWS Functionality.
    :param mock_s3_put - Replacement Function For The Data Saveing AWS Functionality.
    :return Test Pass/Fail
    """
    bucket_name = which_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    test_generic_library.upload_files(client, bucket_name, file_list)

    with open(prepared_data, "r") as file_1:
        test_data_prepared = file_1.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open(method_data, "r") as file_2:
        test_data_out = file_2.read()

    with mock.patch.dict(which_lambda.os.environ,
                         which_environment_variables):
        with mock.patch(lambda_name + '.aws_functions.save_data',
                        side_effect=test_generic_library.replacement_save_data):
            with mock.patch(lambda_name + ".boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object

                mock_client_object.invoke.return_value.get.return_value.read \
                    .return_value.decode.return_value = json.dumps({
                     "data": test_data_out,
                     "success": True,
                     "anomalies": []
                    })

                output = which_lambda.lambda_handler(
                    which_runtime_variables, test_generic_library.context_object
                )

    with open("tests/fixtures/" +
              which_runtime_variables["RuntimeVariables"]["out_file_name"],
              "r") as file_3:
        test_data_produced = file_3.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)


@mock_s3
def test_update_columns():
    """
    Runs the method function.
    :param None.
    :return Test Pass/Fail
    """
    runtime = method_top2_runtime_variables["RuntimeVariables"]
    top1_column = runtime["total_columns"][0] + "_" + runtime["top1_column"]
    top2_column = runtime["total_columns"][0] + "_" + runtime["top2_column"]

    with open("tests/fixtures/test_update_columns_prepared_output.json", "r") as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    with open("tests/fixtures/test_update_columns_input.json", "r") as file_2:
        test_data = file_2.read()
    input_data = pd.DataFrame(json.loads(test_data))

    input_data[[top1_column, top2_column]] = input_data.apply(
        lambda x: lambda_method_top2_function.update_columns(
            x, {'region': 3, 'strata': 'A'}, runtime["aggregated_column"],
            runtime["additional_aggregated_column"], top1_column, top2_column, 225617, 0),
        axis=1)
    produced_data = input_data

    assert_frame_equal(produced_data, prepared_data)


# @mock_s3
# @mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.get_dataframe',
#             side_effect=test_generic_library.replacement_get_dataframe)
# @pytest.mark.parametrize(
#     "which_lambda,which_environment_variables,which_runtime_variables," +
#     "lambda_name,file_list,method_data,prepared_data",
#     [
#         (lambda_pre_wrangler_function, generic_environment_variables,
#          pre_wrangler_runtime_variables, "aggregation_bricks_splitter_wrangler",
#          ["test_wrangler_splitter_input.json"],
#          "tests/fixtures/test_wrangler_agg_input.json",
#          ["tests/fixtures/test_wrangler_agg_input.json",
#           "tests/fixtures/test_wrangler_splitter_prepared_bricks_output.json",
#           "tests/fixtures/test_wrangler_splitter_prepared_region_output.json"])
#     ])
# def test_splitter_wrangler_success(mock_s3_get, which_lambda, which_environment_variables,
#                                    which_runtime_variables, lambda_name,
#                                    file_list, method_data, prepared_data):
#     """
#     Runs the wrangler function.
#     :param mock_s3_get - Replacement Function For The Data Retrieval AWS Functionality.
#     :param mock_s3_put - Replacement Function For The Data Saveing AWS Functionality.
#     :return Test Pass/Fail
#     """
#     bucket_name = which_environment_variables["bucket_name"]
#     client = test_generic_library.create_bucket(bucket_name)
#
#     test_generic_library.upload_files(client, bucket_name, file_list)
#
#     with open(prepared_data[0], "r") as file_1:
#         test_data_prepared = file_1.read()
#     prepared_data = pd.DataFrame(json.loads(test_data_prepared))
#
#     with open(method_data, "r") as file_2:
#         test_data_out = file_2.read()
#
#     with mock.patch.dict(which_lambda.os.environ,
#                          which_environment_variables):
#         with mock.patch(lambda_name + '.aws_functions.save_data',
#                         side_effect=test_generic_library.replacement_save_data):
#             with mock.patch(lambda_name + ".boto3.client") as mock_client:
#                 mock_client_object = mock.Mock()
#                 mock_client.return_value = mock_client_object
#
#                 mock_client_object.invoke.return_value.get.return_value.read \
#                     .return_value.decode.return_value = json.dumps({
#                      "data": test_data_out,
#                      "success": True,
#                      "anomalies": []
#                     })
#
#                 output = which_lambda.lambda_handler(
#                     which_runtime_variables, test_generic_library.context_object
#                 )
#
#     with open("tests/fixtures/" +
#               which_runtime_variables["RuntimeVariables"]["out_file_name"],
#               "r") as file_3:
#         test_data_produced = file_3.read()
#     produced_data = pd.DataFrame(json.loads(test_data_produced))
#
#     assert output
#     assert_frame_equal(produced_data, prepared_data)
