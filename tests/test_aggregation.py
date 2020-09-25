import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_s3
from pandas.testing import assert_frame_equal

import aggregation_bricks_splitter_wrangler as lambda_pre_wrangler_function
import aggregation_column_method as lambda_method_col_function
import aggregation_column_wrangler as lambda_wrangler_col_function
import aggregation_top2_method as lambda_method_top2_function
import aggregation_top2_wrangler as lambda_wrangler_top2_function
import combiner as lambda_combiner_function

combiner_runtime_variables = {
    "RuntimeVariables":
        {
            "run_id": "bob",
            "additional_aggregated_column": "strata",
            "aggregated_column": "region",
            "in_file_name": "test_wrangler_agg_input",
            "out_file_name": "test_wrangler_combiner_output.json",
            "sns_topic_arn": "fake_sns_arn",
            "bpm_queue_url": "fake_queue_url",
            "total_steps": "6",
            "aggregation_files": {
                "ent_ref_agg": "test_wrangler_cell_prepared_output",
                "cell_agg": "test_wrangler_ent_prepared_output",
                "top2_agg": "test_wrangler_top2_prepared_output",
            }
        }
}

generic_environment_variables = {
    "bucket_name": "test_bucket",
    "method_name": "aggregation",
    "run_environment": "something"
}

method_cell_runtime_variables = {
    "RuntimeVariables": {
        "run_id": "bob",
        "data": None,
        "total_columns": ["Q608_total"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "cell_total_column": "cell_total",
        "aggregation_type": "sum"
    }
}

method_ent_runtime_variables = {
    "RuntimeVariables": {
        "run_id": "bob",
        "data": None,
        "total_columns": ["enterprise_reference"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "cell_total_column": "ent_ref_count",
        "aggregation_type": "nunique"
    }
}

method_top2_runtime_variables = {
    "RuntimeVariables": {
        "data": None,
        "run_id": "bob",
        "total_columns": ["Q608_total"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "top1_column": "largest_contributor",
        "top2_column": "second_largest_contributor",
        "bpm_queue_url": "fake_queue_url"
    }
}

method_top2_multi_runtime_variables = {
    "RuntimeVariables": {
        "data": None,
        "run_id": "bob",
        "total_columns": ["Q608_total", "Q607_constructional_fill"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "top1_column": "largest_contributor",
        "top2_column": "second_largest_contributor",
        "bpm_queue_url": "fake_queue_url"
    }
}

pre_wrangler_runtime_variables = {
    "RuntimeVariables":
        {
            "run_id": "bob",
            "in_file_name": "test_wrangler_splitter_input",
            "out_file_name_bricks": "test_wrangler_splitter_bricks_output.json",
            "out_file_name_region": "test_wrangler_splitter_region_output.json",
            "sns_topic_arn": "fake_sns_arn",
            "bpm_queue_url": "fake_queue_url",
            "total_steps": "6",
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
            "unique_identifier": [
                "brick_type",
                "enterprise_reference",
                "region"
            ]
        }
}

wrangler_cell_runtime_variables = {
    "RuntimeVariables":
        {
            "run_id": "bob",
            "additional_aggregated_column": "strata",
            "aggregated_column": "region",
            "aggregation_type": "sum",
            "cell_total_column": "cell_total",
            "in_file_name": "test_wrangler_agg_input",
            "out_file_name": "test_wrangler_cell_output.json",
            "sns_topic_arn": "fake_sns_arn",
            "total_columns": ["Q608_total"],
            "bpm_queue_url": "fake_queue_url",
            "total_steps": "6"
        }
}

wrangler_ent_runtime_variables = {
    "RuntimeVariables": {
        "run_id": "bob",
        "in_file_name": "test_wrangler_agg_input",
        "total_columns": ["enterprise_reference"],
        "additional_aggregated_column": "strata",
        "aggregated_column": "region",
        "cell_total_column": "ent_ref_count",
        "aggregation_type": "nunique",
        "out_file_name": "test_wrangler_ent_output.json",
        "sns_topic_arn": "fake_sns_arn"
    }
}

wrangler_top2_runtime_variables = {
    "RuntimeVariables":
        {
            "run_id": "bob",
            "additional_aggregated_column": "strata",
            "aggregated_column": "region",
            "in_file_name": "test_wrangler_agg_input",
            "out_file_name": "test_wrangler_top2_output.json",
            "sns_topic_arn": "fake_sns_arn",
            "top1_column": "largest_contributor",
            "top2_column": "second_largest_contributor",
            "total_columns": ["Q608_total"],
            "bpm_queue_url": "fake_queue_url",
            "total_steps": "6"
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
         "ClientError", test_generic_library.wrangler_assert)
    ])
@mock.patch('aggregation_top2_wrangler.aws_functions.send_bpm_status')
def test_client_error(mock_bpm_status, which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):

    bucket_name = which_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)
    file_list = ["test_wrangler_agg_input.json", "test_wrangler_splitter_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_wrangler_col_function, wrangler_cell_runtime_variables,
         generic_environment_variables, "aggregation_column_wrangler.EnvironmentSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_wrangler_top2_function, wrangler_top2_runtime_variables,
         generic_environment_variables, "aggregation_top2_wrangler.EnvironmentSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_pre_wrangler_function, pre_wrangler_runtime_variables,
         generic_environment_variables,
         "aggregation_bricks_splitter_wrangler.EnvironmentSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_combiner_function, combiner_runtime_variables,
         generic_environment_variables, "combiner.EnvironmentSchema",
         "Exception", test_generic_library.wrangler_assert),
        (lambda_method_col_function, method_cell_runtime_variables,
         False, "aggregation_column_method.RuntimeSchema",
         "Exception", test_generic_library.method_assert),
        (lambda_method_top2_function, method_top2_runtime_variables,
         False, "aggregation_top2_method.RuntimeSchema",
         "Exception", test_generic_library.method_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@mock_s3
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
def test_incomplete_read_error(which_lambda, which_runtime_variables,
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
@mock.patch('aggregation_top2_wrangler.aws_functions.send_bpm_status')
def test_key_error(mock_bpm_status, which_lambda, which_environment_variables,
                   expected_message, assertion, which_runtime_variables):
    if not which_runtime_variables:
        test_generic_library.key_error(which_lambda, which_environment_variables,
                                       expected_message, assertion)
    else:
        which_runtime_variables["RuntimeVariables"]["data"] = '[{"Test": 0}]'
        test_generic_library.key_error(which_lambda, which_environment_variables,
                                       expected_message, assertion,
                                       which_runtime_variables)


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables," +
    "file_list,lambda_name",
    [
        (lambda_wrangler_col_function, wrangler_cell_runtime_variables,
         generic_environment_variables, ["test_wrangler_agg_input.json"],
         "aggregation_column_wrangler"),
        (lambda_wrangler_top2_function, wrangler_top2_runtime_variables,
         generic_environment_variables, ["test_wrangler_agg_input.json"],
         "aggregation_top2_wrangler"),
        (lambda_pre_wrangler_function, pre_wrangler_runtime_variables,
         generic_environment_variables, ["test_wrangler_splitter_input.json"],
         "aggregation_bricks_splitter_wrangler")
    ])
def test_method_error(which_lambda, which_runtime_variables,
                      which_environment_variables, file_list, lambda_name):
    test_generic_library.wrangler_method_error(which_lambda,
                                               which_runtime_variables,
                                               which_environment_variables,
                                               file_list,
                                               lambda_name)


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_environment_variables",
    [
        (lambda_wrangler_col_function,
         "Error validating environment param",
         test_generic_library.wrangler_assert, {}),
        (lambda_wrangler_col_function,
         "Error validating runtime param",
         test_generic_library.wrangler_assert, generic_environment_variables),
        (lambda_wrangler_top2_function,
         "Error validating environment param",
         test_generic_library.wrangler_assert, {}),
        (lambda_wrangler_top2_function,
         "Error validating runtime param",
         test_generic_library.wrangler_assert, generic_environment_variables),
        (lambda_pre_wrangler_function,
         "Error validating environment param",
         test_generic_library.wrangler_assert, {}),
        (lambda_pre_wrangler_function,
         "Error validating runtime param",
         test_generic_library.wrangler_assert, generic_environment_variables),
        (lambda_combiner_function,
         "Error validating environment param",
         test_generic_library.wrangler_assert, {}),
        (lambda_combiner_function,
         "Error validating runtime param",
         test_generic_library.wrangler_assert, generic_environment_variables),
        (lambda_method_col_function,
         "Error validating runtime param",
         test_generic_library.method_assert, {}),
        (lambda_method_top2_function,
         "Error validating runtime param",
         test_generic_library.method_assert, {})
    ])
def test_value_error(which_lambda, expected_message,
                     assertion, which_environment_variables):
    test_generic_library.value_error(
        which_lambda, expected_message,
        assertion, environment_variables=which_environment_variables)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_s3
def test_calc_top_two():
    """
    Runs the calc_top_two function.
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
    Runs the calculate_row_type function.
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
    produced_data = input_data

    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@mock.patch('combiner.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
@mock.patch('combiner.aws_functions.send_sns_message')
@mock.patch('combiner.aws_functions.send_bpm_status')
def test_combiner_success(mock_bpm_status, mock_sns, mock_s3_put):
    """
    Runs the wrangler function.
    :param mock_s3_put: Replacement Function
                        For The Data Saving AWS Functionality. - Mock.
    :param mock_sns: Replacement function mocking SNS sends.
    :param mock_bpm_status: Replacement function mocking bpm status calls.
    :return Test Pass/Fail
    """
    bucket_name = generic_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = [
        "test_wrangler_agg_input.json",
        "test_wrangler_cell_prepared_output.json",
        "test_wrangler_ent_prepared_output.json",
        "test_wrangler_top2_prepared_output.json"
    ]
    test_generic_library.upload_files(client, bucket_name, file_list)

    with open("tests/fixtures/test_wrangler_combiner_prepared_output.json", "r")\
            as file_1:
        test_data_prepared = file_1.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with mock.patch.dict(lambda_combiner_function.os.environ,
                         generic_environment_variables):

        output = lambda_combiner_function.lambda_handler(
            combiner_runtime_variables, test_generic_library.context_object
        )

    with open("tests/fixtures/" +
              combiner_runtime_variables["RuntimeVariables"]["out_file_name"],
              "r") as file_4:
        test_data_produced = file_4.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)


@pytest.mark.parametrize(
    "input_data,prepared_data",
    [
        ([{"A": 0, "B": 0, "C": 0}], True),
        ([{"A": 1, "B": 2, "C": 3}], False)
    ])
def test_do_check(input_data, prepared_data):

    quest = ["A", "B", "C"]
    working_dataframe = pd.DataFrame(input_data)

    working_dataframe["zero_data"] = working_dataframe.apply(
        lambda x: lambda_pre_wrangler_function.do_check(x, quest), axis=1)

    produced_data = working_dataframe["zero_data"][0]

    assert produced_data == prepared_data


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
         "tests/fixtures/test_method_top2_prepared_output.json"),
        (lambda_method_top2_function, method_top2_multi_runtime_variables,
         "tests/fixtures/test_method_top2_input.json",
         "tests/fixtures/test_method_top2_multi_prepared_output.json")
    ])
def test_method_success(which_lambda, which_runtime_variables, input_data, prepared_data):
    """
    Runs the method function.
    :param which_lambda: Main function.
    :param which_runtime_variables: RuntimeVariables. - Dict.
    :param input_data: File name/location of the data to be passed in. - String.
    :param prepared_data: File name/location of the data
                          to be used for comparison. - String.
    :return Test Pass/Fail
    """
    with open(prepared_data, "r") as file_1:
        file_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(file_data))

    with open(input_data, "r") as file_2:
        test_data = file_2.read()
    which_runtime_variables["RuntimeVariables"]["data"] = test_data

    output = which_lambda.lambda_handler(
        which_runtime_variables, test_generic_library.context_object)

    produced_data = pd.DataFrame(json.loads(output["data"])).sort_index(axis=1)
    assert output["success"]
    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.save_to_s3',
            side_effect=test_generic_library.replacement_save_to_s3)
def test_splitter_wrangler_success(mock_s3_put):
    """
    Runs the wrangler function.
    :param mock_s3_put - Replacement Function For The Data Saving AWS Functionality.
    :return Test Pass/Fail
    """
    bucket_name = generic_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_wrangler_splitter_input.json"]
    test_generic_library.upload_files(client, bucket_name, file_list)

    with open("tests/fixtures/test_wrangler_splitter_prepared_region_output.json", "r")\
            as file_1:
        test_data_prepared_region = file_1.read()
    prepared_data_region = pd.DataFrame(json.loads(test_data_prepared_region))

    with open("tests/fixtures/test_wrangler_splitter_prepared_bricks_output.json", "r")\
            as file_2:
        test_data_prepared_bricks = file_2.read()
    prepared_data_bricks = pd.DataFrame(json.loads(test_data_prepared_bricks))

    with open("tests/fixtures/test_method_splitter_prepared_output.json", "r") as file_3:
        test_data_out = file_3.read()

    with mock.patch.dict(lambda_pre_wrangler_function.os.environ,
                         generic_environment_variables):
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

            output = lambda_pre_wrangler_function.lambda_handler(
                pre_wrangler_runtime_variables, test_generic_library.context_object
            )

    with open("tests/fixtures/" +
              pre_wrangler_runtime_variables["RuntimeVariables"]["out_file_name_region"],
              "r") as file_4:
        test_data_produced_region = file_4.read()
    produced_data_region = pd.DataFrame(json.loads(test_data_produced_region))

    with open("tests/fixtures/" +
              pre_wrangler_runtime_variables["RuntimeVariables"]["out_file_name_bricks"],
              "r") as file_4:
        test_data_produced_bricks = file_4.read()
    produced_data_bricks = pd.DataFrame(json.loads(test_data_produced_bricks))

    assert output
    assert_frame_equal(produced_data_region, prepared_data_region)
    assert_frame_equal(produced_data_bricks, prepared_data_bricks)


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
    produced_data = input_data

    assert_frame_equal(produced_data, prepared_data)


@mock_s3
@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,which_runtime_variables," +
    "lambda_name,file_list,method_data,which_method_variables",
    [
        (lambda_wrangler_col_function, generic_environment_variables,
         wrangler_cell_runtime_variables, "aggregation_column_wrangler",
         ["test_wrangler_agg_input.json"],
         "tests/fixtures/test_method_cell_input.json",
         method_cell_runtime_variables),
        (lambda_wrangler_col_function, generic_environment_variables,
         wrangler_ent_runtime_variables, "aggregation_column_wrangler",
         ["test_wrangler_agg_input.json"],
         "tests/fixtures/test_method_ent_input.json",
         method_ent_runtime_variables),
        (lambda_wrangler_top2_function, generic_environment_variables,
         wrangler_top2_runtime_variables, "aggregation_top2_wrangler",
         ["test_wrangler_agg_input.json"],
         "tests/fixtures/test_method_top2_input.json",
         method_top2_runtime_variables)
    ])
def test_wrangler_success_passed(which_lambda, which_environment_variables,
                                 which_runtime_variables, lambda_name,
                                 file_list, method_data, which_method_variables):
    """
    Runs the wrangler function.
    :param which_lambda: Main function.
    :param which_environment_variables: Environment Variables. - Dict.
    :param which_runtime_variables: RuntimeVariables. - Dict.
    :param lambda_name: Name of the py file. - String.
    :param file_list: Files to be added to the fake S3. - List(String).
    :param method_data: File name/location of the data
                        to be passed out by the method. - String.
    :param which_method_variables: Variables to compare against. - Dict.
    :return Test Pass/Fail
    """
    bucket_name = which_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    test_generic_library.upload_files(client, bucket_name, file_list)

    with mock.patch.dict(which_lambda.os.environ,
                         which_environment_variables):
        with mock.patch(lambda_name + '.aws_functions.save_to_s3',
                        side_effect=test_generic_library.replacement_save_to_s3):
            with mock.patch(lambda_name + ".boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object

                # Rather than mock the get/decode we tell the code that when the invoke is
                # called pass the variables to this replacement function instead.
                mock_client_object.invoke.side_effect = \
                    test_generic_library.replacement_invoke

                # This stops the Error caused by the replacement function from stopping
                # the test.
                with pytest.raises(exception_classes.LambdaFailure):
                    which_lambda.lambda_handler(
                        which_runtime_variables, test_generic_library.context_object
                    )

            with open(method_data, "r") as file_1:
                test_data_prepared = file_1.read()
            prepared_data = pd.DataFrame(json.loads(test_data_prepared), dtype=float)

            with open("tests/fixtures/test_wrangler_to_method_input.json", "r") as file_2:
                test_data_produced = file_2.read()
            produced_data = pd.DataFrame(json.loads(test_data_produced), dtype=float)

            # Compares the data.
            assert_frame_equal(produced_data, prepared_data)

            with open("tests/fixtures/test_wrangler_to_method_runtime.json",
                      "r") as file_3:
                test_dict_prepared = file_3.read()
            produced_dict = json.loads(test_dict_prepared)

            # Ensures data is not in the RuntimeVariables and then compares.
            which_method_variables["RuntimeVariables"]["data"] = None
            assert produced_dict == which_method_variables["RuntimeVariables"]


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
def test_wrangler_success_returned(which_lambda, which_environment_variables,
                                   which_runtime_variables, lambda_name,
                                   file_list, method_data, prepared_data):
    """
    Runs the wrangler function.
    :param which_lambda: Main function.
    :param which_environment_variables: Environment Variables. - Dict.
    :param which_runtime_variables: RuntimeVariables. - Dict.
    :param lambda_name: Name of the py file. - String.
    :param file_list: Files to be added to the fake S3. - List(String).
    :param method_data: File name/location of the data
                        to be passed out by the method. - String.
    :param prepared_data: File name/location of the data
                          to be used for comparison. - String.
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
        with mock.patch(lambda_name + '.aws_functions.save_to_s3',
                        side_effect=test_generic_library.replacement_save_to_s3):
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


def test_col_to_list():
    """
    Runs the col to list method
    :param None.
    :return Test Pass/Fail
    """
    input = pd.Series([1, 2, 3, 4, 5])
    output = lambda_method_top2_function.col_to_list(input)
    assert output == [1, 2, 3, 4, 5]


def test_do_top_two():
    """
    Tests the apply method that sets top two on the data
    :param None.
    :return Test Pass/Fail
    """
    with open("tests/fixtures/test_do_top_two_prepared_output.json", "r") as file_1:
        prepared_data = file_1.read()
    prepared_data = pd.DataFrame(json.loads(prepared_data)).sort_index(axis=1)

    with open("tests/fixtures/test_do_top_two_input.json", "r") as file_2:
        test_data = file_2.read()
    input_data = pd.DataFrame(json.loads(test_data))
    produced_data = input_data.apply(
        lambda x: lambda_method_top2_function.do_top_two(x,
                                                         'Q607_constructional_fill',
                                                         '1',
                                                         '2'), axis=1).\
        sort_index(axis=1)

    assert_frame_equal(prepared_data, produced_data)
