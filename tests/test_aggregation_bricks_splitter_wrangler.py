import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import test_generic_library
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

import aggregation_bricks_splitter_wrangler as lambda_wrangler_function

wrangler_environment_variables = {
    "bucket_name": "mock-bucket",
    "out_file_name_brick": "mock-file",
    "out_file_name_region": "mock-file",
    "checkpoint": "mock-point",
    "sns_topic_arn": "mock-topic-arn",
    "method_name": "mock-method"
}

wrangler_runtime_variables = {
    "RuntimeVariables":
    {
     "factors_parameters": {
         "RuntimeVariables": {
             "regionless_code": "99",
             "region_column": "region"
         }
     },
     "run_id": "001",
     "queue_url": "mock-url",
     "in_file_name": {"bricks_splitter": "mock-in"},
     "incoming_message_group": {"bricks_splitter": "mock-id"}
    }
}


##########################################################################################
#                                     Generic                                            #
##########################################################################################

@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables, None,
         "AWS Error", test_generic_library.wrangler_assert)
    ])
def test_client_error(which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_wrangler_function, wrangler_runtime_variables,
         wrangler_environment_variables,
         "aggregation_bricks_splitter_wrangler.EnvironSchema",
         "General Error", test_generic_library.wrangler_assert)
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
def test_incomplete_read_error(mock_s3_get):
    file_list = ["test_wrangler_input.json"]

    test_generic_library.incomplete_read_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "aggregation_bricks_splitter_wrangler")


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_environment_variables",
    [
        (lambda_wrangler_function, wrangler_environment_variables,
         "Key Error", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, expected_message,
                   assertion, which_environment_variables):
    test_generic_library.key_error(which_lambda,
                                   expected_message, assertion,
                                   which_environment_variables)


@mock_s3
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.get_dataframe',
            side_effect=test_generic_library.replacement_get_dataframe)
def test_method_error(mock_s3_get):
    file_list = ["test_wrangler_input.json"]

    test_generic_library.wrangler_method_error(lambda_wrangler_function,
                                               wrangler_runtime_variables,
                                               wrangler_environment_variables,
                                               file_list,
                                               "aggregation_bricks_splitter_wrangler")


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion",
    [
     (lambda_wrangler_function,
      "Error validating environment params",
      test_generic_library.wrangler_assert)
    ])
def test_value_error(which_lambda, expected_message, assertion):
    test_generic_library.value_error(
        which_lambda, expected_message, assertion)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_s3
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.get_dataframe',
            side_effect=test_generic_library.replacement_get_dataframe)
@mock.patch('aggregation_bricks_splitter_wrangler.aws_functions.save_data',
            side_effect=test_generic_library.replacement_save_data)
def test_wrangler_success(mock_s3_get, mock_s3_put):
    """
    Runs the wrangler function.
    :param None
    :return Test Pass/Fail
    """
    bucket_name = wrangler_environment_variables["bucket_name"]
    client = test_generic_library.create_bucket(bucket_name)

    file_list = ["test_wrangler_input.json"]

    test_generic_library.upload_files(client, bucket_name, file_list)

    with open("tests/fixtures/test_method_output.json", "r") as file_2:
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

    with open("tests/fixtures/test_wrangler_prepared_output.json", "r") as file_3:
        test_data_prepared = file_3.read()
    prepared_data = pd.DataFrame(json.loads(test_data_prepared))

    with open("tests/fixtures/" + wrangler_environment_variables["out_file_name"],
              "r") as file_4:
        test_data_produced = file_4.read()
    produced_data = pd.DataFrame(json.loads(test_data_produced))

    assert output
    assert_frame_equal(produced_data, prepared_data)
