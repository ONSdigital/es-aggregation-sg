import json
import unittest

import mock
import pandas as pd
from botocore.response import StreamingBody
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_sqs

import aggregation_top2_wrangler


class MockContext:
    aws_request_id = 66


wrangler_runtime_variables = {"RuntimeVariables": {
                              "total_columns": ["Q608_total"],
                              "aggregated_column": "county",
                              "additional_aggregated_column": "region",
                              "county_column": "county",
                              "top1_column": "largest_contributor",
                              "top2_column": "second_largest_contributor",
                              "run_id": "bob",
                              "queue_url": "Earl",
                              "reference": 123456789,
                              "in_file_name": {"aggregation_by_column":
                                                   "file_to_get_from_s3.json"}
                              }}

wrangler_runtime_variables_b = {"RuntimeVariables": {
                              "total_columns": ["Q608_total", "Q606_other_gravel"],
                              "aggregated_column": "county",
                              "additional_aggregated_column": "region",
                              "county_column": "county",
                              "top1_column": "largest_contributor",
                              "top2_column": "second_largest_contributor",
                              "run_id": "bob",
                              "queue_url": "Earl",
                              "reference": 123456789,
                              "in_file_name": {"aggregation_by_column":
                                                   "file_to_get_from_s3.json"}
                              }}

context_object = MockContext()


class TestAggregationTop2Wrangler(unittest.TestCase):

    @mock.patch('aggregation_top2_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_happy_path(self, mock_get_from_s3, mock_lambda,
                                 mock_sqs, mock_sns):
        """
        Tests a correct run produces the correct success flags.
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': 'Grooop',
            'out_file_name': 'bob'
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "r") as file:
                in_file = file.read()

                mock_lambda.return_value.invoke.return_value.get.return_value \
                    .read.return_value.decode.return_value = json.dumps(
                        {"success": True, "data": in_file})
                returned_value = aggregation_top2_wrangler.lambda_handler(
                    wrangler_runtime_variables, context_object)

            self.assertTrue(returned_value['success'])

    @mock.patch('aggregation_top2_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_happy_path_multiple_columns(self,
                                                  mock_get_from_s3,
                                                  mock_lambda,
                                                  mock_sqs, mock_sns):
        """
        Tests a correct run produces the correct success flags.
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': 'Grooop',
            'out_file_name': 'bob'
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output_multi_col.json', "r") as file:
                in_file = file.read()

                mock_lambda.return_value.invoke.return_value.get.return_value \
                    .read.return_value.decode.return_value = json.dumps(
                        {"success": True, "data": in_file})
                returned_value = aggregation_top2_wrangler.lambda_handler(
                    wrangler_runtime_variables_b, context_object)

            self.assertTrue(returned_value['success'])

    @mock.patch('aggregation_top2_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.aws_functions.read_dataframe_from_s3')
    def test_missing_environment_variable(self, mock_get_from_s3, mock_lambda,
                                          mock_sqs, mock_sns):
        """
        Tests that the error message contains "parameter validation error"
        if a required parameter is missing.
        (ValueError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name'
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 355)}
                )
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    aggregation_top2_wrangler.lambda_handler(
                        wrangler_runtime_variables, context_object)
                assert "Parameter validation error" in exc_info.exception.error_message

    @mock.patch('aggregation_top2_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.aws_functions.read_dataframe_from_s3')
    def test_bad_data_on_input(self, mock_get_from_s3, mock_lambda,
                               mock_sqs, mock_sns):
        """
        Tests that the error message contains "Bad data encountered"
        if any of the values within the required columns are of the
        wrong data type.
        (TypeError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris",
            'additional_aggregated_column': 'region'
            }
        ):
            with open("tests/fixtures/top2_wrangler_input_err.json") as file:
                input_data = json.load(file)
                mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 322)}
                )
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    aggregation_top2_wrangler.lambda_handler(
                        wrangler_runtime_variables, context_object)
            assert "Bad data encountered" in exc_info.exception.error_message

    @mock.patch('aggregation_top2_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.aws_functions.read_dataframe_from_s3')
    def test_bad_data_on_output(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the error message contains "Bad data encountered" if
        any of the values within the appended columns are of the wrong
        data type.
        (TypeError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            err_file = 'tests/fixtures/top2_method_output_err_type.json'
            with open(err_file, "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 4391)}
                )
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    aggregation_top2_wrangler.lambda_handler(
                        wrangler_runtime_variables, context_object)
                assert "Bad data encountered" in exc_info.exception.error_message

    @mock.patch('aggregation_top2_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.aws_functions.read_dataframe_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the error message contains "Incomplete Lambda response"
        if a partial response received from the Lambda.
        (IncompleteReadError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 2)}
                )
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    aggregation_top2_wrangler.lambda_handler(
                        wrangler_runtime_variables, context_object)
                assert "Incomplete Lambda response" in exc_info.exception.error_message

    @mock.patch('aggregation_top2_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.aws_functions.read_dataframe_from_s3')
    def test_general_error(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the fallthrough for unclassified exceptions is working.
        (Exception)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
        }
                             ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                json.load(file)

            mock_get_from_s3.side_effect = Exception("Whoops")

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 4389)}
                )
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    aggregation_top2_wrangler.lambda_handler(
                        wrangler_runtime_variables, context_object)

                assert "General Error" in exc_info.exception.error_message

    @mock.patch('aggregation_top2_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_method_error(self, mock_get_from_s3, mock_lambda,
                                   mock_sqs, mock_sns):
        """
        Tests a correct run produces the correct success flags.
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            mock_lambda.return_value.invoke.return_value.get.return_value \
                .read.return_value.decode.return_value = json.dumps(
                    {"success": False, "error": "This is an error message"})
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                aggregation_top2_wrangler.lambda_handler(
                    wrangler_runtime_variables, context_object)
            assert "error message" in exc_info.exception.error_message


class TestMoto:

    @mock_sqs
    def test_fail_to_get_from_sqs(self):
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
            }
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                aggregation_top2_wrangler.lambda_handler(
                    wrangler_runtime_variables, context_object)
            assert "AWS Error" in exc_info.exception.error_message

    def test_client_error_exception(self):
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
            }
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                aggregation_top2_wrangler.lambda_handler(
                    wrangler_runtime_variables, context_object)
            assert "AWS Error" in exc_info.exception.error_message
