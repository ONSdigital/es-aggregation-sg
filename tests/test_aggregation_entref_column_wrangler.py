import json
import unittest

import mock
import pandas as pd
from botocore.response import StreamingBody
from es_aws_functions import exception_classes
from moto import mock_sqs

import aggregation_column_wrangler


class MockContext:
    aws_request_id = 66


context_object = MockContext()

wrangler_runtime_variables = {
                    "RuntimeVariables": {
                        "aggregation_type": "sum",
                        "aggregated_column": "county",
                        "cell_total_column": "county_total",
                        "total_columns": ["Q608_total"],
                        "additional_aggregated_column": "region",
                        "run_id": "bob",
                        "queue_url": "Earl",
                        "in_file_name": "moo",
                        'out_file_name': 'file_to_get_from_s3.json',
                        'outgoing_message_group_id': 'random',
                        'incoming_message_group_id': 'jam'
                         },
                     }

wrangler_runtime_variables_b = {
                    "RuntimeVariables":
                    {
                     "aggregation_type": "sum",
                     "aggregated_column": "county",
                     "cell_total_column": "county_total",
                     "total_columns": ["Q608_total", "Q606_other_gravel"],
                     "additional_aggregated_column": "region",
                     "run_id": "bob",
                     "queue_url": "Earl",
                     "in_file_name": "moo",
                     'out_file_name': 'file_to_get_from_s3.json',
                     'outgoing_message_group_id': 'random',
                     'incoming_message_group_id': 'jam'
                    }
                     }


class TestStringMethods(unittest.TestCase):

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_happy_path(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "r") as file:
                mock_lambda.return_value.invoke.return_value.get.return_value \
                    .read.return_value.decode.return_value = json.dumps(
                        {"success": True, "data": file.read()})

                returned_value = aggregation_column_wrangler.\
                    lambda_handler(
                        wrangler_runtime_variables, context_object)

            self.assertTrue(returned_value['success'])

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_happy_path_multiple_columns(self,
                                                  mock_get_from_s3,
                                                  mock_lambda,
                                                  mock_sqs,
                                                  mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam'
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "r") as file:
                mock_lambda.return_value.invoke.return_value.get.return_value \
                    .read.return_value.decode.return_value = json.dumps(
                        {"success": True, "data": file.read()})

                returned_value = aggregation_column_wrangler.\
                    lambda_handler(
                        wrangler_runtime_variables_b, context_object)

            self.assertTrue(returned_value['success'])

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_missing_environment_variable(self, mock_get_from_s3, mock_lambda,
                                          mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name'
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = {"Payload":
                                                                StreamingBody(file, 355)}
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    aggregation_column_wrangler.\
                        lambda_handler(
                            wrangler_runtime_variables, context_object)
            assert "Parameter validation error" in exc_info.exception.error_message

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "county_column": "county",
            "ent_ref_column": "enterprise_ref",
            "cell_total_column": "ent_ref_count"
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = {"Payload":
                                                                StreamingBody(file, 2)}
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    aggregation_column_wrangler.\
                        lambda_handler(
                            wrangler_runtime_variables, context_object)
            assert "Incomplete Lambda response" in exc_info.exception.error_message

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_general_error(self, mock_get_from_s3, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "county_column": "county",
            "ent_ref_column": "enterprise_ref",
            "cell_total_column": "ent_ref_count"
            }
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                aggregation_column_wrangler.\
                    lambda_handler(
                        wrangler_runtime_variables, context_object)
            assert "General Error" in exc_info.exception.error_message

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_method_error(self, mock_get_from_s3, mock_lambda,
                                   mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam'
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            mock_lambda.return_value.invoke.return_value.get.return_value \
                .read.return_value.decode.return_value = json.dumps(
                    {"success": False, "error": "This is an error message"})
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                aggregation_column_wrangler.\
                    lambda_handler(
                        wrangler_runtime_variables, context_object)

            assert "error message" in exc_info.exception.error_message


class TestMoto:

    @mock_sqs
    def test_fail_to_get_from_sqs(self):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "period_column": "period",
            "additional_aggregated_column": "region",
            "county_column": "county",
            "ent_ref_column": "enterprise_ref",
            "cell_total_column": "ent_ref_count"
            }
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                aggregation_column_wrangler.\
                    lambda_handler(
                        wrangler_runtime_variables, context_object)
            assert "AWS Error" in exc_info.exception.error_message

    def test_client_error_exception(self):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "period_column": "period",
            "additional_aggregated_column": "region",
            "county_column": "county",
            "ent_ref_column": "enterprise_ref",
            "cell_total_column": "ent_ref_count"
            }
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                aggregation_column_wrangler.\
                    lambda_handler(
                        wrangler_runtime_variables, context_object)
            assert "AWS Error" in exc_info.exception.error_message
