import json
import unittest

import mock
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_sqs

import aggregation_column_wrangler


class MockContext:
    aws_request_id = 66


context_object = MockContext()


class TestStringMethods(unittest.TestCase):

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_happy_path(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "in_file_name": "moo",
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
                        {"RuntimeVariables": {
                             "period": 201809,
                             "aggregation_type": "nunique",
                             "aggregated_column": "county",
                             "cell_total_column": "ent_ref_count",
                             "total_column": "enterprise_ref",
                             "additional_aggregated_column": "region",
                             "period_column": "period"
                            }}, context_object)

            self.assertTrue(returned_value['success'])

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_missing_environment_variable(self, mock_get_from_s3, mock_lambda,
                                          mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = {"Payload":
                                                                StreamingBody(file, 355)}

                returned_value = aggregation_column_wrangler.\
                    lambda_handler(
                        {"RuntimeVariables": {
                             "period": 201809,
                             "aggregation_type": "nunique",
                             "aggregated_column": "county",
                             "cell_total_column": "ent_ref_count",
                             "total_column": "enterprise_ref",
                             "additional_aggregated_column": "region",
                             "period_column": "period"
                            }}, context_object)

            assert(returned_value['error'].__contains__("""Parameter validation error"""))

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_bad_data_exception(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "in_file_name": "moo",
            "period_column": "period",
            "region_column": "region",
            "county_column": "county",
            "ent_ref_column": "enterprise_ref",
            "cell_total_column": "ent_ref_count"
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                content = file.read()
                content = content.replace("period", "TEST")
                input_data = json.loads(content)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = {"Payload":
                                                                StreamingBody(file, 355)}

                returned_value = aggregation_column_wrangler.\
                    lambda_handler(
                        {"RuntimeVariables": {
                             "period": 201809,
                             "aggregation_type": "nunique",
                             "aggregated_column": "county",
                             "cell_total_column": "ent_ref_count",
                             "total_column": "enterprise_ref",
                             "additional_aggregated_column": "region",
                             "period_column": "period"
                            }}, context_object)

            assert(returned_value['error'].__contains__("""Bad data encountered"""))

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "in_file_name": "moo",
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

                returned_value = aggregation_column_wrangler.\
                    lambda_handler(
                        {"RuntimeVariables": {
                             "period": 201809,
                             "aggregation_type": "nunique",
                             "aggregated_column": "county",
                             "cell_total_column": "ent_ref_count",
                             "total_column": "enterprise_ref",
                             "additional_aggregated_column": "region",
                             "period_column": "period"
                            }}, context_object)

            assert(returned_value['error'].__contains__("""Incomplete Lambda response"""))

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_general_error(self, mock_get_from_s3, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "in_file_name": "moo",
            "county_column": "county",
            "ent_ref_column": "enterprise_ref",
            "cell_total_column": "ent_ref_count"
            }
        ):

            returned_value = aggregation_column_wrangler.\
                lambda_handler(
                    {"RuntimeVariables": {
                         "period": 201809,
                         "aggregation_type": "nunique",
                         "aggregated_column": "county",
                         "cell_total_column": "ent_ref_count",
                         "total_column": "enterprise_ref",
                         "additional_aggregated_column": "region",
                         "period_column": "period"
                        }}, context_object)

            assert(returned_value['error'].__contains__("""General Error"""))

    @mock.patch('aggregation_column_wrangler.aws_functions.send_sns_message')
    @mock.patch('aggregation_column_wrangler.aws_functions.save_data')
    @mock.patch('aggregation_column_wrangler.boto3.client')
    @mock.patch('aggregation_column_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_method_error(self, mock_get_from_s3, mock_lambda,
                                   mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "in_file_name": "moo",
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            mock_lambda.return_value.invoke.return_value.get.return_value \
                .read.return_value.decode.return_value = json.dumps(
                    {"success": False, "error": "This is an error message"})

            returned_value = aggregation_column_wrangler.\
                lambda_handler(
                    {"RuntimeVariables": {
                         "period": 201809,
                         "aggregation_type": "nunique",
                         "aggregated_column": "county",
                         "cell_total_column": "ent_ref_count",
                         "total_column": "enterprise_ref",
                         "additional_aggregated_column": "region",
                         "period_column": "period"
                        }}, context_object)

            assert "success" in returned_value
            assert returned_value["success"] is False
            assert returned_value["error"].__contains__("""This is an error message""")


class TestMoto:

    @mock_sqs
    def test_fail_to_get_from_sqs(self):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "in_file_name": "moo",
            "period_column": "period",
            "region_column": "region",
            "county_column": "county",
            "ent_ref_column": "enterprise_ref",
            "cell_total_column": "ent_ref_count"
            },
        ):
            response = aggregation_column_wrangler.\
                    lambda_handler(
                        {"RuntimeVariables": {
                             "period": 201809,
                             "aggregation_type": "nunique",
                             "aggregated_column": "county",
                             "cell_total_column": "ent_ref_count",
                             "total_column": "enterprise_ref",
                             "additional_aggregated_column": "region",
                             "period_column": "period"
                            }}, context_object)

            assert "success" in response
            assert response["success"] is False
            assert response["error"].__contains__("""AWS Error""")

    def test_client_error_exception(self):
        with mock.patch.dict(aggregation_column_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'out_file_name': 'file_to_get_from_s3.json',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_message_group_id': 'random',
            'method_name': 'random',
            'incoming_message_group': 'jam',
            "in_file_name": "moo",
            "period_column": "period",
            "region_column": "region",
            "county_column": "county",
            "ent_ref_column": "enterprise_ref",
            "cell_total_column": "ent_ref_count"
            },
        ):
            with mock.patch("aggregation_column_wrangler."
                            "aws_functions.read_dataframe_from_s3") as mock_s3:
                with open("tests/fixtures/wrangler_input.json", "r") as file:
                    mock_content = file.read()
                    mock_s3.side_effect = KeyError()
                    mock_s3.return_value = mock_content

                response = aggregation_column_wrangler.\
                    lambda_handler(
                        {"RuntimeVariables": {
                             "period": 201809,
                             "aggregation_type": "nunique",
                             "aggregated_column": "county",
                             "cell_total_column": "ent_ref_count",
                             "total_column": "enterprise_ref",
                             "additional_aggregated_column": "region",
                             "period_column": "period"
                            }}, context_object)

            assert response['error'].__contains__("""Key Error""")
