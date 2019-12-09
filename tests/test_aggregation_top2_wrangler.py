import json
import unittest

import mock
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_sqs

import aggregation_top2_wrangler


class MockContext:
    aws_request_id = 66


context_object = MockContext()


class TestAggregationTop2Wrangler(unittest.TestCase):

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_wrangler_happy_path(self, mock_get_from_s3, mock_lambda,
                                 mock_sqs, mock_sns):
        """
        Tests a correct run produces the correct success flags.
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': 'Grooop',
            'out_file_name': 'bob',
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "r") as file:
                in_file = file.read()

                mock_lambda.return_value.\
                    invoke.return_value.\
                    get.return_value.\
                    read.return_value.\
                    decode.return_value = json.dumps(in_file)
                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            self.assertTrue(returned_value['success'])

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_missing_environment_variable(self, mock_get_from_s3, mock_lambda,
                                          mock_sqs, mock_sns):
        """
        Tests that the error message contains "parameter validation error"
        if a required parameter is missing.
        (ValueError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 355)}
                )

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {"period": 201809,
                                          "total_column": "Q608_total",
                                          "aggregated_column": "county"}
                     }, context_object)

            assert("Parameter validation error" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_missing_column_on_input(self, mock_get_from_s3,
                                     mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the error message contains "Required columns missing" if
        any of the required columns are missing.
        (IndexError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                content = file.read()
                content = content.replace("period", "MissingColTest")
                input_data = json.loads(content)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 355)}
                )

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert ("Required columns missing" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_bad_data_on_input(self, mock_get_from_s3, mock_lambda,
                               mock_sqs, mock_sns):
        """
        Tests that the error message contains "Bad data encountered"
        if any of the values within the required columns are of the
        wrong data type.
        (TypeError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris",
            'period_column': 'period',
            'region_column': 'region'
            }
        ):
            with open("tests/fixtures/top2_wrangler_input_err.json") as file:
                input_data = json.load(file)
                mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 4389)}
                )

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert ("Bad data encountered" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_missing_column_on_output(self, mock_get_from_s3, mock_lambda,
                                      mock_sqs, mock_sns):
        """
        Tests that the error message contains "Required columns missing"
        if any of the appended columns are missing.
        (IndexError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
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

            err_file = 'tests/fixtures/top2_method_output_err_index.json'
            with open(err_file, "r") as file:
                lambda_return = file.read()

                mock_lambda.return_value.invoke.\
                    return_value.get.\
                    return_value.read.\
                    return_value.decode.\
                    return_value = json.dumps(lambda_return)
                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert ("Required columns missing" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_bad_data_on_output(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the error message contains "Bad data encountered" if
        any of the values within the appended columns are of the wrong
        data type.
        (TypeError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
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

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert ("Bad data encountered" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the error message contains "Incomplete Lambda response"
        if a partial response received from the Lambda.
        (IncompleteReadError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
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

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert ("Incomplete Lambda response" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_general_error(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the fallthrough for unclassified exceptions is working.
        (Exception)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
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

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert ("General Error" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.funk.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.funk.save_data')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.funk.read_dataframe_from_s3')
    def test_wrangler_method_error(self, mock_get_from_s3, mock_lambda,
                                   mock_sqs, mock_sns):
        """
        Tests a correct run produces the correct success flags.
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
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
                .read.return_value.decode.return_value = \
                '{"error": "This is an error message"}'
            returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert "success" in returned_value
            assert returned_value["success"] is False
            assert returned_value["error"].__contains__("""This is an error message""")


class TestMoto:

    @mock_sqs
    def test_fail_to_get_from_sqs(self):
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
            },
        ):
            response = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert "success" in response
            assert response["success"] is False
            assert ("AWS Error" in response['error'])

    def test_client_error_exception(self):
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            'in_file_name': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                             '82618934671237/SomethingURL.fifo',
            'sqs_message_group_id': 'random',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            'incoming_message_group': "Gruppe",
            'out_file_name': "boris"
            },
        ):
            with mock.patch("aggregation_top2_wrangler."
                            "funk.read_dataframe_from_s3") as mock_s3:
                with open("tests/fixtures/top2_wrangler_input.json", "r") as file:
                    mock_content = file.read()
                    mock_s3.side_effect = KeyError()
                    mock_s3.return_value = mock_content

                response = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {
                        "period": 201809,
                        "total_column": "Q608_total",
                        "aggregated_column": "county",
                        "additional_aggregated_column": "region",
                        "period_column": "period",
                        "county_column": "county"
                        }}, context_object)

            assert ("Key Error" in response['error'])
