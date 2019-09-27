import json
import unittest

import boto3
import mock
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_s3, mock_sns, mock_sqs

import aggregation_top2_wrangler


class TestAggregationTop2Wrangler(unittest.TestCase):

    @mock.patch('aggregation_top2_wrangler.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.send_sqs_message')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.get_from_s3')
    def test_wrangler_happy_path(self, mock_get_from_s3, mock_lambda,
                                 mock_sqs, mock_sns):
        """
        Tests a correct run produces the correct success flags.
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 4372)}
                )

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            self.assertTrue(returned_value['success'])

    @mock.patch('aggregation_top2_wrangler.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.send_sqs_message')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.get_from_s3')
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
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert("Parameter validation error" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.send_sqs_message')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.get_from_s3')
    def test_missing_column_on_input(self, mock_get_from_s3,
                                     mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the error message contains "Required columns missing" if
        any of the required columns are missing.
        (IndexError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
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
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert ("Required columns missing" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.send_sqs_message')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.get_from_s3')
    def test_bad_data_on_input(self, mock_get_from_s3, mock_lambda,
                               mock_sqs, mock_sns):
        """
        Tests that the error message contains "Bad data encountered"
        if any of the values within the required columns are of the
        wrong data type.
        (TypeError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random'}
        ):
            with open("tests/fixtures/top2_wrangler_input_err.json") as file:
                input_data = json.load(file)
                mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/top2_method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 4389)}
                )

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert ("Bad data encountered" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.send_sqs_message')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.get_from_s3')
    def test_missing_column_on_output(self, mock_get_from_s3, mock_lambda,
                                      mock_sqs, mock_sns):
        """
        Tests that the error message contains "Required columns missing"
        if any of the appended columns are missing.
        (IndexError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            }
        ):
            with open("tests/fixtures/top2_wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            err_file = 'tests/fixtures/top2_method_output_err_index.json'
            with open(err_file, "rb") as file:
                mock_lambda.return_value.invoke.return_value = (
                    {"Payload": StreamingBody(file, 4182)}
                )

                returned_value = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert ("Required columns missing" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.send_sqs_message')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.get_from_s3')
    def test_bad_data_on_output(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the error message contains "Bad data encountered" if
        any of the values within the appended columns are of the wrong
        data type.
        (TypeError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
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
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert ("Bad data encountered" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.send_sqs_message')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.get_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the error message contains "Incomplete Lambda response"
        if a partial response received from the Lambda.
        (IncompleteReadError)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
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
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert ("Incomplete Lambda response" in returned_value['error'])

    @mock.patch('aggregation_top2_wrangler.send_sns_message')
    @mock.patch('aggregation_top2_wrangler.send_sqs_message')
    @mock.patch('aggregation_top2_wrangler.boto3.client')
    @mock.patch('aggregation_top2_wrangler.get_from_s3')
    def test_general_error(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        """
        Tests that the fallthrough for unclassified exceptions is working.
        (Exception)
        """
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random'
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
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert ("General Error" in returned_value['error'])


class TestMoto:

    @mock_s3
    def test_get_and_save_data_from_to_s3(self, s3):

        client = boto3.client(
            "s3",
            region_name="eu-west-2",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="TEMP")
        with open("tests/fixtures/s3_test_file.json", "rb") as file:
            s3 = boto3.resource('s3', region_name="eu-west-2")
            s3.Object("TEMP", "123").put(Body=file)

        response = aggregation_top2_wrangler.get_from_s3("TEMP", "123")

        assert isinstance(response, type(pd.DataFrame()))

    @mock_sns
    def test_publish_sns(self, sns):

        sns = boto3.client('sns', region_name='eu-west-2')
        created = sns.create_topic(Name="some-topic")
        topic_arn = created['TopicArn']

        out = aggregation_top2_wrangler.send_sns_message("3", topic_arn)

        assert (out['ResponseMetadata']['HTTPStatusCode'] == 200)

    @mock_sqs
    def test_sqs_messages(self, sqs):
        sqs = boto3.resource('sqs', region_name='eu-west-2')

        sqs.create_queue(QueueName="test_queue_test.fifo",
                         Attributes={'FifoQueue': 'true'})

        queue_url = sqs.get_queue_by_name(QueueName="test_queue_test.fifo").url

        response = aggregation_top2_wrangler.send_sqs_message(queue_url,
                                                       "{'Test': 'Message'}",
                                                       "test_group_id")
        assert response['MessageId']

    @mock_sqs
    def test_fail_to_get_from_sqs(self):
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            },
        ):
            response = aggregation_top2_wrangler.lambda_handler(
                {"RuntimeVariables": {"period": 201809}},
                {"aws_request_id": "666"}
            )

            assert "success" in response
            assert response["success"] is False
            assert ("AWS Error" in response['error'])

    def test_client_error_exception(self):
        with mock.patch.dict(aggregation_top2_wrangler.os.environ, {
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'random',
            'checkpoint': '3',
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'method_name': 'random',
            },
        ):
            with mock.patch("aggregation_top2_wrangler.get_from_s3") as mock_s3:
                with open("tests/fixtures/top2_wrangler_input.json", "r") as file:
                    mock_content = file.read()
                    mock_s3.side_effect = KeyError()
                    mock_s3.return_value = mock_content

                response = aggregation_top2_wrangler.lambda_handler(
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert ("Key Error" in response['error'])
