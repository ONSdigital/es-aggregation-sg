import unittest
import unittest as mock
import json

import mock
import boto3
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_s3, mock_sns, mock_sqs
from pandas import DataFrame

import aggregation_entref_wrangler


class TestStringMethods(unittest.TestCase):

    # @classmethod
    # def setup_class(cls):
    #     cls.mock_boto_wrangler_patcher = mock.patch('aggregation_entref_wrangler.boto3')
    #     cls.mock_boto_wrangler = cls.mock_boto_wrangler_patcher.start()
    #
    #     cls.mock_os_patcher = mock.patch.dict('os.environ', {
    #         'bucket_name': 'some-bucket-name',
    #         'file_name': 'file_to_get_from_s3.json',
    #         'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
    #                      '82618934671237/SomethingURL.fifo',
    #         'checkpoint': '3',
    #         'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
    #         'sqs_messageid_name': 'random',
    #         'method_name': 'random',
    #         'period': '201809'
    #     })
    #     cls.mock_os = cls.mock_os_patcher.start()
    #
    # @classmethod
    # def teardown_class(cls):
    #     cls.mock_boto_wrangler_patcher.stop()
    #     cls.mock_os_patcher.stop()

    @mock.patch('aggregation_entref_wrangler.send_sns_message')
    @mock.patch('aggregation_entref_wrangler.send_sqs_message')
    @mock.patch('aggregation_entref_wrangler.boto3.client')
    @mock.patch('aggregation_entref_wrangler.get_from_s3')
    def test_wrangler_happy_path(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_entref_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'file_name': 'file_to_get_from_s3.json',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_messageid_name': 'random',
            'method_name': 'random',
            'period': '201809'
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = {"Payload":
                                                                StreamingBody(file,
                                                                              355)}

                returned_value = aggregation_entref_wrangler.lambda_handler(
                    None, {"aws_request_id": "666"}
                )

            self.assertTrue(returned_value['success'])

    @mock.patch('aggregation_entref_wrangler.send_sns_message')
    @mock.patch('aggregation_entref_wrangler.send_sqs_message')
    @mock.patch('aggregation_entref_wrangler.boto3.client')
    @mock.patch('aggregation_entref_wrangler.get_from_s3')
    def test_missing_environment_variable(self, mock_get_from_s3, mock_lambda,
                                          mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_entref_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = {"Payload":
                                                                StreamingBody(file,
                                                                              355)}

                returned_value = aggregation_entref_wrangler.lambda_handler(
                    None, {"aws_request_id": "666"}
                )

            assert(returned_value['error'].__contains__("""Parameter validation error"""))

    @mock.patch('aggregation_entref_wrangler.send_sns_message')
    @mock.patch('aggregation_entref_wrangler.send_sqs_message')
    @mock.patch('aggregation_entref_wrangler.boto3.client')
    @mock.patch('aggregation_entref_wrangler.get_from_s3')
    def test_bad_data_exception(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_entref_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'file_name': 'file_to_get_from_s3.json',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_messageid_name': 'random',
            'method_name': 'random',
            'period': '201809'
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

                returned_value = aggregation_entref_wrangler.lambda_handler(
                    None, {"aws_request_id": "666"}
                )

            assert(returned_value['error'].__contains__("""Bad data encountered"""))

    @mock.patch('aggregation_entref_wrangler.send_sns_message')
    @mock.patch('aggregation_entref_wrangler.send_sqs_message')
    @mock.patch('aggregation_entref_wrangler.boto3.client')
    @mock.patch('aggregation_entref_wrangler.get_from_s3')
    def test_incomplete_json(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_entref_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'file_name': 'file_to_get_from_s3.json',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_messageid_name': 'random',
            'method_name': 'random',
            'period': '201809'
            }
        ):
            with open("tests/fixtures/wrangler_input.json") as file:
                input_data = json.load(file)

            mock_get_from_s3.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/method_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = {"Payload":
                                                                StreamingBody(file, 2)}

                returned_value = aggregation_entref_wrangler.lambda_handler(
                    None, {"aws_request_id": "666"}
                )

            assert(returned_value['error'].__contains__("""Incomplete Lambda response"""))

    @mock.patch('aggregation_entref_wrangler.send_sns_message')
    @mock.patch('aggregation_entref_wrangler.send_sqs_message')
    @mock.patch('aggregation_entref_wrangler.get_from_s3')
    def test_general_error(self, mock_get_from_s3, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_entref_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'file_name': 'file_to_get_from_s3.json',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_messageid_name': 'random',
            'method_name': 'random',
            'period': '201809'
            }
        ):

            returned_value = aggregation_entref_wrangler.lambda_handler(
                None, {"aws_request_id": "666"}
            )

            assert(returned_value['error'].__contains__("""General Error"""))

    @mock.patch('aggregation_entref_wrangler.send_sns_message')
    @mock.patch('aggregation_entref_wrangler.send_sqs_message')
    @mock.patch('aggregation_entref_wrangler.boto3.client')
    @mock.patch('aggregation_entref_wrangler.get_from_s3')
    def test_client_error(self, mock_get_from_s3, mock_lambda, mock_sqs, mock_sns):
        with mock.patch.dict(aggregation_entref_wrangler.os.environ, {
            'bucket_name': 'some-bucket-name',
            'file_name': 'file_to_get_from_s3.json',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'checkpoint': '3',
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'sqs_messageid_name': 'random',
            'method_name': 'random',
            'period': '201809'
            }
        ):
            returned_value = aggregation_entref_wrangler.lambda_handler(
                None, {"aws_request_id": "666"}
            )

            assert(returned_value['error'].__contains__("""AWS Error"""))


class MotoTests(unittest.TestCase):

    @mock_s3
    def test_get_and_save_data_from_to_s3(self):

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

        response = aggregation_entref_wrangler.get_from_s3("TEMP", "123")
        self.assertIs(type(response), type(DataFrame()))

    @mock_sns
    def test_publish_sns(self):

        sns = boto3.client('sns', region_name='eu-west-2')
        created = sns.create_topic(Name="some-topic")
        topic_arn = created['TopicArn']

        out = aggregation_entref_wrangler.send_sns_message("3", topic_arn)

        assert (out['ResponseMetadata']['HTTPStatusCode'] == 200)

    @mock_sqs
    def test_sqs_messages(self):
        sqs = boto3.resource('sqs', region_name='eu-west-2')

        sqs.create_queue(QueueName="test_queue_test.fifo",
                         Attributes={'FifoQueue': 'true'})

        queue_url = sqs.get_queue_by_name(QueueName="tests/test_queue_test.fifo").url

        aggregation_entref_wrangler.send_sqs_message(queue_url, "{'Test': 'Message'}",
                                                     "test_group_id")

        messages = sqs.receive_message(QueueUrl=queue_url)

        # Response is a list if there is a message in the
        # queue and a dict if no message is present.
        assert messages['Messages'][0]['Body'] == "{'Test': 'Message'}"
