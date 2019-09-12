import json
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_s3, mock_sns, mock_sqs

import aggregation_county_wrangler


class TestCountyWranglerMethods():
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "bucket_name": "mock-bucket",
                "file_name": "mock-file",
                "queue_url": "mock-queue-url",
                "checkpoint": "mockpoint",
                "sns_topic_arn": "mock-topic-arn",
                "sqs_messageid_name": "mock-message-id",
                "method_name": "mock-method",
                "period": "201809",
                "AWS_ACCESS_KEY_ID": "testing",
                "AWS_SECRET_ACCESS_KEY": "testing",
                "AWS_SECURITY_TOKEN": "testing",
                "AWS_SESSION_TOKEN": "testing"
            },
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        # Stop the mocking of the os stuff
        cls.mock_os_patcher.stop()

    @mock.patch("aggregation_county_wrangler.send_sqs_message")
    @mock.patch("aggregation_county_wrangler.boto3.client")
    @mock.patch("aggregation_county_wrangler.read_from_s3")
    def test_happy_path(self, mock_s3_return, mock_lambda, mock_sqs):
        with open("tests/fixtures/imp_output_test.json") as input_file:
            input_data = json.load(input_file)
            mock_s3_return.return_value = input_data

            returned_value = aggregation_county_wrangler.lambda_handler(None, None)

            mock_sqs.return_value = {"Messages": [{"Body": json.dumps(input_data),
                                                   "ReceiptHandle": "String"}]}

            assert "success" in returned_value
            assert returned_value["success"] is True

    @mock.patch("aggregation_county_wrangler.boto3")
    @mock.patch("aggregation_county_wrangler.boto3.client")
    @mock.patch("aggregation_county_wrangler.read_from_s3")
    def test_wrangler_general_exception(self, mock_s3_return, mock_client, mock_boto):
        mock_client_object = mock.Mock()
        mock_client.return_value = mock_client_object
        with open("tests/fixtures/imp_output_test.json") as input_file:
            input_data = json.load(input_file)
            mock_s3_return.return_value = input_data
            with mock.patch("aggregation_county_wrangler.pd.DataFrame") as mocked:
                mocked.side_effect = Exception("General exception")
                response = aggregation_county_wrangler.lambda_handler(
                    None,
                    {"aws_request_id": "666"}
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
        # Removing the strata_column to allow for test of missing parameter
        aggregation_county_wrangler.os.environ.pop("method_name")
        response = aggregation_county_wrangler.lambda_handler(None, {"aws_request_id": "666"})  # noqa E501
        aggregation_county_wrangler.os.environ["method_name"] = "mock_method"
        assert """Error validating environment params:""" in response["error"]

    @mock.patch("aggregation_county_wrangler.boto3")
    @mock.patch("aggregation_county_wrangler.boto3.client")
    @mock.patch("aggregation_county_wrangler.read_from_s3")
    def test_wrangler_key_error(self, mock_s3_return, mock_client, mock_boto):
        with open("tests/fixtures/imp_output_test.json") as input_file:
            input_data = json.load(input_file)
            mock_s3_return.return_value = input_data

            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            mock_client_object.invoke.return_value = {
                "Payload": StreamingBody(input_file, 226388)
            }
            with mock.patch("aggregation_county_wrangler.pd.DataFrame") as mocked:
                mocked.side_effect = Exception("Key Error")
                response = aggregation_county_wrangler.lambda_handler(
                    None,
                    {"aws_request_id": "666"}
                )

                assert "success" in response
                assert response["success"] is False
                assert """Key Error""" in response["error"]

    @mock.patch('aggregation_county_wrangler.send_sns_message')
    @mock.patch('aggregation_county_wrangler.send_sqs_message')
    @mock.patch('aggregation_county_wrangler.boto3.client')
    @mock.patch('aggregation_county_wrangler.read_from_s3')
    def test_bad_data_exception(self, mock_s3_return, mock_lambda, mock_sqs, mock_sns):
        with open("tests/fixtures/imp_output_test.json") as file:
            content = file.read()
            content = content.replace("period", "TEST")
            input_data = json.loads(content)

            mock_s3_return.return_value = pd.DataFrame(input_data)

            with open('tests/fixtures/agg_county_output.json', "rb") as file:
                mock_lambda.return_value.invoke.return_value = {"Payload":
                                                                StreamingBody(file, 355)}

                returned_value = aggregation_county_wrangler.lambda_handler(
                    None, {"aws_request_id": "666"}
                )

                assert(returned_value['error'].__contains__("""Bad data"""))

    @mock.patch("aggregation_county_wrangler.send_sqs_message")
    @mock.patch("aggregation_county_wrangler.boto3.client")
    @mock.patch("aggregation_county_wrangler.read_from_s3")
    def test_incomplete_read(self, mock_s3_return, mock_client, mock_sqs):
        with open("tests/fixtures/imp_output_test.json") as input_file:
            input_data = json.load(input_file)
            mock_s3_return.return_value = input_data

            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            mock_client_object.invoke.return_value = {
                "Payload": StreamingBody(input_file, 123456)
            }
            response = aggregation_county_wrangler.lambda_handler(
                None,
                {"aws_request_id": "666"}
            )

            assert "success" in response
            assert response["success"] is False
            assert """Incomplete Lambda response""" in response["error"]

    @mock_sns
    def test_publish_sns(self, sns):

        sns = boto3.client('sns', region_name='eu-west-2')
        created = sns.create_topic(Name="some-topic")
        topic_arn = created['TopicArn']

        out = aggregation_county_wrangler.send_sns_message("3", topic_arn)

        assert (out['ResponseMetadata']['HTTPStatusCode'] == 200)

    def test_client_error_exception(self):
        with mock.patch("aggregation_county_wrangler.read_from_s3") as mock_s3:
            with open("tests/fixtures/imp_output_test.json", "r") as file:
                mock_content = file.read()
                mock_s3.side_effect = KeyError()
                mock_s3.return_value = mock_content

            response = aggregation_county_wrangler.lambda_handler(
                None, {"aws_request_id": "666"}
            )

        assert response['error'].__contains__("""Key Error""")

    @mock_sqs
    def test_sqs_messages(self, sqs):
        sqs = boto3.resource('sqs', region_name='eu-west-2')

        sqs.create_queue(
            QueueName="test_queue_test.fifo",
            Attributes={'FifoQueue': 'true'}
        )

        queue_url = sqs.get_queue_by_name(
            QueueName="test_queue_test.fifo"
        ).url

        response = aggregation_county_wrangler.send_sqs_message(queue_url,
                                                                "{'Test': 'Message'}",
                                                                "test_group_id")
        assert response['MessageId']

    @mock_sqs
    def test_fail_to_get_from_sqs(self):
        response = aggregation_county_wrangler.lambda_handler(
            None, {"aws_request_id": "666"}
        )

        assert "success" in response
        assert response["success"] is False
        assert response["error"].__contains__("""AWS Error""")

    @mock_s3
    def test_get_and_save_data_from_to_s3(self, s3):

        client = boto3.client(
            "s3",
            region_name="eu-west-2",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="TEMP")
        with open("tests/fixtures/imp_output_test.json", "rb") as file:
            s3 = boto3.resource('s3', region_name="eu-west-2")
            s3.Object("TEMP", "123").put(Body=file)

            response = aggregation_county_wrangler.read_from_s3("TEMP", "123")

            with open("tests/fixtures/imp_output_test.json", "rb") as compare_file:

                assert json.loads(response) == json.load(compare_file)
