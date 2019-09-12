import json
import os
import sys
import unittest
from unittest import mock

import boto3
from moto import mock_s3, mock_sns, mock_sqs
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))  # noqa
import combiner  # noqa


class TestCombininator(unittest.TestCase):
    def test_missing_environment_variable(self):
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": "mock_queue",
                "bucket_name": "Bertie Bucket",
            },
        ):
            out = combiner.lambda_handler("mike?", {"aws_request_id": "666"})
            assert not out["success"]
            assert "Error validating environment" in out["error"]

    @mock_sqs
    def test_send_and_get_sqs_message(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        sqs = boto3.client("sqs", region_name="eu-west-2")
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": queue_url,
                "bucket_name": "Bertie Bucket",
                "sqs_messageid_name": "Bob",
            },
        ):
            for i in range(1, 4):
                combiner.send_sqs_message(
                    sqs, queue_url, "message number " + str(i), "666"
                )

            messages = combiner.get_sqs_message(sqs, queue_url, 3)
            assert len(messages["Messages"]) == 3

    @mock_s3
    def test_get_data_from_s3(self):
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        client.create_bucket(Bucket="MIKE")
        client.upload_file(
            Filename="tests/fixtures/test_s3_data.json", Bucket="MIKE", Key="123"
        )
        data = combiner.get_from_s3("MIKE", "123")
        assert data
        assert json.loads(data) == {"MIKE": "MIKE"}

    @mock_sns
    def test_sns_send(self):
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": "Earl",
                "bucket_name": "Bertie Bucket",
                "sqs_messageid_name": "Bob",
            },
        ):
            sns = boto3.client("sns", region_name="eu-west-2")

            topic = sns.create_topic(Name="bloo")
            topic_arn = topic["TopicArn"]

            out = combiner.send_sns_message(topic_arn, 3)

            assert out["ResponseMetadata"]["HTTPStatusCode"] == 200

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_happy_path(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": queue_url,
                "bucket_name": "Bertie Bucket",
                "sqs_messageid_name": "Bob",
            },
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with open("tests/fixtures/agg1.json") as file:
                agg1 = file.read()
            with open("tests/fixtures/agg2.json") as file:
                agg2 = file.read()
            with open("tests/fixtures/agg3.json") as file:
                agg3 = file.read()
            with mock.patch("combiner.get_from_s3") as mock_s3:
                mock_s3.return_value = s3_data
                with mock.patch("combiner.get_sqs_message") as mock_sqs:
                    with mock.patch("combiner.send_sns_message") as mock_sns:  # noqa
                        mock_sqs.return_value = {
                            "Messages": [{"Body": agg1}, {"Body": agg2}, {"Body": agg3}]
                        }
                        out = combiner.lambda_handler("", {"aws_request_id": "666"})
                        assert out["success"]

    @mock_sqs
    @mock_s3
    def test_no_data_in_queue(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": queue_url,
                "bucket_name": "BertieBucket",
                "sqs_messageid_name": "Bob",
            },
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with mock.patch("combiner.get_from_s3") as mock_s3:
                mock_s3.return_value = s3_data
                out = combiner.lambda_handler("", {"aws_request_id": "666"})
                assert "There was no data in sqs queue" in out["error"]

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_not_enough_data_in_queue(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": queue_url,
                "bucket_name": "Bertie Bucket",
                "sqs_messageid_name": "Bob",
            },
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with open("tests/fixtures/agg1.json") as file:
                agg1 = file.read()
            with mock.patch("combiner.get_from_s3") as mock_s3:
                mock_s3.return_value = s3_data
                with mock.patch("combiner.get_sqs_message") as mock_sqs:

                    mock_sqs.return_value = {"Messages": [{"Body": agg1}]}
                    out = combiner.lambda_handler("", {"aws_request_id": "666"})
                    assert "Did not recieve all 3 messages" in out["error"]

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_attribute_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": queue_url,
                "bucket_name": "BertieBucket",
                "sqs_messageid_name": "Bob",
            },
        ):
            with mock.patch("combiner.get_from_s3") as mock_bot:
                mock_bot.side_effect = AttributeError("noo")

                out = combiner.lambda_handler("", {"aws_request_id": "666"})
                assert "Bad data encountered in" in out["error"]

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_client_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": queue_url,
                "bucket_name": "BertieBucket",
                "sqs_messageid_name": "Bob",
            },
        ):

            out = combiner.lambda_handler("", {"aws_request_id": "666"})
            assert "AWS Error" in out["error"]

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_key_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": queue_url,
                "bucket_name": "Bertie Bucket",
                "sqs_messageid_name": "Bob",
            },
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with open("tests/fixtures/agg1.json") as file:
                agg1 = file.read()
            with mock.patch("combiner.get_from_s3") as mock_s3:
                mock_s3.return_value = s3_data
                with mock.patch("combiner.get_sqs_message") as mock_sqs:
                    with mock.patch("combiner.send_sns_message") as mock_sns:  # noqa

                        mock_sqs.return_value = {
                            "Messages": [
                                {"Boody": agg1},
                                {"Boody": agg1},
                                {"Boody": agg1},
                            ]
                        }
                        out = combiner.lambda_handler("", {"aws_request_id": "666"})
                        assert "Key Error" in out["error"]

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_general_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "arn": "not_an_arn",
                "file_name": "mock_method",
                "queue_url": queue_url,
                "bucket_name": "BertieBucket",
                "sqs_messageid_name": "Bob",
            },
        ):
            with mock.patch("combiner.get_from_s3") as mock_bot:
                mock_bot.side_effect = Exception("noo")

                out = combiner.lambda_handler("", {"aws_request_id": "666"})
                assert "General Error" in out["error"]
