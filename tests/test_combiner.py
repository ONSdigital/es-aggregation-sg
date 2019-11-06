import json
import unittest
from unittest import mock

import boto3
import pandas as pd
from moto import mock_s3, mock_sns, mock_sqs

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
                "bucket_name": "bertiebucket",
                "sqs_messageid_name": "Bob"
            },
        ):
            out = combiner.lambda_handler("mike?", {"aws_request_id": "666"})
            assert not out["success"]
            assert "Error validating environment" in out["error"]

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
                "bucket_name": "mrsbucket",
                "sqs_messageid_name": "Bob",
                "s3_file": "sss"
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
            with mock.patch("combiner.funk") as mock_funk:
                mock_funk.read_dataframe_from_s3.return_value = \
                    pd.DataFrame(json.loads(s3_data))
                mock_funk.get_sqs_message.return_value = {
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
                "s3_file": "sss"
            },
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with mock.patch("combiner.funk.read_dataframe_from_s3") as mock_s3:
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
                "s3_file": "sss"
            },
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with open("tests/fixtures/agg1.json") as file:
                agg1 = file.read()
            with mock.patch("combiner.funk.read_dataframe_from_s3") as mock_s3:
                mock_s3.return_value = s3_data
                with mock.patch("combiner.funk.get_sqs_message") as mock_sqs:

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
                "s3_file": "sss"
            },
        ):
            with mock.patch("combiner.funk.read_dataframe_from_s3") as mock_bot:
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
                "s3_file": "sss"
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
                "s3_file": "sss"
            },
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with open("tests/fixtures/agg1.json") as file:
                agg1 = file.read()
            with mock.patch("combiner.funk.read_dataframe_from_s3") as mock_s3:
                mock_s3.return_value = s3_data
                with mock.patch("combiner.funk.get_sqs_message") as mock_sqs:
                    with mock.patch("combiner.funk.send_sns_message") as mock_sns:  # noqa

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
                "s3_file": "sss"
            },
        ):
            with mock.patch("combiner.funk.read_dataframe_from_s3") as mock_bot:
                mock_bot.side_effect = Exception("noo")

                out = combiner.lambda_handler("", {"aws_request_id": "666"})
                assert "General Error" in out["error"]
